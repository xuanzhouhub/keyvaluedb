#include "kvdb/sstable.hpp"
#include "kvdb/block_cache.hpp"
#include "kvdb/config.hpp"
#include "kvdb/internal/crc32.hpp"
#include "kvdb/iterator.hpp"
#include "kvdb/snappy.hpp"

#include <cstring>
#include <fstream>
#include <map>
#include <sstream>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace kvdb {
namespace {

static const uint8_t kCompression = Config::kCompressionNone;

struct SerializedEntry {
    uint32_t key_len; const char* key_data;
    uint32_t value_len; const char* value_data;
    uint64_t timestamp;
    bool is_tombstone = false;
    std::string key_owned; std::string val_owned;  // kept alive until freeze
    size_t WireSize() const { return 4 + key_len + 4 + value_len + 8 + 1; }
};

void CompressBlock(std::vector<char>& buf) {
    if (kCompression == Config::kCompressionSnappy && !buf.empty()) {
        std::string c; Snappy::Compress(buf.data(), buf.size(), c);
        buf.assign(c.begin(), c.end());
    }
}
void DecompressBlock(std::vector<char>& buf) {
    if (kCompression == Config::kCompressionSnappy && !buf.empty()) {
        std::string d; Snappy::Uncompress(buf.data(), buf.size(), d);
        buf.assign(d.begin(), d.end());
    }
}

class BlockBuilder {
public:
    BlockBuilder(size_t block_size) : block_size_(block_size) {}
    bool TryAppend(const SerializedEntry& e, bool allow_empty) {
        size_t ns = data_size_ + e.WireSize();
        if (!allow_empty && count_ > 0 && 4 + ns > block_size_) return false;
        entries_.push_back(e); data_size_ = ns; count_++; return true;
    }
    uint32_t Count() const { return count_; }
    const std::vector<SerializedEntry>& Entries() const { return entries_; }
    void Reset() { entries_.clear(); data_size_ = 0; count_ = 0; }

    void FreezeBlock(std::vector<char>& out, BloomFilter& bloom,
                     std::string* uncompressed_out = nullptr) {
        uint32_t n = Count();
        std::vector<char> buf; buf.reserve(4 + Entries().size() * 24);
        auto p32=[&](uint32_t v){buf.push_back(char(v&0xFF));buf.push_back(char((v>>8)&0xFF));buf.push_back(char((v>>16)&0xFF));buf.push_back(char((v>>24)&0xFF));};
        auto p64=[&](uint64_t v){p32(uint32_t(v));p32(uint32_t(v>>32));};
        p32(n);
        for (auto& e : Entries()) {
            const char* k = e.key_owned.empty() ? e.key_data : e.key_owned.data();
            const char* v = e.val_owned.empty() ? e.value_data : e.val_owned.data();
            p32(e.key_len); buf.insert(buf.end(), k, k + e.key_len);
            p32(e.value_len); buf.insert(buf.end(), v, v + e.value_len);
            p64(e.timestamp); buf.push_back(e.is_tombstone ? 1 : 0); bloom.Add(std::string(k, e.key_len));
        }
        if (uncompressed_out) uncompressed_out->assign(buf.begin(), buf.end());
        std::vector<char> comp; comp.swap(buf); CompressBlock(comp);
        CRC32 crc; crc.Update(comp.data(), comp.size());
        out.clear();
        auto o32=[&](uint32_t v){out.push_back(char(v&0xFF));out.push_back(char((v>>8)&0xFF));out.push_back(char((v>>16)&0xFF));out.push_back(char((v>>24)&0xFF));};
        o32(crc.Finalize());
        out.push_back(static_cast<char>(kCompression));
        out.push_back(static_cast<char>(comp.size()&0xFF));
        out.push_back(static_cast<char>((comp.size()>>8)&0xFF));
        out.push_back(static_cast<char>((comp.size()>>16)&0xFF));
        out.push_back(static_cast<char>((comp.size()>>24)&0xFF));
        out.insert(out.end(), comp.begin(), comp.end());
    }
private:
    size_t block_size_; std::vector<SerializedEntry> entries_;
    size_t data_size_ = 0; uint32_t count_ = 0;
};

void WriteBlock(std::ostream& file, BlockBuilder& builder,
                std::vector<uint64_t>& offsets, BloomFilter& bloom) {
    uint32_t n = builder.Count();
    std::vector<char> buf; buf.reserve(4 + builder.Entries().size() * 24);
    auto p32=[&](uint32_t v){buf.push_back(char(v&0xFF));buf.push_back(char((v>>8)&0xFF));buf.push_back(char((v>>16)&0xFF));buf.push_back(char((v>>24)&0xFF));};
    auto p64=[&](uint64_t v){p32(uint32_t(v));p32(uint32_t(v>>32));};
    p32(n);
    for (auto& e : builder.Entries()) {
        p32(e.key_len); buf.insert(buf.end(), e.key_data, e.key_data+e.key_len);
        p32(e.value_len); buf.insert(buf.end(), e.value_data, e.value_data+e.value_len);
        p64(e.timestamp); buf.push_back(e.is_tombstone ? 1 : 0); bloom.Add(std::string(e.key_data, e.key_len));
    }
    std::vector<char> comp; comp.swap(buf); CompressBlock(comp);
    CRC32 crc; crc.Update(comp.data(), comp.size());
    offsets.push_back(static_cast<uint64_t>(file.tellp()));
    SSTable::WriteUint32LE(file, crc.Finalize());
    file.put(static_cast<char>(kCompression));
    SSTable::WriteUint32LE(file, static_cast<uint32_t>(comp.size()));
    file.write(comp.data(), static_cast<std::streamsize>(comp.size()));
}

uint32_t ReadUint16LE(std::istream& is) {
    return static_cast<uint8_t>(is.get())|(static_cast<uint8_t>(is.get())<<8);
}

} // anonymous namespace

void SSTable::Write(const std::string& filepath, const std::vector<KeyValuePair>& entries) {
    std::ofstream file(filepath, std::ios::binary|std::ios::trunc);
    if(!file.is_open()) throw std::runtime_error("SSTable open failed: "+filepath);
    BloomFilter bloom(entries.empty()?1:entries.size(), 0.01);
    std::string min_key, max_key;
    if(!entries.empty()){min_key=entries[0].key;max_key=entries.back().key;}
    WriteUint32LE(file, Config::kSSTableMagic); WriteUint32LE(file, Config::kSSTableVersion);
    WriteUint32LE(file,static_cast<uint32_t>(Config::kSSTableBlockSize));
    WriteUint32LE(file,static_cast<uint32_t>(entries.size()));
    file.put(static_cast<char>(kCompression)); file.put(0);file.put(0);file.put(0);
    uint16_t mkl=static_cast<uint16_t>(min_key.size()),mxl=static_cast<uint16_t>(max_key.size());
    file.put(static_cast<char>(mkl&0xFF));file.put(static_cast<char>((mkl>>8)&0xFF));
    file.put(static_cast<char>(mxl&0xFF));file.put(static_cast<char>((mxl>>8)&0xFF));
    std::vector<uint64_t> offsets; std::vector<std::string> first_keys;
    BlockBuilder builder(Config::kSSTableBlockSize);
    for(auto& kv:entries){
        SerializedEntry se; se.key_len=static_cast<uint32_t>(kv.key.size());se.key_data=kv.key.data();
        se.value_len=static_cast<uint32_t>(kv.value.size());se.value_data=kv.value.data();se.timestamp=kv.timestamp;se.is_tombstone=kv.is_tombstone;
        bool allow=(builder.Count()==0); if(allow)first_keys.push_back(kv.key);
        while(!builder.TryAppend(se,allow)){WriteBlock(file,builder,offsets,bloom);builder.Reset();allow=true;first_keys.push_back(kv.key);}
    }
    if(builder.Count()>0) WriteBlock(file,builder,offsets,bloom);
    uint64_t filter_off=static_cast<uint64_t>(file.tellp());
    file.write(min_key.data(),min_key.size()); file.write(max_key.data(),max_key.size());
    auto&bd=bloom.Data(); WriteUint32LE(file,static_cast<uint32_t>(bloom.BitCount()));
    WriteUint32LE(file,bloom.HashCount()); WriteUint32LE(file,static_cast<uint32_t>(bd.size()));
    file.write(reinterpret_cast<const char*>(bd.data()),static_cast<std::streamsize>(bd.size()));
    WriteUint32LE(file,static_cast<uint32_t>(offsets.size()));
    for(auto o:offsets)WriteUint64LE(file,o);
    for(auto&fk:first_keys){uint16_t kl=static_cast<uint16_t>(fk.size());file.put(char(kl&0xFF));file.put(char((kl>>8)&0xFF));file.write(fk.data(),kl);}
    WriteUint64LE(file,filter_off); WriteUint32LE(file,Config::kSSTableFooterMagic);
}

void SSTable::WriteFromWalk(const std::string& filepath, BPlusTree::MemTableWalk& walk,
                            size_t entry_count, BlockReader* cache) {
    BloomFilter bloom(entry_count > 0 ? entry_count : 1, 0.01);
    BlockBuilder builder(Config::kSSTableBlockSize);
    struct BlockData { std::vector<char> data; std::string first_key; std::string uncompressed; };
    std::vector<BlockData> blocks;
    std::string min_key, max_key, prev_key, best_val, block_first_key;
    uint64_t best_ts = 0; bool has_best = false; bool best_tomb = false; size_t written = 0;

    auto flushBest = [&]() {
        if (!has_best) return;
        SerializedEntry se;
        se.key_len = static_cast<uint32_t>(prev_key.size());
        se.key_owned = prev_key; se.key_data = se.key_owned.data();
        se.value_len = static_cast<uint32_t>(best_val.size());
        se.val_owned = best_val; se.value_data = se.val_owned.data();
        se.timestamp = best_ts;
        se.is_tombstone = best_tomb;
        if (builder.Count() == 0) block_first_key = prev_key;
        while (!builder.TryAppend(se, builder.Count() == 0)) {
            BlockData bd; bd.first_key = block_first_key;
            builder.FreezeBlock(bd.data, bloom, &bd.uncompressed);
            blocks.push_back(std::move(bd));
            builder.Reset(); block_first_key = prev_key;
        }
        if (min_key.empty()) min_key = prev_key;
        max_key = prev_key; written++; has_best = false;
    };

    while (walk.Valid()) {
        const std::string& k = walk.Key();
        if (k != prev_key) {
            flushBest();
            prev_key = k; best_ts = walk.Timestamp(); best_val = walk.Value(); best_tomb = walk.IsTombstone(); has_best = true;
        } else {
            uint64_t ts = walk.Timestamp();
            if (ts > best_ts) { best_ts = ts; best_val = walk.Value(); best_tomb = walk.IsTombstone(); }
        }
        walk.Next();
    }
    flushBest();
    if (builder.Count() > 0) {
        BlockData bd; builder.FreezeBlock(bd.data, bloom, &bd.uncompressed); blocks.push_back(std::move(bd));
    }

    std::ofstream file(filepath, std::ios::binary | std::ios::trunc);
    WriteUint32LE(file, Config::kSSTableMagic); WriteUint32LE(file, Config::kSSTableVersion);
    WriteUint32LE(file, static_cast<uint32_t>(Config::kSSTableBlockSize));
    WriteUint32LE(file, static_cast<uint32_t>(written));
    file.put(static_cast<char>(kCompression)); file.put(0); file.put(0); file.put(0);
    uint16_t mkl = min_key.empty() ? 0 : static_cast<uint16_t>(min_key.size());
    uint16_t mxl = max_key.empty() ? 0 : static_cast<uint16_t>(max_key.size());
    file.put(static_cast<char>(mkl&0xFF)); file.put(static_cast<char>((mkl>>8)&0xFF));
    file.put(static_cast<char>(mxl&0xFF)); file.put(static_cast<char>((mxl>>8)&0xFF));

    std::vector<uint64_t> offsets; std::vector<std::string> fkeys;
    for (size_t bi = 0; bi < blocks.size(); ++bi) {
        auto& bd = blocks[bi];
        offsets.push_back(static_cast<uint64_t>(file.tellp()));
        file.write(bd.data.data(), static_cast<std::streamsize>(bd.data.size()));
        fkeys.push_back(std::move(bd.first_key));
        if (cache && !bd.uncompressed.empty())
            cache->PutBlock(filepath, static_cast<uint32_t>(bi),
                            bd.uncompressed, 0);
    }

    uint64_t filter_off = static_cast<uint64_t>(file.tellp());
    if (!min_key.empty()) file.write(min_key.data(), min_key.size());
    if (!max_key.empty()) file.write(max_key.data(), max_key.size());
    const auto& bdata = bloom.Data();
    WriteUint32LE(file, static_cast<uint32_t>(bloom.BitCount()));
    WriteUint32LE(file, bloom.HashCount());
    WriteUint32LE(file, static_cast<uint32_t>(bdata.size()));
    file.write(reinterpret_cast<const char*>(bdata.data()), static_cast<std::streamsize>(bdata.size()));
    WriteUint32LE(file, static_cast<uint32_t>(blocks.size()));
    for (auto o : offsets) WriteUint64LE(file, o);
    for (auto& fk : fkeys) {
        uint16_t kl = static_cast<uint16_t>(fk.size());
        file.put(static_cast<char>(kl&0xFF)); file.put(static_cast<char>((kl>>8)&0xFF));
        file.write(fk.data(), kl);
    }
    WriteUint64LE(file, filter_off); WriteUint32LE(file, Config::kSSTableFooterMagic);
}

SSTable::Metadata SSTable::ReadMetadata(const std::string& filepath,
                                        BlockReader* cache) {
    if (cache) {
        Metadata cached;
        if (cache->GetMetadata(filepath, cached))
            return cached;
    }
    std::ifstream file(filepath,std::ios::binary);
    if(!file.is_open())throw std::runtime_error("SSTable open: "+filepath);
    Metadata meta; meta.filepath=filepath;
    if(ReadUint32LE(file)!=Config::kSSTableMagic)throw std::runtime_error("Bad magic: "+filepath);
    if(ReadUint32LE(file)!=Config::kSSTableVersion)throw std::runtime_error("Bad version: "+filepath);
    ReadUint32LE(file); meta.entry_count=ReadUint32LE(file);
    file.seekg(4,std::ios::cur);
    uint16_t minkl=static_cast<uint16_t>(ReadUint16LE(file)),maxkl=static_cast<uint16_t>(ReadUint16LE(file));
    meta.min_key_len=UINT32_MAX;meta.max_key_len=0; uint32_t seen=0;
    while(seen<meta.entry_count){
        uint32_t crc=ReadUint32LE(file);file.seekg(1,std::ios::cur);
        uint32_t csz=ReadUint32LE(file);
        std::vector<char> cbuf(csz);file.read(cbuf.data(),static_cast<std::streamsize>(csz));
        CRC32 c;c.Update(cbuf.data(),cbuf.size());if(c.Finalize()!=crc)throw std::runtime_error("CRC mismatch: "+filepath);
        DecompressBlock(cbuf);
        std::istringstream bs(std::string(cbuf.begin(),cbuf.end()));
        auto r32=[&](){uint32_t v=0;v|=uint8_t(bs.get());v|=uint8_t(bs.get())<<8;v|=uint8_t(bs.get())<<16;v|=uint8_t(bs.get())<<24;return v;};
        uint32_t n=r32();
        for(uint32_t i=0;i<n;++i){uint32_t kl=r32();bs.seekg(kl,std::ios::cur);uint32_t vl=r32();bs.seekg(vl,std::ios::cur);for(int b=0;b<8;++b)bs.get();bs.get();if(kl<meta.min_key_len)meta.min_key_len=kl;if(kl>meta.max_key_len)meta.max_key_len=kl;}
        seen+=n;
    }
    if(minkl>0){meta.min_key.resize(minkl);file.read(&meta.min_key[0],minkl);}
    if(maxkl>0){meta.max_key.resize(maxkl);file.read(&meta.max_key[0],maxkl);}
    uint32_t bb=ReadUint32LE(file),bh=ReadUint32LE(file),bz=ReadUint32LE(file);
    if(bz>0){std::vector<uint8_t> bd(bz);file.read(reinterpret_cast<char*>(bd.data()),bz);meta.bloom=BloomFilter::FromRaw(bd.data(),bb,bh);}
    uint32_t bc=ReadUint32LE(file); meta.block_offsets.resize(bc);
    for(uint32_t i=0;i<bc;++i)meta.block_offsets[i]=ReadUint64LE(file);
    meta.block_first_keys.resize(bc);
    for(uint32_t i=0;i<bc;++i){uint16_t kl=static_cast<uint16_t>(ReadUint16LE(file));meta.block_first_keys[i].resize(kl);if(kl>0)file.read(&meta.block_first_keys[i][0],kl);}
    file.seekg(0,std::ios::end);meta.file_size=static_cast<uint64_t>(file.tellg());
    if (cache) cache->PutMetadata(filepath, meta);
    return meta;
}

std::vector<KeyValuePair> SSTable::ReadAll(const std::string& filepath) {
    std::ifstream file(filepath,std::ios::binary);
    if(!file.is_open())throw std::runtime_error("SSTable open: "+filepath);
    if(ReadUint32LE(file)!=Config::kSSTableMagic)throw std::runtime_error("Bad magic");
    if(ReadUint32LE(file)!=Config::kSSTableVersion)throw std::runtime_error("Bad version");
    ReadUint32LE(file); uint32_t ec=ReadUint32LE(file);
    file.seekg(4,std::ios::cur);file.seekg(4,std::ios::cur);
    std::vector<KeyValuePair> entries;entries.reserve(ec);
    if(ec==0)return entries;
    while(entries.size()<ec){
        ReadUint32LE(file);file.seekg(1,std::ios::cur);
        uint32_t csz=ReadUint32LE(file);
        std::vector<char> cbuf(csz);file.read(cbuf.data(),static_cast<std::streamsize>(csz));DecompressBlock(cbuf);
        std::istringstream bs(std::string(cbuf.begin(),cbuf.end()));
        auto r32=[&](){uint32_t v=0;v|=uint8_t(bs.get());v|=uint8_t(bs.get())<<8;v|=uint8_t(bs.get())<<16;v|=uint8_t(bs.get())<<24;return v;};
        uint32_t n=r32();
        for(uint32_t i=0;i<n;++i){uint32_t kl=r32();std::string k(kl,0);bs.read(&k[0],kl);uint32_t vl=r32();std::string v(vl,0);bs.read(&v[0],vl);uint64_t ts=0;for(int b=0;b<8;++b)ts|=static_cast<uint64_t>(static_cast<uint8_t>(bs.get()))<<(b*8);uint8_t fl=uint8_t(bs.get());entries.push_back({std::move(k),std::move(v),ts,(fl&1)!=0});}
    }
    return entries;
}

bool SSTable::LookupKey(const std::string& filepath, const std::string& key,
                         uint64_t read_ts, std::string& value_out,
                         BlockReader* cache) {
    auto meta=ReadMetadata(filepath, cache);
    if(meta.block_first_keys.empty())return false;
    uint32_t target=0;
    for(uint32_t i=0;i<meta.block_first_keys.size();++i){if(key<meta.block_first_keys[i])break;target=i;}
    auto search=[&](uint32_t idx)->bool{
        if(idx>=meta.block_offsets.size())return false;
        if (cache) {
            std::string block_data;
            uint32_t entry_count;
            if (cache->GetBlock(filepath, idx, block_data, entry_count)) {
                std::istringstream bs(std::move(block_data));
                auto r32=[&](){uint32_t v=0;v|=uint8_t(bs.get());v|=uint8_t(bs.get())<<8;v|=uint8_t(bs.get())<<16;v|=uint8_t(bs.get())<<24;return v;};
                uint32_t n=r32();
                for(uint32_t i=0;i<n;++i){uint32_t kl=r32();std::string k(kl,0);bs.read(&k[0],kl);uint32_t vl=r32();std::string v(vl,0);bs.read(&v[0],vl);uint64_t ts=0;for(int b=0;b<8;++b)ts|=static_cast<uint64_t>(static_cast<uint8_t>(bs.get()))<<(b*8);uint8_t fl=uint8_t(bs.get());if(k==key&&ts<=read_ts){value_out=std::move(v);return true;}}
                return false;
            }
        }
        std::ifstream f(filepath,std::ios::binary);
        f.seekg(static_cast<std::streamoff>(meta.block_offsets[idx]));
        ReadUint32LE(f);f.seekg(1,std::ios::cur);
        uint32_t csz=ReadUint32LE(f);
        std::vector<char> cbuf(csz);f.read(cbuf.data(),static_cast<std::streamsize>(csz));DecompressBlock(cbuf);
        std::istringstream bs(std::string(cbuf.begin(),cbuf.end()));
        auto r32=[&](){uint32_t v=0;v|=uint8_t(bs.get());v|=uint8_t(bs.get())<<8;v|=uint8_t(bs.get())<<16;v|=uint8_t(bs.get())<<24;return v;};
        uint32_t n=r32();
        for(uint32_t i=0;i<n;++i){uint32_t kl=r32();std::string k(kl,0);bs.read(&k[0],kl);uint32_t vl=r32();std::string v(vl,0);bs.read(&v[0],vl);uint64_t ts=0;for(int b=0;b<8;++b)ts|=static_cast<uint64_t>(static_cast<uint8_t>(bs.get()))<<(b*8);uint8_t fl=uint8_t(bs.get());if(k==key&&ts<=read_ts){value_out=std::move(v);return true;}}
        return false;
    };
    if(search(target))return true;
    if(target>0&&search(target-1))return true;
    if(target+1<meta.block_offsets.size()&&search(target+1))return true;
    return false;
}

void SSTable::WriteUint32LE(std::ostream& os, uint32_t value) {
    os.put(static_cast<char>(value&0xFF));os.put(static_cast<char>((value>>8)&0xFF));
    os.put(static_cast<char>((value>>16)&0xFF));os.put(static_cast<char>((value>>24)&0xFF));
}
void SSTable::WriteUint32LE(std::vector<char>& buf, uint32_t value) {
    buf.push_back(static_cast<char>(value&0xFF));buf.push_back(static_cast<char>((value>>8)&0xFF));
    buf.push_back(static_cast<char>((value>>16)&0xFF));buf.push_back(static_cast<char>((value>>24)&0xFF));
}
void SSTable::WriteUint64LE(std::ostream& os, uint64_t value) {
    WriteUint32LE(os,static_cast<uint32_t>(value));WriteUint32LE(os,static_cast<uint32_t>(value>>32));
}
uint32_t SSTable::ReadUint32LE(std::istream& is) {
    uint32_t v=0;v|=static_cast<uint8_t>(is.get());v|=static_cast<uint8_t>(is.get())<<8;
    v|=static_cast<uint8_t>(is.get())<<16;v|=static_cast<uint8_t>(is.get())<<24;return v;
}
uint64_t SSTable::ReadUint64LE(std::istream& is) {
    uint64_t v=0;v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()));
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<8;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<16;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<24;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<32;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<40;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<48;
    v|=static_cast<uint64_t>(static_cast<uint8_t>(is.get()))<<56;return v;
}

void SSTable::Compact(const std::vector<Metadata>& inputs,
                      const std::string& output_dir,
                      uint64_t output_seq_start,
                      int output_level,
                      size_t max_sstable_size,
                      bool is_last_level,
                      const std::string& range_lower,
                      const std::string& range_upper,
                      std::vector<Metadata>& outputs,
                      std::vector<std::string>& garbage_files) {
    struct MergeSrc {
        std::unique_ptr<SourceIterator> iter;
        KeyValuePair cur;
        MergeSrc(std::unique_ptr<SourceIterator> i) : iter(std::move(i)) {
            if (iter->Valid()) cur = iter->Current();
        }
        bool Valid() const { return iter->Valid(); }
        const KeyValuePair& Current() const { return cur; }
        void Next() { iter->Next(); if (iter->Valid()) cur = iter->Current(); }
    };
    std::vector<MergeSrc> sources;

    for (auto& in : inputs) {
        if (in.level == 0) {
            auto it = std::make_unique<SSTableIterator>(in.filepath);
            if (it->Valid()) sources.emplace_back(std::move(it));
        }
    }

    std::map<int, std::vector<SSTable::Metadata>> level_groups;
    for (auto& in : inputs) {
        if (in.level > 0) level_groups[in.level].push_back(in);
    }
    for (auto& [lvl, files] : level_groups) {
        auto li = std::make_unique<LevelIterator>(files);
        if (li->Valid()) sources.emplace_back(std::move(li));
    }

    if (sources.empty()) return;

    auto nextKey = [&]() -> KeyValuePair {
        while (true) {
            int best = -1;
            for (size_t i = 0; i < sources.size(); ++i) {
                if (!sources[i].Valid()) continue;
                if (best < 0 || sources[i].Current().key < sources[best].Current().key)
                    best = static_cast<int>(i);
            }
            if (best < 0) return {"", "", 0, false};

            std::string key = sources[best].Current().key;
            KeyValuePair winner = sources[best].Current();
            sources[best].Next();

            for (size_t i = 0; i < sources.size(); ++i) {
                while (sources[i].Valid() && sources[i].Current().key == key) {
                    if (sources[i].Current().timestamp > winner.timestamp)
                        winner = sources[i].Current();
                    sources[i].Next();
                }
            }
            if (winner.is_tombstone && is_last_level) continue;
            if (!range_lower.empty() && winner.key < range_lower) continue;
            if (!range_upper.empty() && winner.key > range_upper) return {};
            return winner;
        }
    };

    uint64_t seq = output_seq_start;
    std::vector<KeyValuePair> batch;
    size_t approx = 0;

    for (;;) {
        KeyValuePair kv = nextKey();
        if (kv.key.empty()) break;

        size_t es = kv.key.size() + kv.value.size() + 24;
        if (!batch.empty() && approx + es > max_sstable_size) {
            std::ostringstream oss;
            oss << output_dir << "/sstable_" << seq++ << ".sst";
            std::string fpath = oss.str();
            SSTable::Write(fpath, batch);
            Metadata meta = ReadMetadata(fpath);
            meta.filepath = fpath;
            meta.level = output_level;
            outputs.push_back(std::move(meta));
            batch.clear();
            approx = 0;
        }
        batch.push_back(std::move(kv));
        approx += es;
    }

    if (!batch.empty()) {
        std::ostringstream oss;
        oss << output_dir << "/sstable_" << seq << ".sst";
        std::string fpath = oss.str();
        SSTable::Write(fpath, batch);
        Metadata meta = ReadMetadata(fpath);
        meta.filepath = fpath;
        meta.level = output_level;
        outputs.push_back(std::move(meta));
    }

    for (auto& in : inputs) garbage_files.push_back(in.filepath);
}

} // namespace kvdb
