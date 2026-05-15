#include "kvdb/sstable.hpp"
#include <iostream>
#include <sstream>
int main() {
    // Build a block manually
    auto p32=[&](std::vector<char>& b, uint32_t v){b.push_back(char(v&0xFF));b.push_back(char((v>>8)&0xFF));b.push_back(char((v>>16)&0xFF));b.push_back(char((v>>24)&0xFF));};
    
    // Use Write to create an SSTable with known data, then read block 0
    std::vector<kvdb::KeyValuePair> e;
    for (int i=0;i<10;++i) e.push_back({"k"+std::to_string(i),"v"+std::to_string(i),(uint64_t)i});
    kvdb::SSTable::Write("test_frz.sst", e);
    
    // Read all back to verify
    auto r = kvdb::SSTable::ReadAll("test_frz.sst");
    std::cerr << "read " << r.size() << " entries: ";
    for (auto& kv : r) std::cerr << kv.key << "=" << kv.value << " ";
    std::cerr << std::endl;
    
    bool ok = (r.size()==10);
    for (int i=0;i<10&&i<(int)r.size();++i)
        if (r[i].key!="k"+std::to_string(i)) ok=false;
    std::cerr << (ok?"OK":"FAIL") << std::endl;
    return ok?0:1;
}
