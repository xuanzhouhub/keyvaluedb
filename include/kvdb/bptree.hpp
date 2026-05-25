#pragma once

#include "config.hpp"
#include "types.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

namespace kvdb {

class BPlusTree {
public:
    static constexpr size_t kPageSize = 4096;
    static constexpr size_t kInternalFanout = 16;
    static constexpr uint16_t kLargeValFlag = 0x7FFF;

    struct alignas(4096) LeafPage {
        uint32_t count       = 0;
        uint32_t data_start  = kPageSize;
        uint32_t slot_end    = 24;
        uint32_t _pad        = 0;
        LeafPage* next       = nullptr;
        char     data[4072];

        static constexpr size_t kSlotSize = 14;
        static constexpr uint32_t kSlotBase = 24;

        char* Raw() { return reinterpret_cast<char*>(this); }
        const char* Raw() const { return reinterpret_cast<const char*>(this); }
        char* Slot(uint32_t i) { return Raw() + kSlotBase + i * kSlotSize; }
        const char* Slot(uint32_t i) const { return Raw() + kSlotBase + i * kSlotSize; }

        uint16_t Offset(uint32_t i) const { uint16_t v; std::memcpy(&v, Slot(i), 2); return v; }
        uint16_t KeyLen(uint32_t i) const { uint16_t v; std::memcpy(&v, Slot(i)+2, 2); return v; }
        uint16_t ValLen(uint32_t i) const { uint16_t v; std::memcpy(&v, Slot(i)+4, 2); return v & 0x7FFF; }
        bool IsTombstone(uint32_t i) const { uint16_t v; std::memcpy(&v, Slot(i)+4, 2); return v != kLargeValFlag && (v & 0x8000) != 0; }
        uint64_t Timestamp(uint32_t i) const { uint64_t v; std::memcpy(&v, Slot(i)+6, 8); return v; }
        void SetSlot(uint32_t i, uint16_t off, uint16_t klen, uint16_t vlen, uint64_t ts,
                     bool tomb = false) {
            uint16_t ev = tomb ? (vlen | 0x8000) : vlen;
            std::memcpy(Slot(i), &off, 2); std::memcpy(Slot(i)+2, &klen, 2);
            std::memcpy(Slot(i)+4, &ev, 2); std::memcpy(Slot(i)+6, &ts, 8);
        }
        const char* Rec(uint32_t i) const { return Raw() + Offset(i); }
        char* Rec(uint32_t i) { return Raw() + Offset(i); }
        uint32_t Free() const { return data_start > slot_end ? data_start - slot_end : 0U; }

        bool Find(const std::string& key, uint32_t& idx) const {
            int lo = 0, hi = static_cast<int>(count) - 1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                const char* rd = Rec(static_cast<uint32_t>(mid));
                uint16_t klen = KeyLen(static_cast<uint32_t>(mid));
                int cmp = key.compare(0, std::string::npos, rd, klen);
                if (cmp <= 0) hi = mid - 1;
                else lo = mid + 1;
            }
            idx = static_cast<uint32_t>(lo);
            return (idx < count && key.compare(0, std::string::npos, Rec(idx), KeyLen(idx)) == 0);
        }

        bool InsertEntry(uint32_t pos, const std::string& key,
                         const std::string& value, uint64_t timestamp,
                         bool store_blob, bool is_tombstone = false);
    };

template<typename T, size_t N>
struct StaticVec {
    T data[N] = {};
    size_t n = 0;
    size_t size() const { return n; }
    T& operator[](size_t i) { return data[i]; }
    const T& operator[](size_t i) const { return data[i]; }
    T* begin() { return data; }
    T* end() { return data + n; }
    const T* begin() const { return data; }
    const T* end() const { return data + n; }
    void push_back(const T& v) { data[n++] = v; }
    void insert(T* pos, const T& v) {
        size_t idx = static_cast<size_t>(pos - data);
        for (size_t i = n; i > idx; --i) data[i] = data[i-1];
        data[idx] = v; ++n;
    }
    void resize(size_t sz) { n = sz; }
    template<typename It>
    void assign(It b, It e) { n = 0; while (b != e) data[n++] = *b++; }
};

struct InternalNode {
    struct Store {
        static constexpr size_t kKeyBufSize = 1024;
        uint8_t key_count = 0;
        uint8_t _pad = 0;
        uint16_t key_data_end = 0;
        uint16_t key_offs[kInternalFanout] = {};
        uint16_t key_lens[kInternalFanout] = {};
        char key_data[kKeyBufSize] = {};
        StaticVec<InternalNode*, kInternalFanout + 1> children;
        StaticVec<LeafPage*, kInternalFanout + 1> child_leaves;

        uint8_t KeyCount() const { return key_count; }
        std::string KeyStr(uint8_t i) const { return std::string(key_data+key_offs[i], key_lens[i]); }
        const std::string& KeyRef(uint8_t i) const { static std::string tmp; tmp=KeyStr(i); return tmp; }
        void InsKey(uint8_t pos, const std::string& k) {
            for (uint8_t i=key_count; i>pos; --i){key_offs[i]=key_offs[i-1];key_lens[i]=key_lens[i-1];}
            uint16_t len=static_cast<uint16_t>(k.size());
            key_offs[pos]=key_data_end; key_lens[pos]=len;
            std::memcpy(key_data+key_data_end,k.data(),len); key_data_end+=len; ++key_count;
        }
        void PushKey(const std::string& k) { InsKey(key_count,k); }
        void ResizeKeys(uint8_t n) { key_count=n; if(n>0)key_data_end=key_offs[n-1]+key_lens[n-1];else key_data_end=0; }
        void CopyKeysTo(std::vector<std::string>& dst) const {
            dst.clear(); for(uint8_t i=0;i<key_count;++i) dst.push_back(KeyStr(i));
        }
        void AssignKeysFrom(std::vector<std::string>& src) {
            key_count=0;key_data_end=0; for(auto&k:src) PushKey(k);
        }
        void CopyKeysFrom(const Store& src) {
            key_count=src.key_count; key_data_end=src.key_data_end;
            std::memcpy(key_offs,src.key_offs,sizeof(key_offs));
            std::memcpy(key_lens,src.key_lens,sizeof(key_lens));
            std::memcpy(key_data,src.key_data,key_data_end);
        }
        InternalNode* Chld(uint8_t i) const { return children[i]; }
        LeafPage* Lf(uint8_t i) const { return child_leaves[i]; }
        void SetChild(uint8_t i, InternalNode* c) { children[i] = c; }
        void SetLeaf(uint8_t i, LeafPage* l) { child_leaves[i] = l; }
        void InsChild(uint8_t pos, InternalNode* c) { children.insert(children.begin()+pos, c); }
        void InsLeaf(uint8_t pos, LeafPage* l) { child_leaves.insert(child_leaves.begin()+pos, l); }
        void PushChild(InternalNode* c) { children.push_back(c); }
        void PushLeaf(LeafPage* l) { child_leaves.push_back(l); }
        void SwapAll(Store& o) {
            Store tmp; tmp.CopyKeysFrom(*this); CopyKeysFrom(o); o.CopyKeysFrom(tmp);
            uint8_t max_c = children.n > o.children.n ? children.n : o.children.n;
            uint8_t max_l = child_leaves.n > o.child_leaves.n ? child_leaves.n : o.child_leaves.n;
            for (uint8_t i=0;i<max_c;++i) std::swap(children[i],o.children[i]);
            for (uint8_t i=0;i<max_l;++i) std::swap(child_leaves[i],o.child_leaves[i]);
            std::swap(children.n,o.children.n); std::swap(child_leaves.n,o.child_leaves.n);
        }
    };
        Store s;
        std::atomic<uint64_t> version{0};

    InternalNode() {}

        uint32_t FindChild(const std::string& key) const {
            uint32_t lo = 0, hi = s.KeyCount();
            while (lo < hi) { uint32_t mid = (lo+hi)/2; if (key < s.KeyStr(static_cast<uint8_t>(mid))) hi=mid; else lo=mid+1; }
            return lo;
        }
    };

    BPlusTree();
    ~BPlusTree();

    static uint64_t ReadVersion(const std::atomic<uint64_t>& v) { return v.load(std::memory_order_acquire); }
    static bool IsLocked(uint64_t ver) { return (ver & 1) != 0; }
    static bool TryLock(std::atomic<uint64_t>& v) {
        uint64_t expected = v.load(std::memory_order_acquire);
        if (IsLocked(expected)) return false;
        return v.compare_exchange_weak(expected, expected | 1,
                                        std::memory_order_acquire,
                                        std::memory_order_relaxed);
    }
    static void UnlockAndBump(std::atomic<uint64_t>& v) {
        v.fetch_add(1, std::memory_order_release);
    }

    void Insert(const std::string& key, const std::string& value, uint64_t timestamp, bool is_tombstone = false);
    bool Lookup(const std::string& key, uint64_t read_ts, std::string& value_out) const;
    void Export(std::vector<KeyValuePair>& out) const;
    size_t Size() const { return count_; }
    size_t MemoryUsage() const { return memory_usage_; }

    void AddAbortedBatch(uint64_t batch_ts) { aborted_batch_ts_.insert(batch_ts); }
    const std::unordered_set<uint64_t>& AbortedBatches() const { return aborted_batch_ts_; }

    class MemTableWalk {
    public:
        MemTableWalk(const BPlusTree& tree);

        bool Valid() const;
        void Next();
        const std::string& Key() const { return current_.key; }
        const std::string& Value() const { return current_.value; }
        uint64_t Timestamp() const { return current_.timestamp; }
        bool IsTombstone() const { return current_.is_tombstone; }

    private:
        void Load();
        LeafPage* leaf_ = nullptr;
        uint32_t pos_ = 0;
        KeyValuePair current_;
        const BPlusTree* tree_ = nullptr;
    };

    friend MemTableWalk;

    struct LeafWalk {
        const LeafPage* leaf;
        uint32_t pos;
        bool Valid() const { return leaf != nullptr && pos < leaf->count; }
    };
    LeafWalk BeginLeafWalk() const { return {first_leaf_, 0}; }
    bool AdvanceLeafWalk(LeafWalk& w) const {
        if (!w.leaf) return false;
        if (++w.pos >= w.leaf->count) { w.leaf = w.leaf->next; w.pos = 0; }
        return ValidLeafWalk(w);
    }
    bool ValidLeafWalk(const LeafWalk& w) const { return w.leaf && w.pos < w.leaf->count; }

private:
    static void* Alloc(size_t sz, size_t align);
    static void Free(void* p);

    void RetireLeaf(LeafPage* leaf) {
        auto it = std::find(leaf_nodes_.begin(), leaf_nodes_.end(), leaf);
        if (it != leaf_nodes_.end()) leaf_nodes_.erase(it);
        retired_leaves_.push_back(leaf);
    }
    void CopyLeafContent(LeafPage* dst, const LeafPage* src) {
        dst->count = src->count;
        dst->data_start = src->data_start;
        dst->slot_end = src->slot_end;
        dst->next = src->next;
        std::memcpy(dst->data, src->data, 4072);
    }

    // Unified split: single loop from leaf parent up to root
    void SplitCoW(LeafPage* leaf,
                  std::vector<InternalNode*>& path, std::vector<uint32_t>& indices,
                  const std::string& key, const std::string& value, uint64_t ts,
                  bool large, bool tomb, uint32_t pos) {
        LeafPage* left = NewLeaf();
        LeafPage* right = NewLeaf();
        uint32_t mid = leaf->count / 2;
        for (uint32_t i = 0; i < mid; ++i)
            CopyEntry(leaf, i, left, left->count);
        for (uint32_t i = mid; i < leaf->count; ++i)
            CopyEntry(leaf, i, right, right->count);
        if (pos < mid)
            left->InsertEntry(pos, key, value, ts, large, tomb)
                || right->InsertEntry(right->count, key, value, ts, large, tomb);
        else
            right->InsertEntry(pos - mid, key, value, ts, large, tomb)
                || left->InsertEntry(left->count, key, value, ts, large, tomb);
        left->next = right;
        right->next = leaf->next;
        std::string sep(right->Rec(0), right->KeyLen(0));

        LeafPage* cur_leaf_l = left;
        LeafPage* cur_leaf_r = right;
        InternalNode* cur_node_l = nullptr;
        InternalNode* cur_node_r = nullptr;
        bool pair_is_leaf = true;
        InternalNode* nn = nullptr;
        bool placed = false;

        for (int lv = static_cast<int>(path.size()) - 1; lv >= 0; --lv) {
            InternalNode* cp = path[static_cast<size_t>(lv)];
            uint32_t cidx = indices[static_cast<size_t>(lv)];
            nn = NewInternal();
            nn->s.CopyKeysFrom(cp->s);
            for (uint8_t i = 0; i < cp->s.children.size(); ++i) nn->s.PushChild(cp->s.Chld(i));
            for (uint8_t i = 0; i < cp->s.child_leaves.size(); ++i) nn->s.PushLeaf(cp->s.Lf(i));

            if (pair_is_leaf) {
                nn->s.SetChild(static_cast<uint8_t>(cidx), nullptr);
                nn->s.SetLeaf(static_cast<uint8_t>(cidx), cur_leaf_l);
                nn->s.InsChild(static_cast<uint8_t>(cidx + 1), nullptr);
                nn->s.InsLeaf(static_cast<uint8_t>(cidx + 1), cur_leaf_r);
            } else {
                nn->s.SetChild(static_cast<uint8_t>(cidx), cur_node_l);
                nn->s.SetLeaf(static_cast<uint8_t>(cidx), nullptr);
                nn->s.InsChild(static_cast<uint8_t>(cidx + 1), cur_node_r);
                nn->s.InsLeaf(static_cast<uint8_t>(cidx + 1), nullptr);
            }
            nn->s.InsKey(static_cast<uint8_t>(cidx), sep);

            if (nn->s.KeyCount() < kInternalFanout) {
                while (!TryLock(cp->version)) {}
                cp->s.SwapAll(nn->s);
                UnlockAndBump(cp->version);
                placed = true; break;
            }

            // overflow — split nn into internal-node pair
            mid = nn->s.KeyCount() / 2;
            cur_node_l = NewInternal(); cur_node_r = NewInternal();
            cur_node_l->s.ResizeKeys(0);
            for (uint8_t i = 0; i < mid; ++i) cur_node_l->s.PushKey(nn->s.KeyStr(i));
            for (uint8_t i = 0; i <= mid; ++i) { cur_node_l->s.PushChild(nn->s.Chld(i)); cur_node_l->s.PushLeaf(nn->s.Lf(i)); }
            cur_node_r->s.ResizeKeys(0);
            for (uint8_t i = mid + 1; i < nn->s.KeyCount(); ++i) cur_node_r->s.PushKey(nn->s.KeyStr(i));
            for (uint8_t i = mid + 1; i <= nn->s.KeyCount(); ++i) { cur_node_r->s.PushChild(nn->s.Chld(i)); cur_node_r->s.PushLeaf(nn->s.Lf(i)); }
            sep = nn->s.KeyStr(static_cast<uint8_t>(mid));
            pair_is_leaf = false;
        }

        for (LeafPage* p = first_leaf_; p; p = p->next)
            if (p->next == leaf) { p->next = left; break; }
        if (first_leaf_ == leaf) first_leaf_ = left;

        if (!placed) {
            InternalNode* nr = NewInternal();
            nr->s.PushKey(sep);
            if (pair_is_leaf) {
                nr->s.PushChild(nullptr); nr->s.PushLeaf(cur_leaf_l);
                nr->s.PushChild(nullptr); nr->s.PushLeaf(cur_leaf_r);
            } else {
                nr->s.PushChild(cur_node_l); nr->s.PushLeaf(nullptr);
                nr->s.PushChild(cur_node_r); nr->s.PushLeaf(nullptr);
            }
            root_ = nr;
        }

        RetireLeaf(leaf);
    }

    static void CopyEntry(const LeafPage* src, uint32_t i, LeafPage* dst, uint32_t pos) {
        bool blob = (src->ValLen(i) == kLargeValFlag);
        std::string k(src->Rec(i), src->KeyLen(i));
        std::string v;
        if (blob) {
            void* ptr; std::memcpy(&ptr, src->Rec(i) + src->KeyLen(i), sizeof(void*));
            size_t sz; std::memcpy(&sz, ptr, 8);
            v.assign(static_cast<const char*>(ptr) + 8, sz);
        } else {
            v.assign(src->Rec(i) + src->KeyLen(i), src->ValLen(i));
        }
        dst->InsertEntry(pos, k, v, src->Timestamp(i), blob, src->IsTombstone(i));
    }
    LeafPage* NewLeaf();
    InternalNode* NewInternal();
    LeafPage* FindLeaf(const std::string& key) const;
    LeafPage* FindLeafForWrite(const std::string& key,
                               std::vector<InternalNode*>& path,
                               std::vector<uint32_t>& indices);

    InternalNode* root_ = nullptr;
    LeafPage* first_leaf_ = nullptr;
    size_t count_ = 0;
    size_t memory_usage_ = 0;
    std::vector<InternalNode*> internal_nodes_;
    std::vector<LeafPage*> leaf_nodes_;
    std::vector<LeafPage*> retired_leaves_;
    std::unordered_set<uint64_t> aborted_batch_ts_;
};

inline void* BPlusTree::Alloc(size_t sz, size_t align) {
#ifdef _WIN32
    return _aligned_malloc(sz, align);
#else
    void* p = nullptr; posix_memalign(&p, align, sz); return p;
#endif
}
inline void BPlusTree::Free(void* p) {
#ifdef _WIN32
    _aligned_free(p);
#else
    free(p);
#endif
}
inline BPlusTree::LeafPage* BPlusTree::NewLeaf() {
    void* m = Alloc(kPageSize, 4096);
    auto* n = new (m) LeafPage();
    leaf_nodes_.push_back(n);
    return n;
}
inline BPlusTree::InternalNode* BPlusTree::NewInternal() {
    auto* n = new InternalNode();
    internal_nodes_.push_back(n);
    return n;
}
inline BPlusTree::BPlusTree() {
    LeafPage* l = NewLeaf();
    first_leaf_ = l;
    InternalNode* r = NewInternal();
    r->s.child_leaves.push_back(l);
    r->s.children.push_back(nullptr);
    root_ = r;
}
inline BPlusTree::~BPlusTree() {
    for (auto* n : leaf_nodes_) {
        for (uint32_t i = 0; i < n->count; ++i)
            if (n->ValLen(i) == kLargeValFlag) {
                void* blob; std::memcpy(&blob, n->Rec(i) + n->KeyLen(i), sizeof(void*));
                Free(blob);
            }
        n->~LeafPage(); Free(n);
    }
    for (auto* n : internal_nodes_) delete n;
}

inline bool BPlusTree::LeafPage::InsertEntry(
    uint32_t pos, const std::string& key, const std::string& value,
    uint64_t timestamp, bool store_blob, bool is_tombstone) {
    uint32_t val_sz = store_blob ? 8 : static_cast<uint32_t>(value.size());
    uint32_t rec_sz = static_cast<uint32_t>(key.size()) + val_sz;
    uint32_t needed = rec_sz + kSlotSize;
    if (Free() < needed) return false;

    uint16_t rec_off = static_cast<uint16_t>(data_start - rec_sz);
    std::memcpy(Raw() + rec_off, key.data(), key.size());
    if (store_blob) {
        size_t sz = value.size();
        void* blob = Alloc(sz + 8, 16);
        std::memcpy(blob, &sz, 8);
        std::memcpy(static_cast<char*>(blob) + 8, value.data(), sz);
        std::memcpy(Raw() + rec_off + key.size(), &blob, sizeof(void*));
    } else {
        std::memcpy(Raw() + rec_off + key.size(), value.data(), value.size());
    }

    if (pos < count)
        std::memmove(Slot(pos + 1), Slot(pos), (count - pos) * kSlotSize);
    SetSlot(pos, rec_off, static_cast<uint16_t>(key.size()),
            store_blob ? kLargeValFlag : static_cast<uint16_t>(value.size()), timestamp,
            is_tombstone);
    count++;
    data_start = rec_off;
    slot_end += static_cast<uint32_t>(kSlotSize);
    return true;
}

inline BPlusTree::LeafPage* BPlusTree::FindLeaf(const std::string& key) const {
    InternalNode* node = root_;
    while (node) {
        uint64_t v1 = ReadVersion(node->version);
        if (IsLocked(v1)) continue;
        uint32_t idx = node->FindChild(key);
        InternalNode* next = (idx < node->s.children.size()) ? node->s.children[idx] : nullptr;
        if (!next) {
            LeafPage* leaf = (idx < node->s.child_leaves.size()) ? node->s.child_leaves[idx] : nullptr;
            uint64_t v2 = ReadVersion(node->version);
            if (v1 == v2) return leaf;
            continue;
        }
        uint64_t v2 = ReadVersion(node->version);
        if (v1 != v2) continue;
        node = next;
    }
    return nullptr;
}
inline BPlusTree::LeafPage* BPlusTree::FindLeafForWrite(
    const std::string& key, std::vector<InternalNode*>& path, std::vector<uint32_t>& indices) {
    InternalNode* node = root_;
    while (node) {
        uint64_t v1 = ReadVersion(node->version);
        if (IsLocked(v1)) continue;
        uint32_t idx = node->FindChild(key);
        path.push_back(node); indices.push_back(idx);
        InternalNode* next = (idx < node->s.children.size()) ? node->s.children[idx] : nullptr;
        if (!next) {
            LeafPage* leaf = (idx < node->s.child_leaves.size()) ? node->s.child_leaves[idx] : nullptr;
            uint64_t v2 = ReadVersion(node->version);
            if (v1 == v2) return leaf;
            path.pop_back(); indices.pop_back();
            continue;
        }
        uint64_t v2 = ReadVersion(node->version);
        if (v1 != v2) { path.pop_back(); indices.pop_back(); continue; }
        node = next;
    }
    return nullptr;
}
inline void BPlusTree::Insert(const std::string& key, const std::string& value, uint64_t timestamp, bool is_tombstone) {
    std::vector<InternalNode*> path;
    std::vector<uint32_t> indices;
    LeafPage* leaf = FindLeafForWrite(key, path, indices);
    uint32_t pos;
    bool large = (value.size() > kPageSize / 2);
    bool found = leaf->Find(key, pos);
    size_t es = key.size() + value.size() + Config::kMemTableEntryOverheadBytes;

    LeafPage* nleaf = NewLeaf();
    CopyLeafContent(nleaf, leaf);
    if (nleaf->InsertEntry(pos, key, value, timestamp, large, is_tombstone)) {
        for (LeafPage* p = first_leaf_; p; p = p->next)
            if (p->next == leaf) { p->next = nleaf; break; }
        if (!path.empty()) {
            while (!TryLock(path.back()->version)) {}
            path.back()->s.child_leaves[indices.back()] = nleaf;
            UnlockAndBump(path.back()->version);
        }
        else { auto* nr = NewInternal(); nr->s.child_leaves.push_back(nleaf); root_ = nr; }
        if (first_leaf_ == leaf) first_leaf_ = nleaf;
        RetireLeaf(leaf);
        if (!found) count_++;
        memory_usage_ += es;
        return;
    }

    SplitCoW(leaf, path, indices,
             key, value, timestamp, large, is_tombstone, pos);
    if (!found) count_++;
    memory_usage_ += es;
}
inline bool BPlusTree::Lookup(const std::string& key, uint64_t read_ts, std::string& value_out) const {
    for (;;) {
        LeafPage* leaf = FindLeaf(key);
        if (!leaf) return false;
        uint32_t idx;
        if (!leaf->Find(key, idx)) return false;
        for (uint32_t i = idx; i < leaf->count; ++i) {
            if (std::string(leaf->Rec(i), leaf->KeyLen(i)) != key) break;
            if (aborted_batch_ts_.count(leaf->Timestamp(i))) continue;
            if (leaf->Timestamp(i) <= read_ts) {
                if (leaf->IsTombstone(i)) return false;
                if (leaf->ValLen(i) == kLargeValFlag) {
                    void* blob; std::memcpy(&blob, leaf->Rec(i) + leaf->KeyLen(i), sizeof(void*));
                    size_t sz; std::memcpy(&sz, blob, 8);
                    value_out.assign(static_cast<const char*>(blob) + 8, sz);
                } else {
                    value_out.assign(leaf->Rec(i) + leaf->KeyLen(i), leaf->ValLen(i));
                }
                return true;
            }
        }
        return false;
    }
}
inline void BPlusTree::Export(std::vector<KeyValuePair>& out) const {
    for (LeafPage* leaf = first_leaf_; leaf; leaf = leaf->next)
        for (uint32_t i = 0; i < leaf->count; ++i) {
            std::string k(leaf->Rec(i), leaf->KeyLen(i));
            std::string v;
            if (leaf->ValLen(i) == kLargeValFlag) {
                void* blob; std::memcpy(&blob, leaf->Rec(i) + leaf->KeyLen(i), sizeof(void*));
                size_t sz; std::memcpy(&sz, blob, 8);
                v.assign(static_cast<const char*>(blob) + 8, sz);
            } else {
                v.assign(leaf->Rec(i) + leaf->KeyLen(i), leaf->ValLen(i));
            }
            KeyValuePair kv;
            kv.key = std::move(k);
            kv.value = std::move(v);
            kv.timestamp = leaf->Timestamp(i);
            kv.is_tombstone = leaf->IsTombstone(i);
            out.push_back(std::move(kv));
        }
}

inline BPlusTree::MemTableWalk::MemTableWalk(const BPlusTree& tree)
    : leaf_(tree.first_leaf_), tree_(&tree) {
    while (leaf_ && leaf_->count == 0) leaf_ = leaf_->next;
    if (leaf_) {
        while (pos_ < leaf_->count && tree_->aborted_batch_ts_.count(leaf_->Timestamp(pos_)))
            ++pos_;
        if (pos_ >= leaf_->count) { leaf_ = leaf_->next; pos_ = 0; }
    }
    Load();
}

inline bool BPlusTree::MemTableWalk::Valid() const {
    return leaf_ != nullptr && pos_ < leaf_->count;
}

inline void BPlusTree::MemTableWalk::Next() {
    ++pos_;
    if (leaf_ && pos_ >= leaf_->count) {
        do { leaf_ = leaf_->next; } while (leaf_ && leaf_->count == 0);
        pos_ = 0;
    }
    if (tree_ && leaf_) {
        while (pos_ < leaf_->count && tree_->aborted_batch_ts_.count(leaf_->Timestamp(pos_)))
            ++pos_;
        if (pos_ >= leaf_->count) { do { leaf_ = leaf_->next; } while (leaf_ && leaf_->count == 0); pos_ = 0; }
    }
    Load();
}

inline void BPlusTree::MemTableWalk::Load() {
    if (!leaf_ || pos_ >= leaf_->count) return;
    current_.key.assign(leaf_->Rec(pos_), leaf_->KeyLen(pos_));
    uint16_t vl = leaf_->ValLen(pos_);
    if (vl == kLargeValFlag) {
        void* blob; std::memcpy(&blob, leaf_->Rec(pos_) + leaf_->KeyLen(pos_), sizeof(void*));
        size_t sz; std::memcpy(&sz, blob, 8);
        current_.value.assign(static_cast<const char*>(blob) + 8, sz);
    } else {
        current_.value.assign(leaf_->Rec(pos_) + leaf_->KeyLen(pos_), vl);
    }
    current_.timestamp = leaf_->Timestamp(pos_);
    current_.is_tombstone = leaf_->IsTombstone(pos_);
}

} // namespace kvdb
