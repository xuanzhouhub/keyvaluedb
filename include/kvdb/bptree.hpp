#pragma once

#include "config.hpp"
#include "types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace kvdb {

class BPlusTree {
public:
    static constexpr size_t kPageSize = 4096;
    static constexpr size_t kInternalFanout = 16;
    static constexpr uint16_t kLargeValFlag = 0xFFFF;

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
        uint16_t ValLen(uint32_t i) const { uint16_t v; std::memcpy(&v, Slot(i)+4, 2); return v; }
        uint64_t Timestamp(uint32_t i) const { uint64_t v; std::memcpy(&v, Slot(i)+6, 8); return v; }
        void SetSlot(uint32_t i, uint16_t off, uint16_t klen, uint16_t vlen, uint64_t ts) {
            std::memcpy(Slot(i), &off, 2); std::memcpy(Slot(i)+2, &klen, 2);
            std::memcpy(Slot(i)+4, &vlen, 2); std::memcpy(Slot(i)+6, &ts, 8);
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
                         bool store_blob);
    };

    struct InternalNode {
        std::vector<std::string> keys;
        std::vector<InternalNode*> children;
        std::vector<LeafPage*> child_leaves;

        InternalNode() {
            keys.reserve(kInternalFanout);
            children.reserve(kInternalFanout + 1);
            child_leaves.reserve(kInternalFanout + 1);
        }

        uint32_t FindChild(const std::string& key) const {
            uint32_t lo = 0, hi = static_cast<uint32_t>(keys.size());
            while (lo < hi) { uint32_t mid = (lo+hi)/2; if (key < keys[mid]) hi=mid; else lo=mid+1; }
            return lo;
        }
    };

    BPlusTree();
    ~BPlusTree();

    void Insert(const std::string& key, const std::string& value, uint64_t timestamp);
    bool Lookup(const std::string& key, uint64_t read_ts, std::string& value_out) const;
    void Export(std::vector<KeyValuePair>& out) const;
    size_t Size() const { return count_; }
    size_t MemoryUsage() const { return memory_usage_; }

    class MemTableWalk {
    public:
        MemTableWalk(const BPlusTree& tree);

        bool Valid() const;
        void Next();
        const std::string& Key() const { return current_.key; }
        const std::string& Value() const { return current_.value; }
        uint64_t Timestamp() const { return current_.timestamp; }

    private:
        void Load();
        LeafPage* leaf_ = nullptr;
        uint32_t pos_ = 0;
        KeyValuePair current_;
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

    LeafPage* NewLeaf();
    InternalNode* NewInternal();
    LeafPage* FindLeaf(const std::string& key) const;
    LeafPage* FindLeafForWrite(const std::string& key,
                               std::vector<InternalNode*>& path,
                               std::vector<uint32_t>& indices);
    void SplitLeaf(LeafPage* leaf, InternalNode* parent, uint32_t idx,
                   std::vector<InternalNode*>& path, std::vector<uint32_t>& indices);
    void SplitInternal(InternalNode* node, std::vector<InternalNode*>& path,
                       std::vector<uint32_t>& indices);

    InternalNode* root_ = nullptr;
    LeafPage* first_leaf_ = nullptr;
    size_t count_ = 0;
    size_t memory_usage_ = 0;
    std::vector<InternalNode*> internal_nodes_;
    std::vector<LeafPage*> leaf_nodes_;
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
    r->child_leaves.push_back(l);
    r->children.push_back(nullptr);
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
    uint64_t timestamp, bool store_blob) {
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
            store_blob ? kLargeValFlag : static_cast<uint16_t>(value.size()), timestamp);
    count++;
    data_start = rec_off;
    slot_end += static_cast<uint32_t>(kSlotSize);
    return true;
}

inline BPlusTree::LeafPage* BPlusTree::FindLeaf(const std::string& key) const {
    InternalNode* node = root_;
    while (node) {
        uint32_t idx = node->FindChild(key);
        if (node->children[idx] == nullptr) return node->child_leaves[idx];
        node = node->children[idx];
    }
    return nullptr;
}
inline BPlusTree::LeafPage* BPlusTree::FindLeafForWrite(
    const std::string& key, std::vector<InternalNode*>& path, std::vector<uint32_t>& indices) {
    InternalNode* node = root_;
    while (node) {
        uint32_t idx = node->FindChild(key);
        path.push_back(node); indices.push_back(idx);
        if (node->children[idx] == nullptr) return node->child_leaves[idx];
        node = node->children[idx];
    }
    return nullptr;
}
inline void BPlusTree::SplitLeaf(
    LeafPage* leaf, InternalNode* parent, uint32_t child_idx,
    std::vector<InternalNode*>& path, std::vector<uint32_t>& indices) {
    LeafPage* nl = NewLeaf();
    uint32_t mid = leaf->count / 2;
    for (uint32_t i = mid; i < leaf->count; ++i) {
        bool blob = (leaf->ValLen(i) == kLargeValFlag);
        std::string k(leaf->Rec(i), leaf->KeyLen(i));
        std::string v;
        if (blob) {
            void* ptr; std::memcpy(&ptr, leaf->Rec(i) + leaf->KeyLen(i), sizeof(void*));
            size_t sz; std::memcpy(&sz, ptr, 8);
            v.assign(static_cast<const char*>(ptr) + 8, sz);
            Free(ptr);
        } else {
            v.assign(leaf->Rec(i) + leaf->KeyLen(i), leaf->ValLen(i));
        }
        nl->InsertEntry(nl->count, k, v, leaf->Timestamp(i), blob);
    }
    leaf->count = mid;
    leaf->slot_end = LeafPage::kSlotBase + mid * LeafPage::kSlotSize;
    nl->next = leaf->next; leaf->next = nl;

    std::string sep(nl->Rec(0), nl->KeyLen(0));
    parent->child_leaves.insert(
        parent->child_leaves.begin() + static_cast<ptrdiff_t>(child_idx) + 1, nl);
    parent->children.insert(
        parent->children.begin() + static_cast<ptrdiff_t>(child_idx) + 1, nullptr);
    parent->keys.insert(
        parent->keys.begin() + static_cast<ptrdiff_t>(child_idx), sep);
    if (parent->keys.size() >= kInternalFanout) SplitInternal(parent, path, indices);
}
inline void BPlusTree::SplitInternal(
    InternalNode* node, std::vector<InternalNode*>& path, std::vector<uint32_t>& indices) {
    InternalNode* nn = NewInternal();
    uint32_t mid = node->keys.size() / 2;
    std::string mid_key = node->keys[mid];

    nn->keys.assign(node->keys.begin() + static_cast<ptrdiff_t>(mid) + 1, node->keys.end());
    nn->children.assign(node->children.begin() + static_cast<ptrdiff_t>(mid) + 1, node->children.end());
    nn->child_leaves.assign(node->child_leaves.begin() + static_cast<ptrdiff_t>(mid) + 1, node->child_leaves.end());
    node->keys.resize(mid);
    node->children.resize(mid + 1);
    node->child_leaves.resize(mid + 1);

    if (path.size() <= 1) {
        InternalNode* nr = NewInternal();
        nr->keys.push_back(mid_key);
        nr->children.push_back(node); nr->child_leaves.push_back(nullptr);
        nr->children.push_back(nn); nr->child_leaves.push_back(nullptr);
        root_ = nr;
    } else {
        InternalNode* p = path[path.size()-2]; uint32_t idx = indices[indices.size()-2];
        p->keys.insert(p->keys.begin() + static_cast<ptrdiff_t>(idx), mid_key);
        p->children.insert(p->children.begin() + static_cast<ptrdiff_t>(idx) + 1, nn);
        p->child_leaves.insert(p->child_leaves.begin() + static_cast<ptrdiff_t>(idx) + 1, nullptr);
        if (p->keys.size() >= kInternalFanout) { path.pop_back(); indices.pop_back(); SplitInternal(p, path, indices); }
    }
}
inline void BPlusTree::Insert(const std::string& key, const std::string& value, uint64_t timestamp) {
    std::vector<InternalNode*> path;
    std::vector<uint32_t> indices;
    LeafPage* leaf = FindLeafForWrite(key, path, indices);
    uint32_t pos;
    bool large = (value.size() > kPageSize / 2);

    if (leaf->Find(key, pos)) {
        size_t es = key.size() + value.size() + Config::kMemTableEntryOverheadBytes;
        if (!leaf->InsertEntry(pos, key, value, timestamp, large)) {
            SplitLeaf(leaf, path.back(), indices.back(), path, indices);
            leaf = FindLeafForWrite(key, path, indices);
            leaf->Find(key, pos);
            leaf->InsertEntry(pos, key, value, timestamp, large);
        }
        memory_usage_ += es;
        return;
    }

    if (!leaf->InsertEntry(pos, key, value, timestamp, large)) {
        SplitLeaf(leaf, path.back(), indices.back(), path, indices);
        leaf = FindLeafForWrite(key, path, indices);
        leaf->Find(key, pos);
        leaf->InsertEntry(pos, key, value, timestamp, large);
    }
    count_++;
    memory_usage_ += key.size() + value.size() + Config::kMemTableEntryOverheadBytes;
}
inline bool BPlusTree::Lookup(const std::string& key, uint64_t read_ts, std::string& value_out) const {
    LeafPage* leaf = FindLeaf(key);
    if (!leaf) return false;
    uint32_t idx;
    if (!leaf->Find(key, idx)) return false;
    for (uint32_t i = idx; i < leaf->count; ++i) {
        if (std::string(leaf->Rec(i), leaf->KeyLen(i)) != key) break;
        if (leaf->Timestamp(i) <= read_ts) {
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
            kv.is_tombstone = kv.value.empty();
            out.push_back(std::move(kv));
        }
}

inline BPlusTree::MemTableWalk::MemTableWalk(const BPlusTree& tree)
    : leaf_(tree.first_leaf_) {
    while (leaf_ && leaf_->count == 0) leaf_ = leaf_->next;
    if (leaf_) Load();
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
    current_.is_tombstone = current_.value.empty();
}

} // namespace kvdb
