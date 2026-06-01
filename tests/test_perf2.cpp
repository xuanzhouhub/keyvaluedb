#define KVDB_PROFILE_TREE
#include "kvdb/bptree.hpp"
#include <iostream>

int main() {
    kvdb::BPlusTree tree;
    for (int i = 0; i < 20000; ++i)
        tree.Insert("k"+std::to_string(i), "v", i+1, false);
    tree.PrintProfile();
}
