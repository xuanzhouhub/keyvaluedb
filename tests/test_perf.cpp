#include "kvdb/bptree.hpp"
#include <iostream>
int main() {
    kvdb::BPlusTree tree;
    std::string val(512, 'X');
    for (int i = 0; i < 300; ++i)
        tree.Insert("k"+std::to_string(i), val, i+1, false);
    std::cerr << "done\n";
}
