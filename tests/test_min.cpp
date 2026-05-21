#include "kvdb/engine.hpp"
#include <iostream>
#include <filesystem>
namespace fs = std::filesystem;
int main() {
    std::string dir = "./test_mini4";
    fs::remove_all(dir);
    { 
        kvdb::LSMTreeEngine e(dir, 4096);
        for(int i=0;i<5000;++i) e.Insert("key_"+std::to_string(i),"value_"+std::to_string(i));
        e.WaitForPendingFlushes();
        std::cout << "WAIT_OK count=" << e.SSTableCount() << "\n" << std::flush;
    }
    std::cout << "DESTROY_OK\n" << std::flush;
    return 0;
}
