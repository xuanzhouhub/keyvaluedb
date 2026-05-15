#include "kvdb/engine.hpp"
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    std::string dir = "./test_restart_data";
    fs::remove_all(dir);

    int failures = 0;

    std::cout << "--- Phase 1: Write ---" << std::endl;
    {
        kvdb::LSMTreeEngine engine(dir, 4 * 1024 * 1024);

        for (int i = 1; i <= 50; ++i)
            engine.Insert("k" + std::to_string(i), "v" + std::to_string(i));

        engine.Flush();

        std::cout << "  SSTableCount: " << engine.SSTableCount() << std::endl;
        std::cout << "  MemTableEntryCount: " << engine.ActiveMemTableEntryCount() << std::endl;
        std::cout << "  HasWALData: " << engine.HasWALData() << std::endl;

        std::string val;
        bool found = engine.Lookup("k25", val);
        std::cout << "  Lookup k25: " << (found ? val : "NOT FOUND") << std::endl;
        if (!found || val != "v25") {
            std::cerr << "FAIL: Phase1 lookup" << std::endl;
            failures++;
        }

        std::cout << "  (destroying engine)" << std::endl;
    }

    std::cout << "\n--- Phase 2: Restart Recovery ---" << std::endl;
    {
        kvdb::LSMTreeEngine engine(dir, 4 * 1024 * 1024);

        std::cout << "  SSTableCount: " << engine.SSTableCount() << std::endl;
        std::cout << "  MemTableEntryCount: " << engine.ActiveMemTableEntryCount() << std::endl;
        std::cout << "  HasWALData: " << engine.HasWALData() << std::endl;

        std::string val;
        bool found = engine.Lookup("k25", val);
        std::cout << "  Lookup k25: " << (found ? val : "NOT FOUND") << std::endl;
        if (!found || val != "v25") {
            std::cerr << "FAIL: Phase2 lookup" << std::endl;
            failures++;
        }
    }

    fs::remove_all(dir);

    if (failures == 0)
        std::cout << "\nPASSED" << std::endl;
    else
        std::cout << "\nFAILED with " << failures << " errors" << std::endl;
    return failures;
}
