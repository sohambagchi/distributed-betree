//
// Created by Aaditya Rangarajan on 11/10/23.
//
#include <iostream>
#include <thread>
#include <vector>
#include "betree.hpp"
#include <unordered_map>

void insertTest(betree &kvstore, uint64_t key, uint64_t value, int iterations) {
    for (uint64_t i = 0; i < iterations; ++i) {
        kvstore.insert(key + i, value + i);
        std::this_thread::yield();  // Give up the rest of the thread's time slice
    }
}

void readTest(betree &kvstore, uint64_t key, int iterations) {
    for (uint64_t i = 0; i < iterations; ++i) {
        uint64_t value = kvstore.query(key);
        std::cout << "Read: Key = " << key + i << ", Value = " << value << std::endl;
        std::this_thread::yield();  // Give up the rest of the thread's time slice
    }
}

int main() {
    betree kvstore;

    const int iterations = 1000;
    const int numThreads = 5;

    // Create threads for insert and read operations
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back(insertTest, std::ref(kvstore), (uint64_t) i * iterations, (uint64_t) i * iterations, iterations);
        threads.emplace_back(readTest, std::ref(kvstore), (uint64_t) i * iterations, iterations);
    }

    // Join threads
    for (auto &thread: threads) {
        thread.join();
    }

    return 0;
}
