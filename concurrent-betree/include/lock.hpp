//
// Created by Aaditya Rangarajan on 11/9/23.
//

#ifndef CONCURRENT_BETREE_LOCK_HPP
#define CONCURRENT_BETREE_LOCK_HPP
#include <cstdint>
#include <thread>
//! A default value, just in case we are unable to estimate number of threads.
#define DEFAULT_NUM_THREADS 48
//! This file is created to create a simple reader write lock class.
//! This will be used for concurrency control in our b-eps tree.

//! below structure has been created to avoid false sharing cache misses.
//! for more info, see https://en.wikipedia.org/wiki/False_sharing
struct reader {
    uint8_t count;
    uint8_t padding[127];
};

class ReaderWriterLock {
private:
    //! number of readers
    struct reader readers[DEFAULT_NUM_THREADS];
    //! number of writers (only 1 at a time)
    uint8_t writers;
    //! metadata regarding the number of possible threads on a system.
    unsigned int num_threads;
public:
    //! constructors
    ReaderWriterLock() {
        num_threads = std::thread::hardware_concurrency();
        writers = 0;
        struct reader temp;
        for (int i = 0; i < num_threads; i++) {
            temp.count = 0;
            readers[i] = temp;
        }
    }
    //! another constructor, just in the case the user decides they want to tell us num threads
    ReaderWriterLock(int num_threads) : num_threads(num_threads) {
        writers = 0;
        struct reader temp;
        for (int i = 0; i < num_threads; i++) {
            temp.count = 0;
            readers[i] = temp;
        }
    }

    //! interface to interact with the lock
    void acquire_read_lock(uint8_t thread_id);
    void acquire_write_lock(uint8_t thread_id);
};
#endif
