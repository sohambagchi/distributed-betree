//
// Created by Aaditya Rangarajan on 11/9/23.
//

#ifndef CONCURRENT_BETREE_LOCK_HPP
#define CONCURRENT_BETREE_LOCK_HPP
#include <cstdint>
#include <thread>
#include <cstdio>
#include <mutex>
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
    //! not to be accessed
    std::mutex count_mutex;
public:
    //! constructors
    ReaderWriterLock() {
        num_threads = std::thread::hardware_concurrency();
        printf("Number of threads supported by underlying hardware %d\n", num_threads);
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
    void acquire_write_lock();
    void release_read_lock(uint8_t thread_id);
    void release_write_lock();
    //! internal function, used for acquiring write lock
    bool readers_are_present() {
        //! variable to store the final count
        count_mutex.lock();
        uint8_t num_readers = 0;
        //! sum over all counters
	for (int i = 0; i < num_threads; i++) {
		num_readers += readers[i].count;
	}
        count_mutex.unlock();
        return num_readers > 0;
    }
};
#endif
