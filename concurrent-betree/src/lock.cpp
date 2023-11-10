//
// Created by Aaditya Rangarajan on 11/10/23.
//
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#include "lock.hpp"

void ReaderWriterLock::acquire_read_lock(uint8_t thread_id) {
    //! try to acquire read lock
    while (true) {
        __atomic_fetch_add(&this->readers[thread_id % num_threads].count, 1, __ATOMIC_SEQ_CST);
        //! check if there are any writers
        //! if there are, spin.
        if (this->writers) {
            //! give priority to the writer
            __atomic_fetch_add(&this->readers[thread_id % num_threads].count, -1, __ATOMIC_SEQ_CST);
            //! spin
            while (this->writers);
        } else {
            //! read lock acquired
            return;
        }
    }
}

void ReaderWriterLock::acquire_write_lock() {
    //! try to acquire write lock
    while(__sync_lock_test_and_set(&this->writers, 1))
        //! if not acquired, wait for other writer to finish
        while(this->writers);
    //! spin while readers are present
    while (readers_are_present());
    //! acquired
    return;
}

void ReaderWriterLock::release_read_lock(uint8_t thread_id) {
    //! reduce our count and exit
    __atomic_fetch_add(&this->readers[thread_id % num_threads].count, -1, __ATOMIC_SEQ_CST);
    return;
}

void ReaderWriterLock::release_write_lock() {
    //! release and return
    __sync_lock_release(&this->writers);
    return;
}