#include "backing_store.hpp"
#include <iostream>
#include <ext/stdio_filebuf.h>
#include <unistd.h>
#include <cassert>
#include <sys/ipc.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <fstream>

/////////////////////////////////////////////////////////////
// Implementation of the one_file_per_object_backing_store //
/////////////////////////////////////////////////////////////
one_file_per_object_backing_store::one_file_per_object_backing_store(std::string rt)
        : root(rt) {}

//allocate space for a new version of an object
//requires that version be >> any previous version
//logic for this is now handled by the swap space
void one_file_per_object_backing_store::allocate(uint64_t obj_id, uint64_t version) {
    //uint64_t id = nextid++;
    std::string filename = get_filename(obj_id, version);
    std::fstream dummy(filename, std::fstream::out);
    dummy.flush();
    assert(dummy.good());
    //return id;
}


//delete the file associated with an specific version of a node
void one_file_per_object_backing_store::deallocate(uint64_t obj_id, uint64_t version) {
    std::string filename = get_filename(obj_id, version);
    key_t key = ftok(filename.c_str(), 0);
    if (key == -1) {
        // error
    }
    int block_id = shmget(key, 100, 0644 | IPC_CREAT);
    if (block_id == -1) {
        // error
    }
    void* shared_block = shmat(block_id, nullptr, 0);
    if (shared_block == nullptr) {
        // error
    }
    shmdt(shared_block);
}

//return filestream corresponding to an item. Needed for deserialization.
void *one_file_per_object_backing_store::get(uint64_t obj_id, uint64_t version) {
    __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;

    std::string filename = get_filename(obj_id, version);
    key_t key = ftok(filename.c_str(), 0);
    if (key == -1) {
        // error
    }
    int block_id = shmget(key, 100, 0644 | IPC_CREAT);
    if (block_id == -1) {
        // error
    }
    void* shared_block = shmat(block_id, nullptr, 0);
    if (shared_block == nullptr) {
        // error
    }
    block_id_to_file_mapping[shared_block] = filename;
    return shared_block;
}

//push changes from iostream and close.
void one_file_per_object_backing_store::put(void *shared_memory_block) {
    auto it = block_id_to_file_mapping.find(shared_memory_block);
    if (it == block_id_to_file_mapping.end()) {
        // error
    }
    std::ofstream file(it->second, std::ios::binary);
    file.write(static_cast<const char *>(shared_memory_block), sizeof (shared_memory_block));
}


//Given an object and version, return the filename corresponding to it.
std::string one_file_per_object_backing_store::get_filename(uint64_t obj_id, uint64_t version) {

    return root + "/" + std::to_string(obj_id) + "_" + std::to_string(version);

}
