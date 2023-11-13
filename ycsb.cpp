//
// Created by Aaditya Rangarajan on 10/31/23.
//
// file to setup YCSB for odyssey KVS.
#include <iostream>
#include <cstdio>
#include <fstream>
#include <chrono>
#include <thread>

#include "od_interface.h"
#include "od_kvs.h"

struct ycsb_operations {
    int op;
    uint8_t key;
    uint8_t value;
};


enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_D,
    WORKLOAD_F
};

enum {
    I,
    R,
    U
};

enum {
    UNIFORM,
    ZIPFIAN,
};

void run_yscb_workload(int workload) {
    std::string load_file_path;
    // need to create a folder to store data
    if (workload == WORKLOAD_A) {
        // load the files needed for workload A
        load_file_path = "../data/workloadA.dat";
    } else if (workload == WORKLOAD_B) {
        load_file_path = "../data/workloadB.dat";
    } else if (workload == WORKLOAD_D) {
        load_file_path = "../data/workloadD.dat";
    } else if(workload == WORKLOAD_F) {
        load_file_path = "../data/workloadF.data";
    }

    std::ifstream load_file(load_file_path);
    std::string operation;
    uint8_t key;
    uint8_t value; // for updates and inserts only
    std::vector<ycsb_operations> ycsb_ops;
    std::string insert("I");
    std::string read("R");
    std::string update("U");
    int count = 0;
    while (load_file.good()) {
        struct ycsb_operations ops = {};
        load_file >> operation >> key;
        if (operation != insert) {
            ops.op = I;
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        load_file >> value;
        ops.key = key;
        ops.value = value;
        ycsb_ops.push_back(ops);
        count++;
    }
    count--;

    fprintf(stderr, "Loaded %d keys\n", count);

    mica_kv_t *kvs;
    custom_mica_init(0);
    // std::vector<std::thread> workers;
    auto starttime = std::chrono::system_clock::now();
    // this is where our operations go
    auto func = [&] () {
        for (auto &it: ycsb_ops) {
            if (it.op == I) {
                blocking_write(it.key, &it.value, sizeof (it.value), 0);
            } else if(it.op == R) {
                blocking_read(it.key, &it.key, sizeof (it.key), 0);
            }
        }
    };
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "Usage: ./ycsb [workload]\n";
        std::cout << "These are the workloads available: a, b, d and f.\n";
        return 1;
    }
    printf("Your chosen workload is %s\n", argv[1]);
    int workload;
    if (strcmp(argv[1], "a") == 0) {
        workload = WORKLOAD_A;
    } else if (strcmp(argv[1], "b") == 0) {
        workload = WORKLOAD_B;
    } else if (strcmp(argv[1], "d") == 0) {
        workload = WORKLOAD_D;
    } else if (strcmp(argv[1], "f") == 0) {
        workload = WORKLOAD_F;
    } else {
        fprintf(stderr, "Unknown workload %s\n", argv[1]);
        exit(1);
    }
    // todo: the actual ycsb run
    run_yscb_workload(workload);
    mica_kv_t kvs;
}
