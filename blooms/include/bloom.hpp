//
// Created by Aaditya Rangarajan on 10/31/23.
//

#include <cstdint>
#include <cmath>
#include <cstdio>
#include <functional>
#ifndef BLOOMS_BLOOM_HPP
#define BLOOMS_BLOOM_HPP

class simple_bloom_filter {
private:
    uint8_t number_of_hash_functions; //k
    uint8_t number_of_bits; //m
    uint16_t number_of_entries; //n
    double error_rate; //epsilon
    std::vector<std::function<int(int)>> hashes;
    std::vector<int> bloom_filter;
public:
    simple_bloom_filter() = default;
    ~simple_bloom_filter() {}
    simple_bloom_filter(uint8_t num_hashes, uint16_t num_entries, double error){
        this->number_of_hash_functions = num_hashes;
        this->number_of_entries = num_entries;
        this->error_rate = error;
        this->number_of_bits = -1 * (num_entries * log(error)) / pow(log(2.0), 2);
        this->bloom_filter.resize(this->number_of_bits, 0);
    }
    void sbf_param_check() {
        printf("Hashes: %hhu, Entries: %hu, Error rate: %f, Number Of bits: %hhu\n", this->number_of_hash_functions, this->number_of_entries, this
        ->error_rate, this->number_of_bits);
        for (auto it: this->bloom_filter) {
            printf("%d ", it);
        }
        printf("\n");
    }

    void sbf_set_hash_function(std::function<int(int)> hash_function) {
        // set hash functions
        this->hashes.push_back(hash_function);
    }

    void sbf_hash_element(int key) {
        int hash_val;
        for (auto it: this->hashes) {
            hash_val = it(key);
            printf("Output of hash function is %d\n", hash_val);
            bloom_filter[hash_val] = 1;
        }
    }

    bool sbf_check_if_element_exists(int key) {
        bool does_exist = false;
        int hash_val;
        for (auto it: this->hashes) {
            hash_val = it(key);
            if (this->bloom_filter[hash_val] == 0) {
                does_exist = false;
                break;
            }
            else {
                does_exist = true;
            }
        }
        return does_exist;
    }
};

#endif //BLOOMS_BLOOM_HPP
