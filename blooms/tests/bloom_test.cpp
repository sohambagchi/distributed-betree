//
// Created by Aaditya Rangarajan on 10/31/23.
//
#include "../include/bloom.hpp"
#include <iostream>

int mod_10_hash(int key) {
    return key % 10;
}

int mod_9_hash(int key) {
    return key % 9;
}

int mod_8_hash(int key) {
    return key % 8;
}

int mod_7_hash(int key) {
    return key % 7;
}

int mod_6_hash(int key) {
    return key % 6;
}

int mod_5_hash(int key) {
    return key % 5;
}

int mod_4_hash(int key) {
    return key % 4;
}

int main() {
    simple_bloom_filter *sbf = new simple_bloom_filter(7, 1000, 0.01);
    sbf->sbf_param_check();
    sbf->sbf_set_hash_function(mod_10_hash);
    sbf->sbf_set_hash_function(mod_9_hash);
    sbf->sbf_set_hash_function(mod_8_hash);
    sbf->sbf_set_hash_function(mod_7_hash);
    sbf->sbf_set_hash_function(mod_6_hash);
    sbf->sbf_set_hash_function(mod_5_hash);
    sbf->sbf_set_hash_function(mod_4_hash);
    sbf->sbf_hash_element(121);
    sbf->sbf_param_check();
    std::cout << "\nElement exists? " << sbf->sbf_check_if_element_exists(124) << "\n";
}