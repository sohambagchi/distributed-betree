#include <iostream>
#include "betree.hpp"
#include <cstdlib>
#include <ctime>

int main() {
    std::srand(std::time(nullptr));
    betree bt;
    int ran;
    int k;
    for (int i = 0; i < 100; i++) {
        ran = std::rand();
        k = ran/4;
        bt.insert(k, ran);
    }
    bt.dump_messages();
    return 0;
}
