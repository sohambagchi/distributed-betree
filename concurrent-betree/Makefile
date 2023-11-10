CXX = g++
CXXFLAGS = -std=c++11 -Wall -Iinclude
LDFLAGS = -lpthread

all: insertion_test

insertion_test: insertion_test.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ -o $@

insertion_test.o: tests/insertion_test.cpp include/betree.hpp include/lock.hpp lock.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $< -o $@

lock.o: src/lock.cpp include/lock.hpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $< -o $@

clean:
	rm -rf *.o main
