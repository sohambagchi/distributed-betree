CXX = g++
CXXFLAGS = -std=c++11 -lpthread -Wall

all: main

main: main.o
	$(CXX) $(CXXFLAGS) $^ -o $@

main.o: main.cpp include/betree.hpp include/lock.hpp lock.o
	$(CXX) $(CXXFLAGS) -c $< -o $@

lock.o: src/lock.cpp include/lock.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf *.o main