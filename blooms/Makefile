CXXFLAGS = -std=c++17

all: bloom.o
	g++ $(CXXFLAGS) tests/bloom_test.cpp

bloom.o:
	g++ -c  $(CXXFLAGS) include/bloom.hpp

clean:
	rm *.o