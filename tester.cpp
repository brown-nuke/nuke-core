#include <chrono>
#include <iostream>
#include <thread>
#include <utility>

#define MAX_THREADS 4


using namespace std;

thread threads[MAX_THREADS];

struct database{
    unordered_map our_local 
}
// we want a struct for each database that has:
//   the hash map