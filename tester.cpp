#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>

#include <thread>
#include <utility>

#define NUM_USERS 100
#define NUM_OPERATIONS 10
#define NUM_THREADS 4
#define NUM_DATABASE 3
#define NUKE 1

using namespace std;

thread threads[NUM_THREADS];

struct database_mapping_struct{
    unordered_map<string, vector<string>> user_to_row;    // TODO: Concurrent HashMap!
    unordered_map<string, vector<string>> row_to_user;
};

// TODO: Have a concurrent list to keep track of the active taints
// The moment you start nuke, you remove the relevant stuff from this list!

// TODO: We don't have to do full scan, just have two maps storing...
// (K-> row_id, V-> user_id)
// 3 -> [u1, u2, ...]

// (K-> user_id, V-> row_id)
// u1 -> [ row_id1, row_id2 ]
// u2 -> [ row_id2, row_id3 ]

// When a nuke comes in for u1, we check the rows it owns, and see if that row
// has other owners using the first map!
// unordered_map<string, string> database_maps[NUM_DATABASE];
database_mapping_struct database_maps[NUM_DATABASE];
string operations[NUM_DATABASE * 4] = {"mongo_read", "mongo_insert", "mongo_update", "mongo_del", "kv_read",
                                        "kv_insert", "kv_update", "kv_del", "sql_read", "sql_insert", "sql_update", "sql_del",};
string users [NUM_USERS];

/////// STATS //////////
////////////////////////

int total_mongo_reads = 0UL;
int total_mongo_inserts = 0UL;
int total_mongo_updates = 0UL;
int total_mongo_dels = 0UL;
int total_kv_reads = 0UL;
int total_kv_inserts = 0UL;
int total_kv_updates = 0UL;
int total_kv_dels = 0UL;
int total_sql_reads = 0UL;
int total_sql_inserts = 0UL;
int total_sql_updates = 0UL;
int total_sql_dels = 0UL;

int mongo_reads[NUM_THREADS];
int mongo_inserts[NUM_THREADS];
int mongo_updates[NUM_THREADS];
int mongo_dels[NUM_THREADS];
int kv_reads[NUM_THREADS];
int kv_inserts[NUM_THREADS];
int kv_updates[NUM_THREADS];
int kv_dels[NUM_THREADS];
int sql_reads[NUM_THREADS];
int sql_inserts[NUM_THREADS];
int sql_updates[NUM_THREADS];
int sql_dels[NUM_THREADS];

////////////////////////
////////////////////////


void thread_function_start(int id){
    cout << "Thread " << id << " started execution" << endl;
    int random_op, random_user;

    for (int operation_count = 0; operation_count < NUM_OPERATIONS; operation_count++) {
        random_op = rand() / ((RAND_MAX + 1u) / (NUM_DATABASE * 4));    // TODO: not uniformly random
        random_user = rand() / ((RAND_MAX + 1u) / (NUM_USERS));

        if (operations[random_op] == "mongo_read") {
            mongo_reads[id]++;
            // call driver!
        }
        else if (operations[random_op] == "mongo_insert") {
            mongo_inserts[id]++;
            if (NUKE) {
                // find unique identifier (for user + for the database)
                // push to hashmap (k,v from above)
                // call driver
            }
            else {
                // call driver
            }
        }
        else if (operations[random_op] == "mongo_update") {
            mongo_updates[id]++;
        }
        else if (operations[random_op] == "mongo_del") {
            mongo_dels[id]++;
        }
        else if (operations[random_op] == "kv_read") {
            kv_reads[id]++;
            // call driver!
        }
        else if (operations[random_op] == "kv_insert") {
            kv_inserts[id]++;
        }
        else if (operations[random_op] == "kv_update") {
            kv_updates[id]++;
        }
        else if (operations[random_op] == "kv_del") {
            kv_dels[id]++;
        }
        else if (operations[random_op] == "sql_read") {
            sql_reads[id]++;
            // call driver!
        }
        else if (operations[random_op] == "sql_insert") {
            sql_inserts[id]++;
        }
        else if (operations[random_op] == "sql_update") {
            sql_updates[id]++;
        }
        else if (operations[random_op] == "sql_del") {
            sql_dels[id]++;
        }
    }

    cout << "Thread " << id << " done" << endl;
}

int main(){
    // initialize random users
    int start_id = rand() / 2;
    for (int i = 0; i < NUM_USERS; i++) {
        users[i] = to_string(start_id + i);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = thread(thread_function_start, i);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        total_mongo_reads += mongo_reads[i];
        total_mongo_inserts += mongo_inserts[i];
        total_mongo_updates += mongo_updates[i];
        total_mongo_dels += mongo_dels[i];
        total_kv_reads += kv_reads[i];
        total_kv_inserts += kv_inserts[i];
        total_kv_updates += kv_updates[i];
        total_kv_dels += kv_dels[i];
        total_sql_reads += sql_reads[i];
        total_sql_inserts += sql_inserts[i];
        total_sql_updates += sql_updates[i];
        total_sql_dels += sql_dels[i];
    }

    // STATS Aggreagation
    cout << setw(25) << left << "Total mongo_reads " << total_mongo_reads << endl;
    cout << setw(25) << left << "Total mongo_inserts " << total_mongo_inserts << endl;
    cout << setw(25) << left << "Total mongo_updates " << total_mongo_updates << endl;
    cout << setw(25) << left << "Total mongo_dels " << total_mongo_dels << endl;
    cout << setw(25) << left << "Total kv_reads " << total_kv_reads << endl;
    cout << setw(25) << left << "Total kv_inserts " << total_kv_inserts << endl;
    cout << setw(25) << left << "Total kv_updates " << total_kv_updates << endl;
    cout << setw(25) << left << "Total kv_dels " << total_kv_dels << endl;
    cout << setw(25) << left << "Total sql_reads " << total_sql_reads << endl;
    cout << setw(25) << left << "Total sql_inserts " << total_sql_inserts << endl;
    cout << setw(25) << left << "Total sql_updates " << total_sql_updates << endl;
    cout << setw(25) << left << "Total sql_dels " << total_sql_dels << endl;

    cout << setw(25) << left << "Total operations " << total_mongo_reads + total_mongo_inserts + total_mongo_updates + total_mongo_dels + total_kv_reads + total_kv_inserts + total_kv_updates + total_kv_dels + total_sql_dels + total_sql_inserts + total_sql_reads + total_sql_updates << endl;
}