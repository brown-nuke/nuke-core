#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>

#include <thread>
#include <utility>

#include <unordered_map>
#include <vector>
#include <set>
#include <algorithm>

#define NUM_USERS 100
#define NUM_OPERATIONS 10
#define NUM_THREADS 4
#define NUM_DATABASE 3
#define NUKE 1
#define NUKE_CNT 5

using namespace std;

thread threads[NUM_THREADS];

struct database_mapping_struct{
    unordered_map<string, vector<string>> user_to_row;    // TODO: Concurrent Map! or use persisted KV store
    unordered_map<string, vector<string>> row_to_user;
};

// user id to how many times nuked
// if exceeds NUKE_CNT, then remove from users_to_nuke
unordered_map<string, int> users_to_nuke; // TODO: Concurrent Map! or use persisted KV store

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
string operations[NUM_DATABASE * 4] = {
    "mongo_read", "mongo_insert", "mongo_update", "mongo_del",
    "kv_read", "kv_insert", "kv_update", "kv_del",
    "sql_read", "sql_insert", "sql_update", "sql_del"};
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

// retruns true if we can proceed with the insert
bool ownership_update_add(string user_id, string row_id, int database_id) {
    if (users_to_nuke.find(user_id) != users_to_nuke.end()) {
        return false;
    }
    
    database_maps[database_id].user_to_row[user_id].push_back(row_id);
    database_maps[database_id].row_to_user[row_id].push_back(user_id);

    return true;
}

// retruns true if the row is no longer owned by anyone
bool ownership_update_remove(string user_id, string row_id, int database_id) {
    auto vec1 = database_maps[database_id].user_to_row[user_id];
    for (size_t i = 0; i < vec1.size(); i++) {
        if (vec1[i] == row_id) {
            vec1.erase(vec1.begin() + i);
            break;
        }
    }

    // this will be handled by the do_nuke function to not break the iterator
    // if (vec1.size() == 0) {
    //     database_maps[database_id].user_to_row.erase(user_id);
    // }
    
    
    auto vec2 = database_maps[database_id].row_to_user[user_id];
    for (size_t i = 0; i < vec2.size(); i++) {
        if (vec2[i] == row_id) {
            vec2.erase(vec2.begin() + i);
            break;
        }
    }
    if (vec2.size() == 0) {
        database_maps[database_id].row_to_user.erase(row_id);
        return true;
    }

    return false;
}

void nuke(string user_id, int database_id) {
    users_to_nuke[user_id] = 0;
}

void delete_row(string row_id, int database_id) {
    if (database_id == 0) {
        // TODO: mongo db delete
    } else if (database_id == 1) {
        // TODO: kv db delete
    } else if (database_id == 2) {
        // TODO: sql db delete
    }
}

void do_nuke() {
    for (auto it = users_to_nuke.begin(); it != users_to_nuke.end(); it++) {                          // iterate all users to nuke
        for (size_t i = 0; i < NUM_DATABASE; i++) {                                                   // in all databases
            if (database_maps[i].user_to_row.find(it->first) != database_maps[i].user_to_row.end()) { // if user exists in database
                for (auto row_id : database_maps[i].user_to_row[it->first]) {                         // for each row that user owns
                    if (ownership_update_remove(it->first, row_id, i)) {                              // remove user's ownership of row
                        delete_row(row_id, i);                                                        // row is no longer owned by anyone
                    }
                }
                database_maps[i].user_to_row.erase(it->first);                                         // remove user from database
            }
        }
        
        if (it->second++ > NUKE_CNT) {
            // we nuked this user enough times, remove from the map
            users_to_nuke.erase(it->first);
        }
    }
}


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