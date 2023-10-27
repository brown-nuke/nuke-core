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
#define OP_CNT 4

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

/////// INDEX /////////
///////////////////////

// 0 -> mongo
// 1 -> kv
// 2 -> sql

// 0 -> read
// 1 -> insert
// 2 -> update
// 3 -> del

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

int op_counts[NUM_DATABASE][OP_CNT][NUM_THREADS];

///////// ROW OPERATIONS /////////
//////////////////////////////////

void read_row(string row_id, int database_id) {
    if (database_id == 0) {
        // TODO: mongo db read
    } else if (database_id == 1) {
        // TODO: kv db read
    } else if (database_id == 2) {
        // TODO: sql db read
    }
}

void insert_row(string row_id, int database_id) {
    if (database_id == 0) {
        // TODO: mongo db insert
    } else if (database_id == 1) {
        // TODO: kv db insert
    } else if (database_id == 2) {
        // TODO: sql db insert
    }
}

void update_row(string row_id, int database_id) {
    if (database_id == 0) {
        // TODO: mongo db update
    } else if (database_id == 1) {
        // TODO: kv db update
    } else if (database_id == 2) {
        // TODO: sql db update
    }
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

///////// OWNERSHIP /////////
/////////////////////////////

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
    if (database_maps[database_id].user_to_row.find(user_id) != database_maps[database_id].user_to_row.end()) {
        auto vec1 = database_maps[database_id].user_to_row[user_id];
        for (size_t i = 0; i < vec1.size(); i++) {
            if (vec1[i] == row_id) {
                vec1.erase(vec1.begin() + i);
                break;
            }
        }
    }

    // this will be handled by the do_nuke() function to not break the iterator
    // if (vec1.size() == 0) {
    //     database_maps[database_id].user_to_row.erase(user_id);
    // }
    
    if (database_maps[database_id].row_to_user.find(row_id) != database_maps[database_id].row_to_user.end()) {
        auto vec = database_maps[database_id].row_to_user[row_id];
        for (size_t i = 0; i < vec.size(); i++) {
            if (vec[i] == user_id) {
                vec.erase(vec.begin() + i);
                break;
            }
        }

        if (vec.size() == 0) {
            database_maps[database_id].row_to_user.erase(row_id);
            return true;
        }
    }

    return false;
}

void nuke(string user_id, int database_id) {
    users_to_nuke[user_id] = 0;
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

///////// SIMULATION /////////
/////////////////////////////

void row_op(string user_id, string row_id, int database_id, int op_id) {
    if (op_id == 0) {
        read_row(row_id, database_id);
    } else if (op_id == 1) {
        if (NUKE) {
            if (ownership_update_add(user_id, row_id, database_id) == false) {
                // only add data if there is no ongoing nuke
                insert_row(row_id, database_id);
            }
        } else {
            insert_row(row_id, database_id);
        }
    } else if (op_id == 2) {
        update_row(row_id, database_id);
    } else if (op_id == 3) {
        delete_row(row_id, database_id);
    }
}

void thread_function_start(int id){
    cout << "Thread " << id << " started execution" << endl;
    int random_db, random_op, random_user;

    for (int operation_count = 0; operation_count < NUM_OPERATIONS; operation_count++) {

        // TODO: not uniformly random
        random_db = rand() % NUM_DATABASE;
        random_op = rand() % OP_CNT;   
        random_user = rand() % NUM_USERS;

        row_op(users[random_user], to_string(rand()), random_db, random_op);
        op_counts[random_db][random_op][id]++;
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
        total_mongo_reads += op_counts[0][0][i];
        total_mongo_inserts += op_counts[0][1][i];
        total_mongo_updates += op_counts[0][2][i];
        total_mongo_dels += op_counts[0][3][i];
        total_kv_reads += op_counts[1][0][i];
        total_kv_inserts += op_counts[1][1][i];
        total_kv_updates += op_counts[1][2][i];
        total_kv_dels += op_counts[1][3][i];
        total_sql_reads += op_counts[2][0][i];
        total_sql_inserts += op_counts[2][1][i];
        total_sql_updates += op_counts[2][2][i];
        total_sql_dels += op_counts[2][3][i];
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

    cout << "Before nuke: " << endl;
    for (size_t i = 0; i < NUM_DATABASE; i++) {
        cout << database_maps[i].user_to_row.size() << " " << database_maps[i].row_to_user.size() << endl;
    }
    

    for (size_t i = 0; i < NUM_DATABASE; i++) {
        for (int j = 0; j < NUM_USERS; j++) {
            nuke(users[j], i);
        }
    }

    do_nuke();

    // ideally we should see all zeroes
    // but note that we use non concurrent maps
    cout << "After nuke: " << endl;
    for (size_t i = 0; i < NUM_DATABASE; i++) {
        cout << database_maps[i].user_to_row.size() << " " << database_maps[i].row_to_user.size() << endl;
    }
}