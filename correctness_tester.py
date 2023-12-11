import string
import argparse
import random
import time
import sys
import os

import redis
import sqlite3
from pymongo import MongoClient
from nuke import nuke_ownership_update_add, nuke, do_nuke

operations = ["mongo_read", "mongo_insert", "mongo_update", "mongo_del", "kv_read", 
              "kv_insert", "kv_update", "kv_del", "sql_read", "sql_insert", "sql_update", "sql_del"]
users = [None]

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def db_read(row_id, db_id):
    if db_id == 0:      # redis
        redis_client.get(row_id)
    elif db_id == 2:    # sql
        cursor.execute(f"SELECT * FROM correctness_test WHERE \"id\" = ?;", (row_id,))
    elif db_id == 1:    # mongo
        mongo_db.main.find_one({"_id": row_id})

def db_insert(row_id, db_id):
    if db_id == 0:      # redis
        redis_client.set(row_id, random_string(5))
    elif db_id == 2:    # sql
        cursor.execute(f"INSERT INTO correctness_test (id, content) VALUES (?, ?)", (row_id, random_string(5)))
    elif db_id == 1:    # mongo
        mongo_db.main.insert_one({"_id": row_id, "content": random_string(5)})

def db_update(row_id, db_id):
    if db_id == 0:      # redis
        redis_client.set(row_id, random_string(5))
    elif db_id == 2:    # sql
        cursor.execute(f"UPDATE correctness_test SET content = ? WHERE id = ?", (random_string(5), row_id))
    elif db_id == 1:    # mongo
        mongo_db.main.replace_one({"_id": row_id}, {"content": random_string(5)}, False)

def db_delete(row_id, db_id):
    if db_id == 0:      # redis
        redis_client.delete(row_id)
    elif db_id == 2:    # sql
        cursor.execute(f"DELETE FROM correctness_test WHERE id = ?", (row_id,))
    elif db_id == 1:    # mongo
        mongo_db.main.delete_one({"_id": row_id})

def start():
    ownerships = {}
    mongo_keys = []
    sql_keys = []
    kv_keys = []

    # initialize ownerships
    for i in range(NUM_USERS + 1):
        ownerships[i] = []

    # TODO: Preload database here
    for i in range(3):          # 3 databases
        for j in range(10):     # preload 10 rows
            random_id = pick_unique_random_id(mongo_keys, sql_keys, kv_keys, i)
            random_user = random.randint(0, NUM_USERS)
            ownerships[random_user].append(random_id)
            do_operation(random_id, 1, i, random_user)
    
    for i in range(NUM_OPERATIONS):
        random_db = random.randint(0, 2)
        random_user = random.randint(0, NUM_USERS)
        random_op = random.choices([0, 1, 2, 3, 4], weights=[0, 50, 40, 1, 5], k=1)[0]

        if random_db == 0:
            random_id = random.choice(kv_keys)
        elif random_db == 1:
            random_id = random.choice(mongo_keys) + NUM_OPERATIONS
        else:
            random_id = random.choice(sql_keys) + 2 * NUM_OPERATIONS

        if random_op == 1:  # if insert
            random_id = pick_unique_random_id(mongo_keys, sql_keys, kv_keys, random_db)
            ownerships[random_user].append(random_id)
        elif random_op == 3:  # if ownership update
            ownerships[random_user].append(random_id)
        elif random_op == 4:  # if delete
            while len(ownerships[random_user]) == 0:
                random_user = random.randint(0, NUM_USERS)
            random_id = random.choice(ownerships[random_user])
            ownerships[random_user].remove(random_id)
                    
        do_operation(random_id, random_op, random_db, random_user)

    return ownerships

def pick_unique_random_id(mongo_keys, sql_keys, kv_keys, random_db):
    random_id = random.randint(0, NUM_OPERATIONS)
    if random_db == 0:
        while random_id in kv_keys:
            random_id = random.randint(0, NUM_OPERATIONS)
        kv_keys.append(random_id)
    elif random_db == 1:
        while random_id in mongo_keys:
            random_id = random.randint(0, NUM_OPERATIONS)
        mongo_keys.append(random_id)
    else:
        while random_id in sql_keys:
            random_id = random.randint(0, NUM_OPERATIONS)
        sql_keys.append(random_id)

    return random_id

def do_operation(random_id, random_op, random_db, random_user):
    # print_table(cursor)
    # print_mongo_collection(mongo_db)
    # print_redis_db(redis_client)

    if random_op == 0:
        db_read(random_id, random_db)
    elif random_op == 1:
        # TODO: multiple owners? - must do for correctness
        if nuke_ownership_update_add(random_user, random_db, random_db, random_id):
            db_insert(random_id, random_db)
        else:
            print("User is nuked.")
    elif random_op == 2:
        db_update(random_id, random_db)
    elif random_op == 3:
        if nuke_ownership_update_add(random_user, random_db, random_db, random_id):
            pass
        else:
            print("User is nuked.")
    elif random_op == 4:
        db_delete(random_id, random_db)
    
    # print()

def main():
    global NUM_USERS
    global NUM_OPERATIONS
    global mongo_db
    global redis_client
    global cursor

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num-operations', type=int, default=100, help='number of operations to perform')
    args = parser.parse_args()

    NUM_USERS = 1000
    NUM_OPERATIONS = args.num_operations
    print("Number of operations: ", NUM_OPERATIONS)

    # initialize DBs
    con = sqlite3.connect("correctness_test.db", check_same_thread=False)
    cursor = con.cursor()
    cursor.execute("DROP TABLE IF EXISTS correctness_test;")

    mongo_client = MongoClient()
    mongo_db = mongo_client["correctness_mongo"]
    mongo_db.main.delete_many({})

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS correctness_test (
        id INTEGER PRIMARY KEY,
        content TEXT
                );''')
    
    redis_client = redis.Redis(host="localhost", port=6379, db=9, decode_responses=True)
    redis_client.flushall()

    ownerships = start()
    # print(ownerships)

    # TODO: nuklings
    users_to_nuke = []
    for i in range(NUM_USERS // 5):
        random_user = random.randint(0, NUM_USERS)
        users_to_nuke.append(random_user)
        nuke(random_user)

    con.close()

    # changing stdout to devnull to suppress print statements in nuke
    original_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')

    start_time = time.time()
    do_nuke()
    end_time = time.time()

    sys.stdout = original_stdout

    print(f"Execution time of nuke: {end_time - start_time} seconds")

    con = sqlite3.connect("correctness_test.db", check_same_thread=False)
    cursor = con.cursor()

    print("Users being nuked are: ", users_to_nuke)
    # print_table()
    # print_mongo_collection()
    # print_redis_db()

    print_results(users_to_nuke, ownerships)

    # cleanup DBs
    cursor.execute("DROP TABLE IF EXISTS correctness_test;")
    redis_client.flushall()
    mongo_db.main.delete_many({})

def print_results(nuked_users, ownerships):
    for user in nuked_users:
        for row_id in ownerships[user]:
            if row_id < NUM_OPERATIONS:
                cursor.execute(f"SELECT * FROM correctness_test WHERE id = ?;", (row_id,))
                rows = cursor.fetchall()
                if len(rows) != 0:
                    print("nuke left hanging data in sql :(")
            elif row_id < 2 * NUM_OPERATIONS:
                hanging = mongo_db.main.find_one({"_id": row_id})
                if hanging != None:
                    print("nuke left hanging data in mongo :(")
            else:
                hanging = redis_client.get(row_id)
                if hanging != None:
                    print("nuke left hanging data in redis :(")

    print("No hanging data left behind :)")
    print("Correctness test passed with flying colors!")

######
# debugging helpers
def print_table():
    cursor.execute("SELECT * FROM correctness_test")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

def print_mongo_collection():
    documents = mongo_db.main.find()

    for doc in documents:
        print(doc)

def print_redis_db():
    keys = redis_client.keys()

    for key in keys:
        value = redis_client.get(key)
        print(f"{key}: {value}")

# thread = threading.Thread(target=dummy_seq, args=(thread_results, i))
def dummy(results, thread_id, queue):
    count = 0
    
    for i in range(100000000):
        count += 1

    queue.put(count)
    
def dummy_seq(results, thread_id):
    count = 0
    
    for i in range(100000000):
        count += 1
    
    results[thread_id] = count

if __name__ == "__main__":
    main()