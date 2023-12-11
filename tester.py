# from nuke_core.py import nuke_ownership_add, nuke_ownership_remove, nuke, do_nuke
import multiprocessing
import string
import time
import argparse
import random

import redis
import sqlite3
from pymongo import MongoClient


operations = ["mongo_read", "mongo_insert", "mongo_update", "mongo_del", "kv_read", 
              "kv_insert", "kv_update", "kv_del", "sql_read", "sql_insert", "sql_update", "sql_del"]
users = [None]

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def db_read(row_id, db_id, redis_client, cursor, mongo_db):
    if db_id == 0:      # redis
        redis_client.get(row_id)
    elif db_id == 1:    # sql
        cursor.execute(f"SELECT * FROM throughput_test WHERE \"id\" = ?;", (row_id,))
    elif db_id == 2:    # mongo
        mongo_db.main.find_one({"_id": row_id})

def db_insert(row_id, db_id, redis_client, cursor, mongo_db):
    if db_id == 0:      # redis
        redis_client.set(row_id, random_string(5))
    elif db_id == 1:    # sql
        cursor.execute(f"INSERT INTO throughput_test (id, content) VALUES (?, ?)", (row_id, random_string(5)))
    elif db_id == 2:    # mongo
        mongo_db.main.insert_one({"_id": row_id, "content": random_string(5)})

def db_update(row_id, db_id, redis_client, cursor, mongo_db):
    if db_id == 0:      # redis
        redis_client.set(row_id, random_string(5))
    elif db_id == 1:    # sql
        cursor.execute(f"UPDATE throughput_test SET content = ? WHERE id = ?", (random_string(5), row_id))
    elif db_id == 2:    # mongo
        mongo_db.main.replace_one({"_id": row_id}, {"content": random_string(5)}, False)
        pass

def thread_function_start(thread_id, queue, NUM_OPERATIONS, NUM_USERS, nuke_flag):
    print(f'Thread {thread_id} started execution...')

    # Should each thread establish a seperate connection to DBs?
    con = sqlite3.connect("throughput_sql.db")
    cursor = con.cursor()

    mongo_client = MongoClient()
    mongo_db = mongo_client["throughput_mongo"]

    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    thread_result = {"kv_read": 0, "kv_insert": 0, "kv_update": 0, "kv_del": 0, "sql_read": 0, "sql_insert": 0,
                     "sql_update": 0, "sql_del": 0, "mongo_read": 0, "mongo_insert": 0, "mongo_update": 0, "mongo_del": 0}
    mongo_ids = []
    sql_ids = []
    kv_ids = []
    prev_ids = [thread_id * NUM_OPERATIONS + 1, thread_id * NUM_OPERATIONS + 1, thread_id * NUM_OPERATIONS + 1]

    # TODO: Preload database here
    do_operation(prev_ids[0], 1, 0, redis_client, cursor, mongo_db, nuke_flag)
    do_operation(prev_ids[1], 1, 1, redis_client, cursor, mongo_db, nuke_flag)
    do_operation(prev_ids[2], 1, 2, redis_client, cursor, mongo_db, nuke_flag)
    

    for i in range(NUM_OPERATIONS):
        random_db = random.randint(0, 2)
        random_op = random.randint(0, 3)

        if random_op == 1:  # if insert
            random_id = prev_ids[random_db] + 1
            prev_ids[random_db] += 1
        else:
            # TODO: filter for deletes
            random_id = random.randint(0, prev_ids[random_db])
            while random_id == 0:
                random_id = random.randint(0, prev_ids[random_db])
            
        operation = operations[random_db * 4 + random_op]
        
        do_operation(random_id, random_op, random_db, redis_client, cursor, mongo_db, nuke_flag)

        thread_result[operation] += 1

    queue.put(thread_result)
    print(f'Thread {thread_id} exiting...')

def do_operation(random_id, random_op, random_db, redis_client, cursor, mongo_db, nuke_flag):
    # print_table(cursor)
    # print_mongo_collection(mongo_db)
    # print_redis_db(redis_client)

    if nuke_flag:
        if random_op == 0:
            db_read(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 1:
            # TODO: multiple owners? - must do for correctness
            # nuke_ownership_update_add(random.randint(0, NUM_USERS), random_id, random_db)
            db_insert(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 2:
            db_update(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 3:
            # TODO: Should we include point deletes as well?
            # It's probably better for our purposes cause we don't add overhead
            pass
    else:
        if random_op == 0:
            db_read(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 1:
            db_insert(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 2:
            db_update(random_id, random_db, redis_client, cursor, mongo_db)
        elif random_op == 3:
            # TODO: point deletes?
            pass
    
    # print()

def main():
    thread_pool = []
    number_threads = 4
    thread_results = [0] * number_threads 

    parser = argparse.ArgumentParser()
    parser.add_argument('--nuke', action="store_true", help='runs the throughput test with nuking')
    parser.add_argument('-n', '--num-operations', type=int, default=100, help='number of operations to perform')
    args = parser.parse_args()

    NUM_OPERATIONS = args.num_operations
    print("Number of operations: ", NUM_OPERATIONS)
    print("Are we nuking? ", args.nuke)

    # initialize DBs
    con = sqlite3.connect("throughput_sql.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS throughput_test;")

    mongo_client = MongoClient()
    mongo_db = mongo_client["throughput_mongo"]
    mongo_db.main.delete_many({})

    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS throughput_test (
        id INTEGER PRIMARY KEY,
        content TEXT
                );''')
    
    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    redis_client.flushall()

    queue = multiprocessing.Queue()

    start_time = time.time()

    for i in range(number_threads):
        thread = multiprocessing.Process(target=thread_function_start, args=(i, queue, NUM_OPERATIONS, 4, args.nuke))
        
        thread.start()
        thread_pool.append(thread)

    for thread in thread_pool:
        print(queue.get())
        thread.join()
    
    end_time = time.time()

    # cleanup DBs
    cur.execute("DROP TABLE IF EXISTS throughput_test;")
    redis_client.flushall()

    # print the execution time
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
    print(f"{NUM_OPERATIONS * number_threads / execution_time} operations per second")

if __name__ == "__main__":
    main()


"""
if args.nuke:
        print("nuking")
    else:
        print("normalling")


def perform_database_operation(operation_id):
    # Connect to the database
    conn = sqlite3.connect('your_database.db')
    cursor = conn.cursor()

    # Perform the database operation
    # Replace this with your actual database operation code
    time.sleep(1)  # Simulating some database operation

    # Close the database connection
    conn.close()

    # Return the result of the operation
    return f"Operation {operation_id} completed"
"""

# testing for multiprocessing

def print_table(cursor):
    cursor.execute("SELECT * FROM throughput_test")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

def print_mongo_collection(mongo_db):
    documents = mongo_db.main.find()

    for doc in documents:
        print(doc)

def print_redis_db(redis_db):
    keys = redis_db.keys()

    for key in keys:
        value = redis_db.get(key)
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