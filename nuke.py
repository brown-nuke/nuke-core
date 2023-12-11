import sqlite3
import time
from threading import Thread

import redis
from pymongo import MongoClient

users_to_nuke = "__users_to_nuke__"
r = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

user_prefix = "u:"
row_prefix = "r:"

sqlite_con = sqlite3.connect("correctness_test.db", check_same_thread=False)    # Problematic for application-agnosticism
sqlite_cur = sqlite_con.cursor()
mongo_db = MongoClient()["nukedit"]
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def nuke_ownership_update_add(user_id, database_id, table_id, row_id):
    merged_row_id = str(database_id) + ":" + str(table_id) + ":" + str(row_id)

    if user_id in r.smembers(users_to_nuke):
        return False

    r.sadd(user_prefix + str(user_id), merged_row_id)
    r.sadd(row_prefix + str(merged_row_id), user_id)

    return True


def nuke(user_id):
    r.sadd(users_to_nuke, user_id)


def do_nuke():
    for user_id in r.smembers(users_to_nuke):
        print("Nuking user: " + user_id)
        for merged_row_id in r.smembers(user_prefix + user_id):
            print(merged_row_id)
            r.srem(row_prefix + merged_row_id, user_id)
            r.srem(user_prefix + user_id, merged_row_id)
            if r.scard(row_prefix + merged_row_id) == 0:
                delete_row(merged_row_id)
        r.srem(users_to_nuke, user_id)


def delete_row(merged_row_id):
    database_id, table_id, row_id = merged_row_id.split(":")
    print(
        f"Deleting row: database_id={database_id}, table_id={table_id}, row_id={row_id}"
    )

    if database_id == "0":
        redis_client.delete(row_id)
    elif database_id == "1":
        mongo_db[table_id].delete_one({"_id": int(row_id)})
    elif database_id == "2":
        # Problematic for application-agnosticism!!
        sqlite_cur.execute(f"DELETE FROM correctness_test WHERE id = {int(row_id)};")
        sqlite_con.commit()


def background_nuke():
    while True:
        time.sleep(10)
        do_nuke()


nuking_thread = Thread(target=background_nuke, daemon=True)
nuking_thread.start()
