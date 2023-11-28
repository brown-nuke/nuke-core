import redis

users_to_nuke = "__users_to_nuke__"
database_maps = {
    0: redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
}
connected_dbs = "__connected_dbs__"
database_maps[0].sadd(connected_dbs, 0)

user_prefix = "u:"
row_prefix = "r:"


# here we connect to different redis db for each database
def connect_to_redis_db(database_id):
    if database_id not in database_maps:
        database_maps[0].sadd(connected_dbs, database_id)
        database_maps[database_id] = redis.Redis(
            host="localhost", port=6379, db=database_id, decode_responses=True
        )

    return database_maps[database_id]


def nuke_ownership_update_add(user_id, row_id, database_id):
    r = connect_to_redis_db(database_id)

    if user_id in r.smembers(users_to_nuke):
        return False

    r.sadd(user_prefix + user_id, row_id)
    r.sadd(row_prefix + row_id, user_id)

    return True


def nuke_ownership_update_remove(user_id, row_id, database_id):
    r = connect_to_redis_db(database_id)
    r.srem(user_prefix + user_id, row_id)
    r.srem(row_prefix + row_id, user_id)

    return True


def nuke(user_id):
    database_maps[0].sadd(users_to_nuke, user_id)


def do_nuke():
    for database_id in database_maps[0].smembers(connected_dbs):
        r = connect_to_redis_db(database_id)
        for user_id in r.smembers(users_to_nuke):
            for row_id in r.smembers(user_prefix + user_id):
                r.srem(row_prefix + row_id, user_id)
                r.srem(user_prefix + user_id, row_id)
                if r.scard(row_prefix + row_id) == 0:
                    print("deleting row: " + row_id)
                    # delete row from db
