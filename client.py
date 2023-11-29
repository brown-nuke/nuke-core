import cmd
import sqlite3
import tempfile
from subprocess import call

import pyfiglet
import redis
from pymongo import MongoClient

import nuke

# 0 -> Redis
# 1 -> MongoDB
# 2 -> SQLite


class NukeditClient(cmd.Cmd):
    intro = (
        "\033[0m" + "Welcome to the Nukedit shell. Type help or ? to list commands.\n"
    )
    success = False
    username = None

    sqlite_con = sqlite3.connect("nukedit.db")
    sqlite_cur = sqlite_con.cursor()

    mongo_client = MongoClient()
    mongo_db = mongo_client["nukedit"]

    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    while not success:
        print("\033[0m" + "====================================")
        print("\033[92m" + "Do you have an account?")
        answer = input("\033[96m" + "[Y/N] : ")
        print()
        if "y" in answer.strip().lower():
            print("\033[92m" + "Please login.")
            username = input("\033[0m" + "[Username] : ")
            password = input("\033[0m" + "[Password] : ")
            account_exists = redis_client.exists(username)
            if account_exists:
                get_password = redis_client.get(username)
                if get_password == password:
                    print("\033[92m" + "\nLogin Success.")
                    success = True
                else:
                    print("\033[91m" + "\nLogin Failed.")
            else:
                print("\033[91m" + "Login Failed.")
        else:
            print("\033[92m" + "Create Account.")
            username = input("\033[0m" + "[Username] : ")
            account_exists = redis_client.exists(username)
            if account_exists:
                print("\033[91m" + "Username already taken. Try again.")
            else:
                password = input("\033[0m" + "[Password] : ")
                if nuke.nuke_ownership_update_add(username, 0, 0, username):
                    redis_client.set(username, password)
                else:
                    print("\033[91m" + "Account is going to be nuked.")
        print()

    DEFAULT_HOMEPAGE = "Nukedit Homepage"
    current_location = DEFAULT_HOMEPAGE

    # ----- basic commands -----
    def do_ls(self, arg):
        "Show posts in the sub: ls"
        self.get_list(*parse(arg))

    def do_cat(self, arg):
        "Move to a different <post_id>"
        self.get_post(*parse(arg))

    def do_post(self, arg):
        "Create a post with VIM editor"
        self.create_post(*parse(arg))

    def do_cd(self, arg):
        "Move to a different <commmunity_id>"
        if len(arg) == 0:
            self.current_location = self.DEFAULT_HOMEPAGE
        else:
            self.set_community(*parse(arg))

    def do_comment(self, arg):
        "Comment on a post like: comment <post_id>"
        self.create_comment(*parse(arg))

    def do_reset(self, arg):
        "Reset all of the databases. Used for testing purposes."
        self.reset(self, *parse(arg))

    def do_nuke(self, arg):
        "Nuke user data"
        nuke.nuke(self.username)

        # ----- function  -----

    def get_list(self, *args):
        if str(self.current_location) == self.DEFAULT_HOMEPAGE:
            print(pyfiglet.figlet_format(str(self.current_location)))
        else:
            try:
                list_of_posts = self.mongo_db[self.current_location].find()
                print(pyfiglet.figlet_format(str(self.current_location)))
                for post in list_of_posts:
                    print(
                        "<"
                        + str(post["_id"])
                        + ">"
                        + " ↑"
                        + str(0)
                        + " ↓"
                        + str(0)
                        + " "
                        + str(post["title"])
                    )
            except:  # noqa: E722
                print("This community cannot be found on Nukedit.")
            print()
            print()

    def set_community(self, *args):
        self.current_location = args[0]

    def get_post(self, *args):
        post_collection = self.mongo_db[self.current_location]
        post = post_collection.find_one({"_id": int(args[0])})
        print(post["title"])
        print(post["content"])

        read_comment = "SELECT * FROM {}".format(self.current_location + str(args[0]))
        self.sqlite_cur.execute(read_comment)
        comments = self.sqlite_cur.fetchall()
        for c in comments:
            print(c[1] + ": " + c[2])

    def create_post(self, *args):
        enter_text = b"Create post ..."  # if you want to set up the file somehow
        with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
            tf.write(enter_text)
            tf.flush()
            call(["vim", "+set backupcopy=yes", tf.name])

            tf.seek(0)
            title = tf.readline()
            title = title.decode("utf-8").strip()
            edited_message = tf.read()
            post_text = edited_message.decode("utf-8")

            # for some reason it's not saving text.

            post_id = self.redis_client.incr("post_id")
            post_collection = self.mongo_db[self.current_location]

            if not nuke.nuke_ownership_update_add(
                self.username, 1, self.current_location, post_id
            ):
                print("Account is going to be nuked.")
                return

            post_collection.insert_one(
                {
                    "_id": post_id,
                    "title": title,
                    "content": post_text,
                }
            )

            comment_thread = str(self.current_location + str(post_id))
            create_comment_thread = f"""
                CREATE TABLE IF NOT EXISTS {comment_thread} (
                    comment_id INTEGER PRIMARY KEY,
                    user TEXT,
                    content TEXT
                )
            """

            self.sqlite_cur.execute(create_comment_thread)

    def create_comment(self, *args):
        enter_text = b"Comment ..."
        with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
            tf.write(enter_text)
            tf.flush()
            call(["vim", "+set backupcopy=yes", tf.name])

            tf.seek(0)
            edited_message = tf.read()
            comment_text = edited_message.decode("utf-8")

            comment_thread = self.current_location + str(args[0])
            comment_id = self.redis_client.incr(f"comment_id:post_id={str(args[0])}")

            if not nuke.nuke_ownership_update_add(
                self.username, 2, comment_thread, comment_id
            ):
                print("Account is going to be nuked.")
                return

            self.sqlite_cur.execute(
                f"INSERT INTO {comment_thread} (comment_id, user, content) VALUES (?, ?, ?)",
                (comment_id, self.username, comment_text),
            )
            self.sqlite_con.commit()

    def reset(self, *args):
        for table in self.sqlite_cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ):
            table_name = table[0]
            if table_name != "sqlite_sequence":
                self.sqlite_cur.execute(f"DROP TABLE {table[0]}")
        self.redis_client.flushall()
        self.mongo_client.drop_database("nukedit")

        exit()


def parse(arg):
    return tuple(map(str, arg.split()))


if __name__ == "__main__":
    NukeditClient().cmdloop()
