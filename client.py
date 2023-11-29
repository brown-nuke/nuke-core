import cmd
import tempfile
import pyfiglet
import sqlite3
import nuke

from subprocess import call

# Database Map
# 0 -> Redis
# 1 -> 
# 2 -> 

redis_client = nuke.database_maps[0] 

class Query():
    def __init__(self, con, cursor):
        self.con = con
        self.cursor = cursor

    def query_posts(self, comm_name):
        select_query = "SELECT post_id, title FROM {};".format(comm_name)
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        rows = [r + (0,0) for r in rows]
        return rows[::-1]

class Community():
    def __init__(self, name):
        self.name = name

class NukedItClient(cmd.Cmd):
   intro = '\033[0m' + 'Welcome to the NukedIt shell. Type help or ? to list commands.\n'
   success = False
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
                print("\033[92m" + "Login Success.")
                success = True
            else:
                print("\033[91m" +  "Login Failed.")
        else:
            print("\033[91m" + "Login Failed.")
    else:
        print("\033[92m" + "Create Account.")
        username = input('\033[0m' + "[Username] : ")
        account_exists = redis_client.exists(username)
        if account_exists:
            print("\033[91m" + "Username already taken. Try again.")
        else:
            password = input('\033[0m' + "[Password] : ")
            redis_client.set(username, password)
    print()
   
   con = sqlite3.connect("nukedit.db")
   cur = con.cursor()

   server = Query(con, cur)

   DEFAULT_HOMEPAGE = "NukedIt Homepage"
   current_location = DEFAULT_HOMEPAGE

   # ----- basic commands -----
   def do_ls(self, arg):
       'Show posts in the sub: ls'
       self.get_list(*parse(arg))

   def do_cat(self, arg):
       'Move to a different <post_id>'
       self.get_post(*parse(arg))

   def do_mkpost(self, arg):
       'Create a post with VIM editor'
       self.create_post(*parse(arg))

   def do_cd(self, arg):
       'Move to a different <commmunity_id>'
       if len(arg) == 0:
           self.current_location = self.DEFAULT_HOMEPAGE
       else:
           self.set_community(*parse(arg))

   def do_mkcomm(self, arg):
       'Create a community <community_id>'
       self.create_community(*parse(arg))

   # ----- function  -----
   def get_list(self, *args):
       if str(self.current_location) == self.DEFAULT_HOMEPAGE:
            print(pyfiglet.figlet_format(str(self.current_location)))
       else: 
        try:
            list_of_posts = self.server.query_posts(self.current_location)
            print(pyfiglet.figlet_format(str(self.current_location)))
            for post, title, upvote, downvote in list_of_posts:
                print("<" + str(post)+ ">" + " ↑" + str(upvote) + " ↓" + str(downvote) + " " + str(title))
        except:
            print("This community cannot be found on NukedIt.")
        print()
        print()

   def set_community(self, *args):
        self.current_location = args[0]

   def create_community(self, *args):
        cm = Community(args[0])
        community_name = cm.name

        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        post_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        content TEXT
        )
        '''.format(community_name))

   def get_post(self, *args):
        select_query = "SELECT content FROM {} WHERE post_id = ?;".format(self.current_location)
        self.cur.execute(select_query, (args[0],))
        result = self.cur.fetchone()
        print(result[0])

   def create_post(self, *args):
        enter_text = b"Create post ..." # if you want to set up the file somehow
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

            self.cur.execute("INSERT INTO {} (title, content) VALUES (?, ?)".format(self.current_location), (title, post_text,))

            # Commit the transaction
            self.con.commit()


def parse(arg):
   return tuple(map(str, arg.split()))

if __name__ == '__main__':
   NukedItClient().cmdloop()
