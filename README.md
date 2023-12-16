# Nukedit Client

This is a command-line social media platform.

### Installation:
```
python3 -m venv venv
source ./venv/bin/activate
pip install pyfiglet redis pymongo
```

#### Docker (for Redis):
```
curl https://get.docker.com | sh && sudo systemctl --now enable docker
sudo ./start_redis.sh
```

#### Mongodb
Please install and run mongodb following instructions here:
```
https://www.mongodb.com/docs/manual/administration/install-community/

sudo systemctl start mongod
```

#### Vim (for editing posts)
```
sudo apt-get install vim
```

### The following commands are available:

Explore a community.
```
cd <community_name>
```

List the posts.
```
ls
```

Make a post.
```
post
```

Read a post and its comments.
```
cat <post_id>
```

Comment
```
comment <cpost_id>
```
