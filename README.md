# NukedIt Client

This is a command-line social media platform.

### Installation:
```
python3 -m venv venv
source ./venv/bin/activate
pip install pyfiglet
```

#### Docker (for Redis):
```
curl https://get.docker.com | sh && sudo systemctl --now enable docker
sudo ./start_redis.sh
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
mkpost
```


Read a post.
```
cat <post_id>
```


Make a community
```
mkcomm <community_name>
```
