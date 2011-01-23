#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT, EACCES, EEXIST
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import re
import redis
from pprint import pprint, pformat

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


def layer(path, level):
  if level == 0 and path == "/":
    return "/"
  split = re.split(r"[.]", path)
  # /".here.we.are", 0 == ".here"
  #                  1 == "we.are"
  if path[1:2] == '.' and level == 0:
    return "/" + "." + split[1]
  elif path[1:2] == '.' and level == 1:
    return ".".join(split[2:])
  else:
    if level == 0 and len(split) > level:
      return split[level]
    elif len(split) > level:
      return ".".join(split[level:])
    else:
      return False

def path_key(path):
  """One layer in"""
  return layer(path, 0)

def path_field(path):
  """Two layers in"""
  return layer(path, 1)


class Redis(LoggingMixIn, Operations):
  """Redis-as-FS"""
  
  def __init__(self, host, port):
    self.redis = redis.Redis(host=host, port=port)
    self.files = {}
    self.data = defaultdict(str)
    self.fd = 0
    self.dirs = []
    now = time()
    self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
      st_mtime=now, st_atime=now, st_nlink=2)
    
  def chmod(self, path, mode):
    self.files[path]['st_mode'] &= 0770000
    self.files[path]['st_mode'] |= mode
    return 0

  def chown(self, path, uid, gid):
    self.files[path]['st_uid'] = uid
    self.files[path]['st_gid'] = gid
  
  def create(self, path, mode):
    key = path_key(path)
    field = path_field(path)
    if path in self.files:
      raise FuseOSError(EEXIST)
    if field:
      use_path = key + "." + field
      self.files[use_path] = self.mkfile(key, 'hash_field', field)
      self.add_new_file(use_path)
      # If this is the first field in a hash, make the hash object too:
      if key not in self.files:
        self.files[key] = self.mkfile(key, 'hash')
        self.add_new_file(key)
    else:
      self.files[path] = self.mkfile(key, 'string')
      self.add_new_file(path)
    self.fd += 1
    return self.fd
  
  def getattr(self, path, fh=None):
    if path not in self.files:
      raise FuseOSError(ENOENT)
    st = self.files[path]
    return st
  
  def getxattr(self, path, name, position=0):
    attrs = self.files[path].get('attrs', {})
    try:
      return attrs[name]
    except KeyError:
      return ''     # Should return ENOATTR
  
  def listxattr(self, path):
    attrs = self.files[path].get('attrs', {})
    return attrs.keys()
 
  # directories are keyspaces:
  # mount/usr/local/bin ==> usr:local:bin
  # find dirents by keys(usr:local:bin:*)
  # mount/usr/local/bin/bash ==> usr:local:bin:bash => content
  # mount/.git/objects ==> .git:objects

  # want: .git/objects/hashes/HASH ==> .git:objects:hashes:HASH => CONTENT
  # So, extract_dirs([.git, objects, hashes, HASH], [])
  # dirs[/.git] = [., .., objects[
  # dirs[/.git/objects] = [., .., hashes]
  # dirs[/.git/objects/hashes] = [., ..]
  def extract_dirs(self, key, unprocessed = [], path_so_far = ''):
    if len(unprocessed) < 2:
      return
    
    self.dirs[path_so_far + '/' + unprocessed[0]] = unprocessed[1]
    if not future_parts:
      return
    else:
      self.dirs["/".join(processed_parts + [future_parts[0]])]

  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
        st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
    self.files['/']['st_nlink'] += 1
  
  def open(self, path, flags):
    self.fd += 1
    return self.fd
 
  def read(self, path, size, offset, fh):
    key = path_key(path[1:])
    field = path_field(path)
    type = self.redis.type(key)
    return self.representation(key, field, type)

  def representation(self, key, field, type):
    value = ''
    if type == 'hash' or type == 'hash_field':
      if field:
        value = self.redis.hget(key, field)
      else:
        value = self.redis.hgetall(key)
    elif type == 'string':
      value = self.redis.get(key)
    elif type == 'list':
      value = self.redis.lrange(key, 0, -1)
    elif type == 'set':
      value = self.redis.smembers(key)
    elif type == 'zset':
      value = self.redis.zrange(key, 0, -1)

    if isinstance(value, str):
      return value
    else:
      # hgetall, sets, lists, and ranges aren't returnable unless formatted
      return pformat(value)

  
  def readdir(self, path, fh):
    if self.files["/"]["st_nlink"] == 2:
      self.populate_files()
    return self.dirs

  def populate_files(self):
    for key in self.redis.keys():
      if key == '':
        continue
#      if re.search(':', key):
#        self.populate_directories(key)
#      else:
      file_key = "/" + key
      self.files[file_key] = self.mkfile(key)
      if self.files[file_key]['r_type'] == 'hash':
        for field in self.redis.hkeys(key):
          self.files[file_key + '.' + field] = self.mkfile(key, 'hash_field', field)
    self.files["/"]["st_nlink"] = len(self.files) + 1
    self.dirs = ['.', '..'] + [x[1:] for x in self.files if x != '/']

  def readlink(self, path):
    print "READING LINK:", path
    return self.read(self, path)
  
  def removexattr(self, path, name):
    attrs = self.files[path].get('attrs', {})
    try:
      del attrs[name]
    except KeyError:
      pass    # Should return ENOATTR
  
  def rename(self, old, new):
    # Need to check for hash key stuff here
    # If no field, rename as-is.
    # If field, read from field, write to new field, delete old field
    #   Technically, that would allow cross-hash renames too
    #   Promote a hash field to a top level string?
    self.redis.rename(old[1:], new[1:])
    self.files[new] = self.files.pop(old)
    self.dirs.remove(old[1:])
    self.dirs.append(new[1:])
  
  def rmdir(self, path):
    self.files.pop(path)
    self.files['/']['st_nlink'] -= 1
  
  def setxattr(self, path, name, value, options, position=0):
    # Ignore options
    attrs = self.files[path].setdefault('attrs', {})
    attrs[name] = value
  
  def statfs(self, path):
    # Figure out if these need to be accurate or if these lies are okay
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
  def symlink(self, target, source):
    # incorporate with link functionality in er
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source))
    self.data[target] = source
  
  def truncate(self, path, length, fh=None):
    key = path_key(path[1:])
    field = path_field(path)
    if path in self.files:
      self.files[path]['st_size'] = length
    if field:
      # ugh.  read/set
      val = self.redis.hget(key, field)
      self.redis.hset(key, val[:length])
    else:
      # ugh.  read/set
      val = self.redis.get(key)
      self.redis.set(key, val[:length])
  
  def unlink(self, path):
    self.files.pop(path)
    self.dirs.remove(path[1:])
    key = path_key(path[1:])
    field = path_field(path)
    if field:
      self.redis.hdel(key, field)
      if not self.redis.hkeys(key):  # If this was the last field in the hash, delete the toplevel hash representation
        self.files.pop(path_key(path))
        self.dirs.remove(key)
    else:
      self.redis.delete(key)
  
  def utimens(self, path, times=None):
    now = time()
    atime, mtime = times if times else (now, now)
    self.files[path]['st_atime'] = atime
    self.files[path]['st_mtime'] = mtime
 
  def add_new_file(self, path):
    self.files["/"]["st_nlink"] += 1
    self.dirs.append(path[1:])
    
  def write(self, path, data, offset, fh):
    key = path_key(path[1:])
    field = path_field(path)
    type = ''
    use_path = path
    if path in self.files:
      print "FOUND PATH IN FILES", path
      type = self.files[path]['r_type']
    elif field:  # new hash field
      type = 'hash_field'
      use_path = path_key(path) + '.' + field
      self.files[use_path] = self.mkfile(key, 'hash_field', field)
      self.add_new_file(use_path)
    else:  # new string
      type = 'string'
      use_path = path_key(path)
      self.files[use_path] = self.mkfile(key, 'string')
      self.add_new_file(use_path)

    if type == 'hash':  # writing existing python representation of a redis hash
      raise FuseOSError(EACCES)
#      hash_eval = eval(data)
#      for field in hash_eval:
#        self.redis.hset(key, field, hash_eval[field])
#        use_path = path_key(path) + '.' + field
#        self.files[use_path] = self.mkfile(key, 'hash_field', field)
#        self.add_new_file(use_path)
#      self.files[file_key] = self.mkfile(key)
#      self.add_new_file(file_key)
        
    if field and type == 'hash_field':
      existing_data = self.redis.hget(key, field)
      use_data = existing_data[:offset] + data if existing_data else data
      self.redis.hset(key, field, use_data)
      if use_data:
        self.files[use_path]['st_size'] = len(use_data)
        self.files[path_key(path)] = self.mkfile(key, 'hash')
    elif type == 'string':
      print "SET STRING", key, offset
      self.redis.setrange(key, offset, data)
      self.files[use_path]['st_size'] = self.redis.strlen(key)
    else:
      # no writing to hashes directly (and sets, zsets, or lists)
      raise FuseOSError(EACCES)

    return len(data)

  def mkfile(self, key, r_type=False, field=False):
    type = r_type or self.redis.type(key)
    size = 0
    if type == 'string':
      size = self.redis.strlen(key)
    elif type == 'hash':
      val = self.representation(key, field, type)
      if val:
        size = len(val)
    elif field and type == 'hash_field':
      # redis can't do strlen on hash keys.  sad.
      # read the entire value just to get the size
      val = self.representation(key, field, type)
      if val:
        size = len(val)
    return dict(st_mode=(S_IFREG | 0755), st_nlink=1,
             r_type = type,
             st_size=size, st_ctime=time(), st_mtime=time(), st_atime=time())

  
def mkdir():
  return dict(st_mode=(S_IFDIR | 0755), st_nlink=2,
           st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())

def mklink():
  return dict(st_mode=(S_IFLNK | 0755), st_nlink=2,
           st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())


if __name__ == "__main__":
  if len(argv) != 4:
    print 'usage: %s <server> <port> <mountpoint>' % argv[0]
    exit(1)
  fuse = FUSE(Redis(argv[1], int(argv[2])), argv[3], foreground=True)
