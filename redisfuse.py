#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT, EACCES, EEXIST
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from pprint import pprint, pformat
import re
import redis

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


def layer(path, level):
  if level == 0 and path == "/":
    return "/"
  split = path.split(".")
  # /".here.we.are", 0 == ".here"
  #                  1 == "we.are"
  matched_slash_dot = re.match(r'.*/\..*', path)
  if matched_slash_dot and level == 0:
    return split[0] + "." + split[1]
  elif matched_slash_dot and level == 1:
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

def blank_files_and_dirs():
  files = {}
  dirs = defaultdict(list)
  now = time()
  dirs['/'].extend([".", ".."])
  files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
      st_mtime=now, st_atime=now, st_nlink=2)
  return (files, dirs)

class Redis(LoggingMixIn, Operations):
  """Redis-as-FS"""

  def __init__(self, host, port):
    self.redis = redis.Redis(host=host, port=port)
    (self.files, self.dirs) = blank_files_and_dirs();
    self.fd = 0
    self.repr = False
    self.disallow_unlink_representations = True
    self.disallow_rename_representations = True
    
  def hashkey(self, filename, field, dir):
    if not field:
      return False
    if dir == '/':
      dir = ''
    dirkey = dir + '/' + filename.replace('.' + field, '') # + "_representation"
    print "hashkey:", dirkey
    return dirkey

  def splitpath(self, path):
    """ Given any path, return the parts we need to manipulate it in redis.

        Returns (rediskey, hash-field-name, parent-directory, filename)
    """
    splits = filter(None, path.split("/"))
    dirent = splits[-1]
    key = False
    field = False
    if path in self.files and 'r_type' in self.files[path] \
        and self.files[path]["r_type"] == 'string':
      key = ":".join(filter(None, path.split("/")))
      field = False
    else:
      key = ":".join(filter(None, path_key(path).split("/")))
      field = path_field(path)
    dir = "/" + "/".join(splits[:-1])
    solution = (key, field, dir, dirent)
    print solution
    return solution

  def chmod(self, path, mode):
    self.files[path]['st_mode'] &= 0770000
    self.files[path]['st_mode'] |= mode
    return 0

  def chown(self, path, uid, gid):
    self.files[path]['st_uid'] = uid
    self.files[path]['st_gid'] = gid
  
  def create(self, path, mode):
    (key, field, dir, filename) = self.splitpath(path)

    dirkey = self.hashkey(filename, field, dir)

    if path in self.files:
      raise FuseOSError(EEXIST)

    # don't turn lock files into hashes
    if field == 'lock':
      print "LOCK", filename
      self.files[path] = self.mkfile(filename, 'string')
      self.add_new_file(path)
    # If the parent key is a string, we can't make this a hash.  re-string.
    elif field and dirkey in self.files \
        and self.files[dirkey]["r_type"] == 'string':
      print "STRING HASH", filename
      self.files[path] = self.mkfile(filename, 'string')
      self.add_new_file(path)
    # else, we have hash
    elif field:
      print "FIELD", key, field
      self.files[path] = self.mkfile(key, 'hash_field', field)
      self.add_new_file(path)
      # If this is the first field in a hash, make the hash object too:
      hk = self.hashkey(filename, field, dir)
      if self.repr and hk not in self.files:
        self.files[hk] = self.mkfile(key, 'hash')
        self.add_new_file(hk)
    # else, else, we have string again.  :(
    else:
      print "OTHER", key
      self.files[path] = self.mkfile(key, 'string')
      self.add_new_file(path)
    self.fd += 1
    return self.fd
  
  def getattr(self, path, fh=None):
    if path == "/.updater":
      print "Updating Listings..."
      self.populate_files()

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
  # dirs[/.git] = [., .., objects]
  # dirs[/.git/objects] = [., .., hashes]
  # dirs[/.git/objects/hashes] = [., ..]
  def extract_dirs(self, unprocessed = [], path_so_far = ''):
    if len(unprocessed) < 2:
      return path_so_far or '/'

    path = path_so_far + '/' + unprocessed[0]

    if not path in self.dirs:
      self.mkdir(path, 0755)

    return self.extract_dirs(unprocessed[1:], path)

  def mkdir(self, path, mode):
    (key, field, parent_dir, filename) = self.splitpath(path)
    if path in self.files:
      raise FuseOSError(EEXIST)

    self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
        st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
    self.dirs[path].extend([".", ".."])

    # make parent dir too
    if not parent_dir in self.files:
      self.files[parent_dir] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
          st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
    else:
      self.files[parent_dir]['st_nlink'] += 1

    if not self.dirs[parent_dir]:
      self.dirs[parent_dir].extend([".", ".."])

    self.dirs[parent_dir].append(filename)

  def open(self, path, flags):
    self.fd += 1
    return self.fd
 
  def read(self, path, size, offset, fh):
    (key, field, dir, filename) = self.splitpath(path)
    type = self.redis.type(key)
    solution = self.representation(key, field, type)
    self.files[path]["st_size"] = len(solution)
    return solution

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
      # hgetall, sets, lists, and zsets aren't returnable unless formatted
      # also, let's be nice and throw in a newline so we can `cat` nicely
      return pformat(value) + "\n"

  
  def readdir(self, path, fh):
    if self.files["/"]["st_nlink"] == 2:
      self.populate_files()
    dir = self.dirs[path]
    if not dir:
      raise FuseOSError(ENOENT)
    else:
      return dir

  def populate_files(self):
    (self.files, self.dirs) = blank_files_and_dirs();
    for key in self.redis.keys():
      if not key:
        continue
      dir_for_key = '/'
      if re.search(':', key):
        dir_for_key = self.extract_dirs(key.split(":"))

      path = "/" + "/".join(key.split(":"))

      made_file = self.mkfile(key)
      update_paths = []

      # if we are a hash, make entries for each hash key but not the hash itself
      if made_file['r_type'] == 'hash':
        base_path = path
        for field in self.redis.hkeys(key):
          path = base_path + '.' + field
          self.files[path] = self.mkfile(key, 'hash_field', field)
          update_paths.append(path)
      # else, we are a non-hash, so just make the file the key name
      else:
        self.files[path] = made_file
        update_paths.append(path)

      for update in update_paths:
        (key, field, dir, filename) = self.splitpath(update)
        self.dirs[dir_for_key].append(filename)
        self.files[dir_for_key]["st_nlink"] = len(self.dirs[dir_for_key])


  def readlink(self, path):
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
    (okey, ofield, odir, ofilename) = self.splitpath(old)
    (nkey, nfield, ndir, nfilename) = self.splitpath(new)

    # If vim is trying to rename the file to re-write it, disallow.
    # It breaks any non-string or non-hash field because it renames the existing
    # set, zset, list, or hash, writes a new STRING to the old name, then delets
    # the renamed set, zset, list, or hash.
    disallow_types = ('hash', 'set', 'zset', 'list')
    if self.disallow_rename_representations and \
       self.files[old]["r_type"] in disallow_types or (new in self.files and \
       self.files[new]["r_type"] in disallow_types):
      raise FuseOSError(EACCES)

    # if we are trying to rename a hash field, don't allow it.
    # we need another half dozen checks to make hash renaming work
    if ofield or nfield:
      raise FuseOSError(EACCES)

    self.redis.rename(okey, nkey)
    self.files[new] = self.files.pop(old)
    self.dirs[odir].remove(ofilename)
    self.files[odir]["st_nlink"] -= 1
    self.files[ndir]["st_nlink"] += 1
    # make sure new_dir exist before doing this or else we won't have . and ..
    if not ndir in self.dirs:
      self.dirs[ndir].extend([".", ".."])
    self.dirs[ndir].append(nfilename)
  
  def rmdir(self, path):
    (key, field, dir, filename) = self.splitpath(path)
    self.files.pop(path)
    self.dirs[dir].remove(filename)
    # delete from parent directory here too
    self.files[dir]['st_nlink'] -= 1
  
  def setxattr(self, path, name, value, options, position=0):
    # Ignore options
    attrs = self.files[path].setdefault('attrs', {})
    attrs[name] = value
  
  def statfs(self, path):
    # Figure out if these need to be accurate or if lies are okay
    # look ma, we have 128 petabytes free!
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2**48)
  
  def symlink(self, target, source):
    # incorporate with link functionality in er
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source))
  
  def truncate(self, path, length, fh=None):
    (key, field, dir, filename) = self.splitpath(path)

    if self.disallow_unlink_representations and \
       self.files[path]["r_type"] in ('hash', 'set', 'zset', 'list'):
      raise FuseOSError(EACCES)

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
    (key, field, dir, filename) = self.splitpath(path)
    if self.repr and re.match(r".*_representation$", path):
      return

    #  Allowing removal of non-strings is tricky.  If you edit a representation
    # file in a text editor, to save the file, the editor does:
    # make temp file; rename original file to old file; rename temp to existing 
    # file; delete renamed original file.
    #  Doing this turns representations into STRINGS which is horrible.
    #  So, to get around this, we are disallowing renames and deletions of
    # non-strings and non-hash-fields.  There should be a config option
    # for "allow renames" and "allow delete" for all types.
    if self.disallow_unlink_representations and \
       self.files[path]["r_type"] in ('hash', 'set', 'zset', 'list'):
      raise FuseOSError(EACCES)

    self.files.pop(path)
    self.dirs[dir].remove(filename)
    self.files[dir]['st_nlink'] -= 1
    if field:
      self.redis.hdel(key, field)
      # If last field in the hash, delete the toplevel hash representation
      if self.repr and not self.redis.hkeys(key):
        hk = self.hashkey(filename, field, dir)
        self.files.pop(hk)
        (hkey, hfield, hdir, hfilename) = self.splitpath(hk)
        self.dirs[dir].remove(hfilename)
        self.files[dir]['st_nlink'] -= 1
    else:
      self.redis.delete(key)

  def utimens(self, path, times=None):
    now = time()
    atime, mtime = times if times else (now, now)
    self.files[path]['st_atime'] = atime
    self.files[path]['st_mtime'] = mtime
 
  def add_new_file(self, path):
    (key, field, dir, filename) = self.splitpath(path)
    self.files[dir]["st_nlink"] += 1
    self.dirs[dir].append(filename)
    
  def write(self, path, data, offset, fh):
    (key, field, dir, filename) = self.splitpath(path)
    type = ''
    if path in self.files:
      type = self.files[path]['r_type']
    elif field and self.redis.type(key) == 'hash':  # new hash field
      type = 'hash_field'
      self.files[path] = self.mkfile(key, 'hash_field', field)
    else:  # new string
      type = 'string'
      self.files[path] = self.mkfile(key, 'string')

    if type == 'hash':  # writing existing python representation of a redis hash
      raise FuseOSError(EACCES)

    if field and type == 'hash_field':
      existing_data = self.redis.hget(key, field)
      use_data = existing_data[:offset] + data if existing_data else data
      self.redis.hset(key, field, use_data)
      if self.repr and use_data:
        hk = self.hashkey(filename, field, dir)
        self.files[path]['st_size'] = len(use_data)
        self.files[hk] = self.mkfile(key, 'hash')
    elif type == 'string':
      print "WRITING TO", key, path, data
      self.redis.setrange(key, offset, data)
      self.files[path]['st_size'] = self.redis.strlen(key)
    else:
      # no writing to hashes directly (and sets, zsets, or lists)
      raise FuseOSError(EACCES)

    return len(data)

  def mkfile(self, key, r_type=False, field=False):
    type = r_type or self.redis.type(key)
    size = 0
    if type == 'string':
      size = self.redis.strlen(key)
    elif type in ('hash', 'zset', 'set', 'list'):
      # read the entire data for each key, format it, get the size of the
      # formatted version, then throw the representation away.
      # maybe we should cache it?  maybe not?
      val = self.representation(key, field, type)
      if val:
        size = len(val)
    elif field and type == 'hash_field':
      # redis can't do strlen on hash fields.  sad.
      # read the entire value just to get the size
      val = self.representation(key, field, type)
      if val:
        size = len(val)
    return dict(st_mode=(S_IFREG | 0755), st_nlink=1,
             r_type = type,
             st_size=size, st_ctime=time(), st_mtime=time(), st_atime=time())


if __name__ == "__main__":
  if len(argv) != 4:
    print 'usage: %s <server> <port> <mountpoint>' % argv[0]
    exit(1)
  fuse = FUSE(Redis(argv[1], int(argv[2])), argv[3], foreground=True)
