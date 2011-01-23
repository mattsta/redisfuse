redisfuse: Redis-as-FS
======================

Usage
-----
Warning/Disclaimer: This is awesome, but very young, software.  It could destroy all your redis data.  It can pollute your redis data set with .DS_Store and other OS-specific keys (vim .swp files, .lock files, .tmp files, etc).  On startup, it will read a large part of your redis data set to determine sizes of non-string keys.  It's recommended you set up an isolated redis instance for redisfuse testing or point it to a read-only instance of production data.

###  Install FUSE:
* OS X: http://code.google.com/p/macfuse/
* Linux: use your favorite package manager

### Mount Redis-as-FS
        mkvirtualenv redisfusepython
        source redisfusepython/bin/activate
        pip install redis
        ./redisfuse.py <redis-server> <redis-port> <mountpoint>

  (fuse.py is included directly because pip doesn't install the latest version)

### Optionally, mount a remote redis locally using SSH:
  Basically, 
        ssh -L [remote-redis-port]:127.0.0.1:[forwarded-redis-port] you@remote-server

  More specifically,
        ssh -L 3679:127.0.0.1:33679 preproduction.server
        ./redisfuse.py 127.0.0.1 33679 localredis


### Rules for writing and creating new files:
#### redis namespaces
Colons in a key are directory delimiters.  File .git/info/exclude has redis key .git:info:exclude

Hash keys show up as file extensions.  File .git/hooks/post-update.sample is redis key .git:hooks:post-update with data stored in hash key 'sample'.

#### redis string examples
`bob.lock`: always a string.  Why?  git makes a HEAD.lock before creating
HEAD.  If HEAD.lock is a hash, HEAD can't be a string.  *.lock files are
hard coded to always be strings.

`bob`: always a string.  Any newly created filename with no extension is
a string.

`bob.hello`: always a string if the filename with no extension already exists
as a string.

#### redis hashes
`hello.howdy`: field howdy on hash hello.  If a new file has an extension,
redisfuse writes a hash field.

#### redis other
You can only write to strings and hash fields.  You can't write to top-level
hashes, or sets, zsets, or lists.  


TODO
----
* Test suite, dammit.
* More client side caching/validation
* Fix redis-py to not uselessly SELECT a redis DB on every command
  * Make a python redis driver using hiredis directly?
* Allow directory list to be updated from redis after mounting (currently everything is pulled from redis on your first ls then never updated again)
  * Allow directory list to be selectively populated so we don't keys(*) and pull down the entire dataset
* Move configuration options to an external file (/etc/redisfuse.conf)?
* Add a read-only configuration option

What would make things easier?
------------------------------
* new redis features
  * setrange, getrange, strlen on hash fields
    * for hashes, we have to read the data, edit it on the client, then push it back.  For string we can just set the new data directly at a offset.
  * a truncate (or setlength) operation on strings and hash fields
    * for both strings and hashes we have to read the data, truncate on the client, then push it back.
  * a quick hash function on strings and hash fields
    * hmd5 hash-key field-key
    * md5 string-key

So you made a change and you don't know if things broke?
--------------------------------------------------------
Run the commands below against your mount.  If you hit errors or unexpected
faults, look for Traceback from the redisfuse log output.

### Test basic structural integrity
        cd mount
        echo hello > hello; echo hi > hi; echo bob > bob
        git init; git add *; git commit -m first commit
        git fsck --full --strict
        cd ..
        git clone mount mount-cloned
        umount mount
        <remount>
        cd mount
        git fsck --full --strict

### Test hashes
        cd mount
        echo hello > hello.hello
        cat hello.hello

### Test directory keys
        cd mount
        mkdir -p key1/key2/key3/key4
        cd key1/key2/key3/key4
        echo dummy key data > dummy.data
        cat dummy.data

History
-------
Why does this exist?  I wanted to make a site where all templates and data gets stored in redis.  This presents a problem of, if everything is stored in redis, how do you make the webpage enabling you to upload stuff and store it in redis?

You could create a file and cat it into a key using redis-cli, but that's not
elegant.  Why not edit files in redis directly using your favorite text editor?
So, that's what redisfuse does.  We can edit strings and hashes directly in redis using your favorite text editor.

redisfuse started out as a copy of http://code.google.com/p/fusepy/source/browse/trunk/memory.py and grew as features were needed.

Notable things absent from redisfuse: any sense of file locking, consistency, or knowledge of keyspace changes once the FS is mounted.  If you have a redis key open in your editor and someone else writes a different version, you have no way of knowing you are going to obliterate their changes when you save your file.  If someone creates a key after your FS is mounted, you can't see the key until you umount and remount redisfuse.

Other than that, it should work fine for most simple bootstrap purposes.  I can use `dd` on it, I can `git init`, and I can clone git repositories stored entirely in redis.  Nifty.


Blame
------
Created by Matt. https://github.com/mattsta/
