#!/usr/bin/env python
import os
import sys
import fcntl
import errno
import time
import logging
import sqlite3


from fuse import FUSE, FuseOSError, Operations
import stat

from optparse import OptionParser

## constants and helpers
def dir_tags(path):
    if len(path) < 2:
        return []
    return [t.lstrip('.') for t in path.strip('/').split('/')]

def file_tags(path):
    path = path[:path.rindex('/')]
    if len(path) < 2:
        return []
    return [t.lstrip('.') for t in path.strip('/').split('/')]

def file_name(path):
    return path.split('/')[-1].strip('.')

class Tagfs(Operations):
    def __init__(self, root, mount, flat_delete):
        logging.info("init on "+ root)
        self.root = root
        self.mount = mount
        self.flat_delete = flat_delete
        self.store = os.path.join(self.root, 'store')
        # check to make sure we have a valid store structure
        if not os.path.exists(self.store):
            logging.info("Could not find actual store directory. Creating directory " + self.store)
            os.mkdir(self.store)
        self.con = sqlite3.connect(os.path.join(self.root, '.sqlite'))
        logging.debug("table_exists: " + str(self.con.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()))
        with self.con as c:
            c.execute("PRAGMA journal_mode = WAL")
            c.execute("""CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            UNIQUE (name)
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            UNIQUE (name)
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS file_tags (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            tag_id INTEGER,
            FOREIGN KEY (file_id) REFERENCES files (id),
            FOREIGN KEY (tag_id) REFERENCES tags (id)
            )""")
            c.execute("""CREATE VIEW IF NOT EXISTS taggings AS
            SELECT
                tags.name AS tag,
                files.name AS file
            FROM tags
            INNER JOIN file_tags ON tags.id = file_tags.tag_id
            INNER JOIN files ON files.id = file_tags.file_id""")
                
        logging.debug(self._tags())
        pass

    def _tags(self):
        return [x[0] for x in self.con.execute("SELECT name FROM tags").fetchall()]
    

    def _files(self):
        return [x[0] for x in self.con.execute("SELECT name FROM files").fetchall()]

    def _consistent_file_path(self, path):
        name = file_name(path)
        tags = file_tags(path)
        if self.con.execute("SELECT 1 FROM files WHERE name = ?", (name,)).fetchone() is None:
            return False
        true_tags = [x[0] for x in self.con.execute("SELECT tag FROM taggings WHERE file = ?", (name,)).fetchall()]
        logging.debug("true tags: " + str(true_tags))
        if path[path.rindex('/')+1] == '.':
            return set(true_tags) == set(tags)
        return set(tags).issubset(set(true_tags))

    def _store_path(self, tag_path):
        return os.path.join(self.store, tag_path.split('/')[-1].lstrip('.'))

    def getxattr(self, path, name, *args):
        logging.info("API: getxattr " + path + ", " + str(name) + ", " +str(args))
        if path == '/' or path.split('/')[-1].strip('.') in self._tags():
            if set(dir_tags(path)).issubset(set(self._tags())):
                return os.getxattr(self.store, name, *args)
            else:
                raise FuseOSError(errno.ENOENT)
        if not self._consistent_file_path(path):
            logging.debug(path + " deemed inconsistent")
            raise FuseOSError(errno.ENOENT)
        return os.getxattr(self._store_path(path), name, *args)

    def access(self, path, mode):
        # check if this is a directory
        logging.info("API: access " + path + " " + oct(mode))
        store_path = self._store_path(path)
        logging.debug("store path: " + store_path)
        if path[-1] == '/' or file_name(path) not in self._files():
            for tag in dir_tags(path):
                if tag not in self._tags():
                    raise FuseOSError(errno.ENOENT)
            if not os.access(self.store, mode):
                raise FuseOSError(errno.EACCES)
        else:
            if not os.access(self._store_path(path), mode):
                raise FuseOSError(errno.EACCES)
        logging.debug("Permission granted: " + path + " " + oct(mode))

    def chmod(self, path, mode):
        logging.info("API: chmod")
        return os.chmod(self._store_path(path), mode)

    def chown(self, path, uid, gid):
        logging.info("API: chown")
        return os.chown(self._store_path(path), uid, gid)

    def getattr(self, path, fh=None):
        logging.info("API: getattr " + path)
        perm = 0o777
        # we're going to lie about the number of hardlinks we have to path (st_nlinks). 
        # internally, we should be able to get away with it because deleting tags should never delete media.

        full_path = self._store_path(path)
        if path[-1] == '/' or file_name(path) not in self._files(): # we (may) have a directory
            for tag in dir_tags(path):
                if tag not in self._tags():
                    logging.debug(tag + " not in " + str(self._tags()))
                    raise FuseOSError(errno.ENOENT)
            st = os.lstat(self.store)
            return {key: getattr(st, key) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}
            return 

        st = os.lstat(full_path)
        if not self._consistent_file_path(path):
            logging.debug(path + " deemed inconsistent")
            raise FuseOSError(errno.ENOENT)
        return {key: getattr(st, key) for key in
                ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}

    def readdir(self, path, fh):
        '''Implements directory listing as a generator.
        The path is just split into tags.  No tag may be a filename.
        Items matching all tags are listed.  Those which are additionally
        members of other tags are hidden.  Existing tags will be shown if they
        contain some of those hidden members, otherwise hidden.'''
        logging.info("API: readdir " + path)
        tags = dir_tags(path)
        true_tags = self._tags()
        for tag in tags:
            if tag not in true_tags:
                raise FuseOSError(errno.ENOENT)
        tset = set(tags)
        dirents = ['.', '..']
        if len(tags) == 0:
            dirents.extend(self._tags())
            dirents.extend(self.con.execute("""SELECT CASE tag_id
            WHEN NULL THEN name
            ELSE '.' ||  name END
            FROM files LEFT JOIN file_tags ON files.id = file_tags.file_id
            """).fetchall())
        else:
            file_select = """SELECT path_tag_count.file AS file, other_tags.tag AS tag
                FROM ( SELECT file, COUNT(tag) AS count
                       FROM taggings WHERE tag IN ( """ + ', '.join(["?"]*len(tags)) + """ ) 
                     ) path_tag_count
                LEFT JOIN (SELECT tag, file FROM taggings
                           WHERE tag NOT IN ( """ + ', '.join(["?"]*len(tags)) + """ )) other_tags
                ON path_tag_count.file = other_tags.file
                WHERE path_tag_count.count = ?""" # , tags + tags + [len(tags)])

            with self.con as c:

                file_ents = c.execute("""SELECT CASE tag
                WHEN NULL THEN file
                ELSE  '.' || file END
                FROM (SELECT file, MIN(tag) FROM ( """+ file_select + """ )
                     ) AS unique_files""",
                                         tags + tags + [len(tags)]).fetchall()
                logging.info("file ents: " + str(file_ents))
                dirents.extend([x[0] for x in file_ents])
                
                tag_ents = c.execute("""SELECT CASE file
                WHEN NULL THEN '.' || tag
                ELSE tag END
                FROM (SELECT name AS tag FROM tags WHERE name NOT IN ( """+ ', '.join(["?"]*len(tags)) + """ ))
                LEFT JOIN (SELECT MIN(file), tag FROM """+ file_select +""")
                ON name=other_tags""", tags + tags + tags + [len(tags)]).fetchall()
                logging.info("tag ents: " + str(tag_ents))
                dirents.extend([x[0] for x in tag_ents])

            
        logging.debug('finished making dir listing')
        for r in dirents:
            logging.debug(str(r))
        for r in dirents:
            yield r

    def readlink(self, path):
        logging.info("API: readlink " + path)
        read_dir = os.path.join(self.mount, '/'.join(file_tags(path)))
        logging.debug("raw link: " + os.readlink(self._store_path(path)))
        logging.debug("read dir: " + read_dir)
        path_from_store = os.readlink(self._store_path(path))
        if path_from_store[0] == '/':
            pathname = path_from_store
        else:
            pathname = os.path.join(os.path.relpath(self.store, read_dir),
                                    path_from_store)
            pathname = os.path.normpath(pathname)
        logging.debug("pathname: " + pathname)
        return pathname

    def mknod(self, path, mode, dev):
        logging.info("API: mknod " + path)
        '''Generates a normal file (not folder-tag).
        Uses the path to set initial tags.'''

        name = file_name(path)
        if con.execute("SELECT 1 FROM files WHERE name = ?", (name,)).fetchone():
            # possibly odd behavior to not overwrite, might need to be changed
            raise FuseOSError(errno.EEXIST)
        for val in executemany("SELECT 1 FROM tags WHERE name = ?", tags):
            if val is None:
                raise FuseOSError(errno.ENOENT)
        tags = file_tags(path)
        with self.con as c:
            c.execute("INSERT INTO files (name) VALUES ('?')", (name,))
            file_id = c.lastrowid
            self.con.executemany("INSERT INTO file_tags (file_id, tag_id) SELECT '?',id FROM tags WHERE name = ?",
                                 ((file_id, tag) for tag in tags))
            retval = os.mknod(self._store_path(path), mode, dev)
        return retval

    def mkdir(self, path, mode):
        logging.info("API: mkdir " + path)
        '''Create a new tag.'''
        new_tag = dir_tags(path)[-1]
        if self.con.execute("SELECT 1 FROM tags WHERE name = ?", (new_tag,)).fetchone():
            raise FuseOSError(errno.EEXIST)
        with self.con as c:
            c.execute("INSERT INTO tags (name) VALUES (?)", (new_tag,))

    def rmdir(self, path):
        '''Deletes an empty tag.'''
        logging.info("API: rmdir " + path)
        tag = dir_tags(path)[-1]

        with self.con as c:
            if c.execute("SELECT 1 FROM tags WHERE name = ?", (tag,)).fetchone() is None:
                raise FuseOSError(errno.ENOENT)
            if c.execute("SELECT 1 FROM tags t INNER JOIN file_tags d ON t.id = d.tag_id").fetchone() is not None:
                raise FuseOSError(errno.ENOTEMPTY)
            c.execute("DELETE FROM tags WHERE name = ?", (tag,))

    def statfs(self, path):
        logging.info("API: statfs " + path)
        # does this break on directories?
        full_path = self._store_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        logging.info("API: unlink " + path)
        store_path = self._store_path(path)
        name = file_name(path)
        if len(tags := file_tags(path)) != 0 and self.flat_delete:
            self.con.execute("""DELETE FROM file_tags WHERE
            tag_id = (SELECT id FROM tags WHERE name = ?) AND
            file_id = (SELECT id FROM files WHERE name = ?)""",
                             (tags[-1], name))
            return
        with self.con as c:
            c.execute("""DELETE FROM file_tags WHERE
            file_id = (SELECT id FROM files WHERE name = ?)""", (name,))
            c.execute("DELETE FROM files WHERE name = ?", (name,))
            os.unlink(store_path)

    def symlink(self, name, target):
        '''Creates a symlink.  You shouldn't need as many inside this filesystem.'''
        logging.info("API: symlink " + name + " to " + target)
        # make a stripped name symlink in the store
        # errors here if there's something wrong with making that symlink
        if target[0] != '/':
            target = os.path.join(os.path.relpath(self.mount, self.store),
                                  target)
        # add the tags we need
        tags = file_tags(name)
        name = file_name(name)
        with self.con as c:
            c.execute("INSERT INTO files (name) VALUES (?)", name)
            file_id = c.lastrowid
            c.executemany("""INSERT INTO file_tags (file_id, tag_id)
            SELECT ?, id FROM tags WHERE name = ?""",
                          ((file_id, tag) for tag in tags))
            retval = os.symlink(target, self._store_path(name))
        return retval

    def rename(self, old, new):
        '''Changes the tag lists of a file, the name of a file, or the title of a tag.'''
        logging.info("API: rename " + old + " to " + new)
        old_name = file_name(old)
        new_name = file_name(new)
        # are we dealing with a file or a folder?
        with self.con as c:
            if old[-1] == '/' or c.execute("SELECT 1 FROM files WHERE name = ?", (old_name,)).fetchone() is None:
                # we are dealing with a (potentially bad) directory
                logging.debug("renaming as directory")
                old_tags = dir_tags(old)
                new_tags = dir_tags(new)
                old_tag_count = c.execute("SELECT count(name) FROM tags WHERE name IN (" +
                                          ", ".join(["?"]*len(old_tags)) + ")", old_tags).fetchall()
                logging.info("old_tag_count: " + str(old_tag_count) + ", given old tags: " + str(old_tags))
                if old_tag_count < len(old_tags):
                    raise FuseOSError(errno.ENOENT)
                # if someone adds extra dirs after the one they want to change, that's not covered
                for t_old, t_new in zip(old_tags[:-1], new_tags[:-1]):
                    if t_old != t_new:
                        raise FuseOSError(errno.ENOSYS)
                old_tag = old_tags[-1]
                new_tag = new_tags[-1]
                if len(old_tags) == 1 and new == "/..deleteme": # magic dir name to delete a tag from windows
                    self.rmdir(old)
                    return
                if c.execute("SELECT 1 FROM tags WHERE name = ?", (new_tag,)).fetchone() is not None:
                    raise FuseOSError(errno.EEXIST)
                c.execute("UPDATE tags SET name = ? WHERE name = ?", (new_tag, old_tag))
            else:
                logging.debug("renaming as file")
                # handle taglist change
                if set(from_tags := file_tags(old)) != set(to_tags := file_tags(new)):
                    if not self._consistent_file_path(old):
                        logging.debug(path + " deemed inconsistent")
                        raise FuseOSError(errno.ENOENT)
                    if len(from_tags) == 0 or old.split('/')[-1][0] == ".": # add only
                        c.executemany("""INSERT OR IGNORE INTO file_tags (file_id, tag_id)
                        SELECT f.id, t.id FROM files AS f CROSS JOIN tags AS t
                        WHERE f.name = ? AND t.name = ?""", ((old_name, tag) for tag in to_tags))
                    else:
                        c.execute("""DELETE FROM file_tags WHERE
                        file_id = (SELECT id FROM files WHERE name = ?)""", (old_name,))
                        c.executemany("""INSERT INTO file_tags (file_id, tag_id)
                        SELECT f.id, t.id FROM files AS f CROSS JOIN tags AS t
                        WHERE f.name = ? AND t.name = ?""", ((old_name, tag) for tag in to_tags))

                # handle filename change
                if old_name != new_name:
                    old_path = self._store_path(old)
                    new_path = self._store_path(new)
                    c.execute("UPDATE files SET name = ? WHERE name = ?", (new_name, old_name))
                    os.rename(old_path, new_path)

    def link(self, target, name):
        if not self._consistent_file_name(target):
            logging.debug(path + " deemed inconsistent")
            raise FuseOSError(errno.ENOENT)
        if file_name(target) != file_name(name):
            raise FuseOSError(errno.EPERM)
        with self.con as c:
            for v in c.executemany("SELECT 1 FROM tags WHERE name = ?", ((x,) for x in file_tags(name))).fetchall():
                if v is None:
                    raise FuseOSError(errno.ENOENT)
            c.executemany("""INSERT OR IGNORE INTO file_tags (file_id, tag_id)
            SELECT f.id, t.id FROM files AS f CROSS JOIN tags AS t
            WHERE f.name = ? AND t.name = ?""", ((file_name(name), tag) for tag in file_tags(name)))
            

    def utimens(self, path, times=None):
        logging.info("API: utimens " + path)
        return os.utime(self._store_path(path), times)

    def open(self, path, flags):
        logging.info("API: open " + path)
        store_path = self._store_path(path)
        handle = os.open(store_path, flags)
        logging.debug("Handle: " + str(handle))
        return handle

    def create(self, path, mode):
        logging.info("API: create " + path)
        store_path = self._store_path(path)
        tags = file_tags(path)
        name = file_name(path)

        with self.con as c:
            cur = c.cursor()
            cur.execute("INSERT INTO files (name) VALUES (?)", (name,))
            id = cur.lastrowid
            cur.executemany("INSERT INTO file_tags (file_id, tag_id) SELECT ?,id FROM tags WHERE name = ?",
                          ((id, tag) for tag in tags))
            handle = os.open(store_path, os.O_WRONLY | os.O_CREAT, mode)
        return handle

    def read(self, path, length, offset, fh):
        logging.info("API: read " + path)
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        logging.info("API: write to " + path)
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        logging.info("API: truncate " + path + ", len: " + str(length))
        full_path = self._store_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        logging.info("API: flush " + path)
        return os.fsync(fh)

    def release(self, path, fh):
        logging.info("API: release " + path)
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        logging.info("API: fsync " + path)
        return self.flush(path, fh)


def main(mountpoint, root, options, flat_delete):
    logging.info("Mountpoint: "+ str(mountpoint)+ ", root: "+ str(root))
    FUSE(Tagfs(root, mountpoint, flat_delete), mountpoint, nothreads=True, foreground=True, **options)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="count", dest="verbosity", default=0,
                      help="print information about interesting calls")
    parser.add_option("-s", "--show_fusepy_errors", action="store_false", dest="silent", default=True,
                      help="print normal fusepy errors without high verbosity")
    parser.add_option("-m", "--mountpoint", dest="mountpoint",
                      help="mountpoint of the tag filesystem")
    parser.add_option("-d", "--datastore", dest="datastore",
                      help="Data store directory for the tag filesystem")
    parser.add_option("-o", "--options", dest="fuse_options",
                      help="FUSE filesystem options")
    parser.add_option("-a", "--anywhere-delete", dest="flat_delete", action="store_false", default=True,
                      help="allow deletion anywhere, instead of just in the root of the fileystem")
    options, args = parser.parse_args()
    if options.verbosity > 0:
        logging.root.setLevel(logging.INFO)
        if options.verbosity > 1:
            logging.root.setLevel(logging.DEBUG)
        logging.info("Verbosity: "+ str(options.verbosity))
    if options.silent:
        class DevNull:
            def write(self, msg):
                pass
        sys.stderr = DevNull()
        sys.tracebacklimit = 0

    if options.fuse_options is not None:
        kwargs = {opt: True for opt in options.fuse_options.split(",")}
        logging.info("FS options: " + str(kwargs))
    else:
        kwargs = {}
    main(options.mountpoint, options.datastore, kwargs, options.flat_delete)
