#!/usr/bin/env python
import os
import sys
import fcntl
import errno
import time
from sqlitedict import SqliteDict
import logging
import ctypes

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
        self.tags = SqliteDict(os.path.join(self.root, '.tags.sqlite'), autocommit=False)
        self.contents = SqliteDict(os.path.join(self.root, '.contents.sqlite'), autocommit=False)

    def __del__(self):
        self._flush_tags()

    def _flush_tags(self):
        self.tags.commit()
        self.contents.commit()
        logging.info("flushed tags and contents")

    def _consistent_file_path(self, path):
        name = file_name(path)
        tags = file_tags(path)
        if path[path.rindex('/')+1] == '.':
            return(name in self.tags.keys() and set(tags).issubset(self.tags[name]))
        return(name in self.tags.keys() and set(tags) == self.tags[name])

    def _store_path(self, tag_path):
        return os.path.join(self.store, tag_path.split('/')[-1].lstrip('.'))

    def getxattr(self, path, name, *args):
        logging.info("API: getxattr " + path + ", " + str(name) + ", " +str(args))
        if path == '/' or path.split('/')[-1].strip('.') in self.contents.keys():
            if set(dir_tags(path)).issubset(set(self.contents.keys())):
                return os.getxattr(self.store, name, *args)
            else:
                raise FuseOSError(errno.ENOENT)
        if not self._consistent_file_path(path):
            raise FuseOSError(errno.ENOENT)
        return os.getxattr(self._store_path(path), name, *args)

    def access(self, path, mode):
        # check if this is a directory
        logging.info("API: access " + path + " " + oct(mode))
        store_path = self._store_path(path)
        logging.debug("store path: " + store_path)
        if path[-1] == '/' or file_name(path) not in self.tags.keys():
            for tag in dir_tags(path):
                if tag not in self.contents.keys():
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
        if path[-1] == '/' or file_name(path) not in self.tags.keys(): # we (may) have a directory
            for tag in dir_tags(path):
                if tag not in self.contents.keys():
                    raise FuseOSError(errno.ENOENT)
            st = os.lstat(self.store)
            return {key: getattr(st, key) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}
            return 

        st = os.lstat(full_path)
        if not self._consistent_file_path(path):
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
        logging.debug("Path as tags: " + str(tags))
        for tag in tags:
            if tag not in self.contents.keys():
                raise FuseOSError(errno.ENOENT)
        tset = set(tags)
        dirents = ['.', '..']
        logging.debug("Contents: " + str(dict(self.contents)))
        logging.debug("Tags: " + str(dict(self.tags)))
        if len(tags) == 0:
            dirents.extend(self.contents.keys())
            dirents.extend([f if len(self.tags[f]) == 0 else '.' + f
                            for f in os.listdir(self.store)])
        else:
            matches = set.intersection(*[self.contents[tag] for tag in tags])
            dirents.extend(['.' + d if len(self.contents[d].intersection(matches)) == 0 else d
                            for d in self.contents.keys() if d not in tags])
            dirents.extend([f if tset == self.tags[f] else '.' + f
                            for f in matches])
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
            #pathname = os.path.join("../"*(read_dir.count('/')), self.store, path_from_store)
            pathname = os.path.join(os.path.relpath(self.store, read_dir),
                                    path_from_store)
            pathname = os.path.normpath(pathname)
        logging.debug("pathname: " + pathname)
        return pathname

    def mknod(self, path, mode, dev):
        logging.info("API: mknod")
        '''Generates a normal file (not folder-tag).
        Uses the path to set initial tags.'''
        # FIXME check to make sure this file doesn't exist.
        tags = [t.strip() for t in path.split('/')[0:-1]]
        name = path.split('/')[-1].strip()
        self.tags[name] = set(tags)
        for tag in tags:
            self.contents[tag].add(name)
        self._flush_tags()
        # return the link to the new store file
        return os.mknod(self._store_path(path), mode, dev)

    def mkdir(self, path, mode):
        logging.info("API: mkdir")
        '''Create a new tag.'''
        new_tag = path.split('/')[-1].strip("/.")
        self.contents[new_tag] = set()
        self._flush_tags()

    def rmdir(self, path):
        '''Deletes an empty tag.'''
        logging.info("API: rmdir " + path)
        # check we don't have a leaf
        # if os.path.isfile(_store_path(path)):
        #     raise OSError(20, "Not a tag")
        tag = path.split('/')[-1].strip() # note: it better be hidden or we'll throw 39
        # check we are trying to delete a real tag
        if tag not in self.contents.keys():
            raise FuseOSError(errno.ENOENT)
        # check that the tag has no members anywhere
        if self.contents[tag] != set():
            raise FuseOSError(errno.ENOTEMPTY)
        # remove it (it's not in any tags)
        del self.contents[tag]
        # return none
        self._flush_tags()

    def statfs(self, path):
        logging.info("API: statfs")
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
            tag = tags[-1]
            self.tags[name] = self.tags[name] - {tag}
            self.contents[tag] = self.contents[tag] - {name}
            self._flush_tags()
            return
        os.unlink(store_path)
        for tag in self.tags[name]:
            self.contents[tag] = self.contents[tag] - {name}
        del self.tags[name]
        self._flush_tags()

    def symlink(self, name, target):
        '''Creates a symlink.  You shouldn't need as many inside this filesystem.'''
        logging.info("API: symlink " + name + " to " + target)
        # make a stripped name symlink in the store
        # errors here if there's something wrong with making that symlink
        if target[0] != '/':
            target = os.path.join(os.path.relpath(self.mount, self.store),
                                  target)
        retval = os.symlink(target, self._store_path(name))
        logging.debug("continuing after attempting to symlink with result " + str(retval))
        # add the tags we need
        tags = file_tags(name)
        name = file_name(name)
        self.tags[name] = set(tags)
        for tag in tags:
            self.contents[tag] = self.contents[tag].union({name})
        self._flush_tags()
        return retval

    def rename(self, old, new):
        '''Changes the tag lists of a file, the name of a file, or the title of a tag.'''
        logging.info("API: rename " + old + " to " + new)
        old_name = file_name(old)
        new_name = file_name(new)
        # are we dealing with a file or a folder?
        #if old[-1] == '/' or not os.path.exists(self._store_path(old)):
        if old[-1] == '/' or file_name(old) not in self.tags.keys():
            # we are dealing with a (potentially bad) directory
            logging.debug("renaming as directory")
            old_tags = dir_tags(old)
            new_tags = dir_tags(new)
            if old_tags[-1] not in self.contents.keys():
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
            if new_tag in self.contents.keys():
                raise FuseOSError(errno.EEXIST)
            self.contents[new_tag] = self.contents.pop(old_tag)
            for f in self.contents[new_tag]:
                self.tags[f] = self.tags[f] - {old_tag}
                self.tags[f] = self.tags[f].union({new_tag})
            self._flush_tags()
        else:
            logging.debug("renaming as file")
            # handle taglist change
            if (from_tags := set(file_tags(old))) != (to_tags := set(file_tags(new))):
                old_tags = self.tags[old_name]
                if from_tags == set() or old.split('/')[-1][0] == ".":
                    new_tags = old_tags.union(to_tags)
                else:
                    new_tags = to_tags

                removed_tags = old_tags - new_tags
                added_tags = new_tags - old_tags
                logging.debug("changing tags: " + str(list(old_tags)) + " - " + str(list(removed_tags)) +
                                                    " + " + str(list(added_tags)) +
                                                    " = " + str(list(new_tags)))
                self.tags[old_name] = new_tags
                for tag in removed_tags:
                    self.contents[tag] = self.contents[tag] - {old_name}
                for tag in added_tags:
                    self.contents[tag] = self.contents[tag].union({old_name})
                logging.debug("contents: " + str(dict(self.contents)))
            # handle filename change
            if old_name != new_name:
                logging.debug("changing name")
                old_path = self._store_path(old)
                new_path = self._store_path(new)
                os.rename(old_path, new_path)
                self.tags[new_name] = self.tags.pop(old_name)
                for tag in file_tags(new):
                    self.contents[tag].remove(old_name)
                    self.contents[tag].add(new_name)
            self._flush_tags()

        

    def link(self, target, name):
        '''Hardlink: union tags'''
        logging.info("API: link")
        # refuse if the file name is different
        if _store_path(target) != _store_path(name):
            raise OSError(38, "hardlinking different names for an item not allowed")
        # make a set of old tags
        old_tags = set(target.split("/")[0:-2])
        # make a set of new tags
        new_tags = {x.strip('.') for x in name.split("/")}
        # union
        sum_tags = old_tags.union(new_tags)
        # set contents
        fname = file_name(name)
        self.tags[fname] |= new_tags
        # set tags
        for tag in sum_tags:
            self.contents[tag].add(name)
        self._flush_tags()

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
        self.tags[name]=set(tags)
        for tag in tags:
            self.contents[tag] = self.contents[tag].union({name})
        self._flush_tags()
        handle = os.open(store_path, os.O_WRONLY | os.O_CREAT, mode)
        logging.debug("Opened handle: " + str(handle))
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
