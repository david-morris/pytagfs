#!/usr/bin/env python
import os
import sys
import errno
import time
from sqlitedict import SqliteDict
import logging

from fuse import FUSE, FuseOSError, Operations
import stat

from optparse import OptionParser

# TODO handle deletions from file store
# TODO disallow or strip . at begin or end of file/folder name.
# TODO straighten out _store_path and _file_name

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

def guess_tags(path):
    if path[-1] != '/':
        path = path[:path.rindex('/')]
    if len(path) < 2: # check for root/file in root.
        return []
    path = path.strip('/')
    return [t.lstrip('.') for t in path.split("/")]

def _file_name(path):
    return path.split('/')[-1].strip('.')

class Tagfs(Operations):
    def __init__(self, root, flat_delete):
        logging.info("init on "+ root)
        self.root = root
        self.flat_delete = flat_delete
        self.store = os.path.join(self.root, 'store')
        # check to make sure we have a valid store structure
        if not os.path.exists(self.store):
            logging.info("Could not find actual store directory. Creating directory " + self.store)
            os.mkdir(self.store)
        self.tags = SqliteDict(os.path.join(self.root, '.tags.sqlite'), autocommit=False)
        self.contents = SqliteDict(os.path.join(self.root, '.contents.sqlite'), autocommit=False)

    def _flush_tags(self):
        logging.info("flushing tags and contents")
        self.tags.commit()
        self.contents.commit()


    def _store_path(self, tag_path):
        return os.path.join(self.store, tag_path.split('/')[-1].lstrip('.'))

    def lock(self, path, cmd, length, fh):
        logging.info("API: Locking " + path + ", cmd: " + str(cmd) + ", lock: " + str(lock) + ", fh: " + str(fh))
        store_path = self._store_path(path)
        if path[-1] == '/' or (not os.path.exists(store_path)):
            raise FuseOSError(errno.ENOSYS) # let's see if we can get away with that.
        else:
            return os.lockf(fh, cmd, length)


    def access(self, path, mode):
        # check if this is a directory
        logging.info("API: access " + path + " " + oct(mode))
        store_path = self._store_path(path)
        logging.debug("store path: " + store_path)
        if path[-1] == '/' or (not os.path.exists(store_path)):
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
        if path[-1] == '/' or (not os.path.exists(full_path)): # we (may) have a directory
            for tag in dir_tags(path):
                if tag not in self.contents.keys():
                    raise FuseOSError(errno.ENOENT)
            st = os.lstat(self.store)
            return {key: getattr(st, key) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}
            return 
        st = os.lstat(full_path)
        for tag in file_tags(path):
            if tag not in self.contents.keys():
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
        logging.info("API: readlink")
        pathname = os.readlink(self._store_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
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
        name = _file_name(path)
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


        # # check if the file exists
        # if not os.path.isfile(self._store_path(path)):
        #     raise FuseOSError(errno.ENOENT)
        # path_parts = [x.strip() for x in path.split('/')]
        # # check if we have a tagless item
        # if len(path_parts) == 1:
        #     return os.unlink(self._store_path(path))
        # # check if we have the right tags
        # for tag in path_parts[0:-1]:
        #     if tag not in self.tags[name]:
        #         raise FuseOSError(errno.ENOENT)
        # # remove the last tag
        # tags[path_parts[-1]].remove(path_parts[-2])
        # contents[path_parts[-2]].remove(path_parts[-1])

    '''

    def unlink (self, path): 
        name = path.split('/')[-1].strip()
        # check if the file exists
        if not os.path.isfile(self._store_path(path)):
            raise OSError(2, "No such file")
        # clean it away
        for tag in tags[name]:
            contents[tag].remove(name)
        del tags[name]
        self._flush_tags()
        return os.unlink(self._store_path(path))
    '''

    def symlink(self, name, target):
        '''You shouldn't be symlinking inside of this filesystem.'''
        logging.info("API: symlink")
        # TODO add a check to see if we are trying to link inside this FS
        # refuse to accept relative paths
        if target[0] != '/':
            raise OSError(38, "Symlinks must be absolute")
        # refuse to make a symlink to 2 points inside the tagfs
        if target[0:size(self.root)] == self.root:
            raise OSError(38, "No symlinking inside the tagfs")
        # make a stripped name symlink in the store
        retval = os.symlink(target, self._store_path(name)) # errors here if there's something wrong with making that symlink
        # add the tags we need
        tags = [t.strip() for t in name.split('/')[0:-1]]
        name = self._store_path(name)
        self.tags[name] = set(tags)
        for tag in tags:
            self.contents[tag].add(name)
        self._flush_tags()
        return retval

    def rename(self, old, new):
        '''Changes the tag lists of a file, the name of a file, or the title of a tag.'''
        logging.info("API: rename " + old + " to " + new)
        old_name = _file_name(old)
        new_name = _file_name(new)
        # are we dealing with a file or a folder?
        if old[-1] == '/' or not os.path.exists(self._store_path(old)):
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
            if new_tag in self.contents.keys():
                raise FuseOSError(errno.EEXIST)
            self.contents[new_tag] = self.contents.pop(old_tag)
            for f in self.contents[new_tag]:
                self.tags[f].remove(old_tag)
                self.tags[f].add(new_tag)
            self._flush_tags()
        else:
            logging.debug("renaming as file")
            # handle taglist change
            if (old_tags := set(file_tags(old))) != (new_tags := set(file_tags(new))):
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
        fname = _file_name(name)
        self.tags[fname] |= new_tags
        # set tags
        for tag in sum_tags:
            self.contents[tag].add(name)
        self._flush_tags()

    def utimens(self, path, times=None):
        logging.info("API: utimens")
        return os.utime(self._store_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        logging.info("API: open " + path)
        store_path = self._store_path(path)
        return os.open(store_path, flags)

    def create(self, path, mode):
        logging.info("API: create " + path)
        store_path = self._store_path(path)
        tags = file_tags(path)
        name = _file_name(path)
        self.tags[name]=set(tags)
        for tag in tags:
            self.contents[tag] = self.contents[tag].union({name})
        self._flush_tags()
        return os.open(store_path, os.O_WRONLY | os.O_CREAT, mode)

    # def create(self, path, mode, fi=None):
    #     full_path = self._full_path(path)
    #     # TODO handle tags!
    #     return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        logging.info("API: read " + path)
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        logging.info("API: write")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        logging.info("API: truncate")
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
        logging.info("API: fsync")
        return self.flush(path, fh)


def main(mountpoint, root, options, flat_delete):
    logging.info("Mountpoint: "+ str(mountpoint)+ ", root: "+ str(root))
    FUSE(Tagfs(root, flat_delete), mountpoint, nothreads=True, foreground=True, **options)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="count", dest="verbosity",
                      help="print information about interesting calls")
    parser.add_option("-s", "--silent", action="store_true", dest="silent", default=False,
                      help="do not print normal fusepy errors")
    parser.add_option("-m", "--mountpoint", dest="mountpoint",
                      help="mountpoint of the tag filesystem")
    parser.add_option("-d", "--datastore", dest="datastore",
                      help="Data store directory for the tag filesystem")
    parser.add_option("-o", "--options", dest="fuse_options",
                      help="FUSE filesystem options")
    parser.add_option("-f", "--flat-delete", dest="flat_delete", action="store_true", default=False,
                      help="only allow deletion in the root, so that windows doesn't recursively delete everything")
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
