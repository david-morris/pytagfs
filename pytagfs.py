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
def as_tags(path):
    if path[-1] != '/':
        path = path[:path.rindex('/')]
    if len(path) < 2: # check for root/file in root.
        return []
    path = path.strip('/')
    return [t.lstrip('.') for t in path.split("/")]

def _file_name(path):
    return path.split('/')[-1].strip('.')

class Tagfs(Operations):
    def __init__(self, root):
        logging.info("init on "+ root)
        self.root = root
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

    def access(self, path, mode):
        logging.info("API: access")
        if not os.access(self._store_path(path), mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        logging.info("API: chmod")
        return os.chmod(self._store_path(path), mode)

    def chown(self, path, uid, gid):
        logging.info("API: chown")
        return os.chown(self._store_path(path), uid, gid)

    def getattr(self, path, fh=None):
        logging.info("API: getattr")
        logging.debug("Path: " + path)
        perm = 0o777
        # we're going to lie about the number of hardlinks we have to path (st_nlinks). 
        # internally, we should be able to get away with it because deleting tags should never delete media.

        if path[-1] == '/': # we have a directory
            for tag in as_tags(path):
                if tag not in self.contents.keys():
                    return -errno.ENOENT
            st = os.lstat(self.store)
            return {key: getattr(st, key) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}
            return 
        full_path = self._store_path(path)
        st = os.lstat(full_path)
        for tag in as_tags(path):
            if tag not in self.contents.keys():
                return -errno.ENOENT
        return {key: getattr(st, key) for key in
                ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}

    def readdir(self, path, fh):
        '''Implements directory listing as a generator.
        The path is just split into tags.  No tag may be a filename.
        Items matching all tags are listed.  Those which are additionally
        members of other tags are hidden.  Existing tags will be shown if they
        contain some of those hidden members, otherwise hidden.'''
        logging.info("API: readdir")
        tags = as_tags(path)
        tset = set(tags)
        dirents = ['.', '..']
        logging.debug("Contents:")
        logging.debug(str(self.contents))
        logging.debug("Tags:")
        logging.debug(str(self.tags))
        if len(tags) == 0:
            dirents.extend(['.' + d if len(self.contents[d]) == 0 else d
                            for d in self.tags.keys()])
            dirents.extend(['.' + f if len(self.tags[f]) == 0 else f
                            for f in os.listdir(self.store)])
        else:
            matches = set.intersection(*[self.contents[tag] for tag in tags])
            dirents.extend(['.' + d if len(self.contents[d].intersection(matches)) == 0
                            else d for d in self.contents.keys()])
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
        logging.info("API: rmdir")
        # check we don't have a leaf
        # if os.path.isfile(_store_path(path)):
        #     raise OSError(20, "Not a tag")
        tag = path.split('/')[-1].strip() # note: it better be hidden or we'll throw 39
        # check we are trying to delete a real tag
        if tag not in contents.keys():
            raise OSError(2, "No such tag")
        # check that the tag has no members anywhere
        if contents[tag] != set():
            raise OSError(39, "Tag still applies to files")
        # remove it (it's not in any tags)
        del contents[tag]
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

    def unlink(self, path): # what's this supposed to do?  How's it supposed to read mv's mind?
        logging.info("API: unlink")
        # check if the file exists
        if not os.path.isfile(self._store_path(path)):
            raise OSError(2, "No such file")
        path_parts = [x.strip() for x in path.split('/')]
        # check if we have a tagless item
        if len(path_parts) == 1:
            return os.unlink(self._store_path(path))
        # check if we have the right tags
        for tag in path_parts[0:-1]:
            if tag not in self.tags[name]:
                raise OSError(2, "File described with incorrect tags.")
        # remove the last tag
        tags[path_parts[-1]].remove(path_parts[-2])
        contents[path_parts[-2]].remove(path_parts[-1])

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
        '''Changes the tag lists of a file.'''
        logging.info("API: rename")
        # FIXME clear up by rewriting entirely
        path_parts_old = [x.strip() for x in old.split('/')]
        path_parts_new = [x.strip() for x in new.split('/')]
        # check to make sure the old file exists
        if not os.path.isfile(self._store_path(old)):
            raise OSError(2, "No such file")
        # check to make sure the new name is the same or free
        if path_parts_old[-1] != path_parts_new[-1] and os.path.isfile(self._store_path(new)):
            raise OSError(38, "Overwriting disallowed")
        # check to make sure the new name is not a directory
        if path_parts_new[-1] in contents.keys():
            raise OSError(21, "Can't replace a tag name with a file name.")
        old_tags = set(path_parts_old[0:-1])
        # check to make sure the old tags were accurate
        for tag in old_tags:
            if tag not in tags[path_parts_old[-1]]:
                raise OSError(2, "Incorrect tags.")
        new_tags = set(path_parts_new[0:-1])
        # check to make sure the new tags all exist
        for tag in new_tags:
            if tag not in contents.keys():
                raise OSError(2, "No such directory")
        # check if we change the entry name
        if path_parts_new[-1] != path_parts_old[-1]:
            tags[path_parts_new[-1]] = new_tags
            for tag in new_tags:
                contents[tag].add(path_parts_new[-1])
            for tag in old_tags:
                contents[tag].remove(path_parts_old[-1])
            del tags[path_parts_old[-1]]
            return os.rename(self._store_path(old), self._store_path(new))
        # compute tags to remove
        name = path_parts_old[-1]
        #tags[name] -= old_tags - new_tags
        for tag in old_tags - new_tags:
            contents[tag] -= name
        # compute tags to add
        tags[name] |= new_tags
        for tag in new_tags - old_tags:
            contents[tag].add(name)
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
        logging.info("API: open")
        store_path = self._store_path(path)
        return os.open(store_path, flags)

    def create(self, path, mode):
        logging.info("API: create")
        logging.debug("path: " + path)
        store_path = self._store_path(path)
        tags = as_tags(path)
        name = _file_name(path)
        self.tags[name]=set(tags)
        for tag in tags:
            self.contents[tag].add(name)
        self._flush_tags()
        os.close(os.fdopen(store_path, os.O_WRONLY | os.O_CREAT, mode))
        return 0
    # def create(self, path, mode, fi=None):
    #     full_path = self._full_path(path)
    #     # TODO handle tags!
    #     return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        logging.info("API: read")
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
        logging.info("API: flush")
        return os.fsync(fh)

    def release(self, path, fh):
        logging.info("API: release")
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        logging.info("API: fsync")
        return self.flush(path, fh)


def main(mountpoint, root, options):
    logging.info("Mountpoint: "+ str(mountpoint)+ ", root: "+ str(root))
    FUSE(Tagfs(root), mountpoint, nothreads=True, foreground=True, **options)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="print information about interesting calls")
    parser.add_option("-m", "--mountpoint", dest="mountpoint",
                      help="mountpoint of the tag filesystem")
    parser.add_option("-d", "--datastore", dest="datastore",
                      help="Data store directory for the tag filesystem")
    parser.add_option("-o", "--options", dest="fuse_options",
                      help="FUSE filesystem options")
    options, args = parser.parse_args()
    if options.verbose:
        logging.root.setLevel(logging.DEBUG)
        logging.info("Verbose logging enabled.")
    if options.fuse_options is not None:
        kwargs = {opt: True for opt in options.fuse_options.split(",")}
        logging.info("FS options: " + str(kwargs))
    else:
        kwargs = {}
    main(options.mountpoint, options.datastore, kwargs)
