import os
import errno
import logging
from sqlalchemy import create_engine, update, select
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from fuse import FUSE, FuseOSError, Operations

from storage import Storage
from .models.models import Base, File, Tag
from .helpers import file_name, file_tags, dir_tags
import stat

class Tagfs(Operations):
    def __init__(self, root: str, mount: str, flat_delete: bool, hidden_limit: int):
        logging.info("init on "+ root)
        self.root = root
        self.mount = mount
        self.hidden_limit = hidden_limit
        self.flat_delete = flat_delete
        self.storage = Storage(root)
        # these should be removed after setting up the facade
        self.store = os.path.join(self.root, 'store')
        self.engine = create_engine("sqlite://" + os.path.join(self.root, ".sqlite"))

    def _consistent_file_path(self, path: str):
        name = file_name(path)
        tags = file_tags(path)
        s = self.engine # we might want a session later?
        # grab the associated file object
        file = s.query(File).filter_by(name=name).first()
        if file is None:
            return False
        true_tags = {tag_obj.name for tag_obj in file.tags}
        return set(tags) <= true_tags

    def _store_path(self, tag_path: str):
        return os.path.join(self.store, tag_path.split('/')[-1].lstrip('.'))
        
    def _name(self, tag_path: str):
        return tag_path.split('/')[-1].lstrip('.')

    def _tags(self, tag_path: str):
        if len(tag_path) < 2:
            return []
        return [t.lstrip('.') for t in tag_path.strip('/').split('/')]

    def _tags_exclude(self, tag_path: str)->list[str]:
        return []

    def _specifier(self, tag_path: str)-> tuple[str, list[str], list[str]]:
        return (self._name(), self._tags(), self._tags_exclude())


    def getxattr(self, path: str, name: os.PathLike, *args):
        logging.info("API: getxattr " + path + ", " + str(name) + ", " +str(args))
        return self.storage.getxattr(*self._specifier(), name, *args)

    def access(self, path: str, mode: int):
        logging.info("API: access " + path + " " + oct(mode))
        self.storage.access(self._specifier(path), mode)
        logging.debug("Permission granted: " + path + " " + oct(mode))

    def chmod(self, path: str, mode:int):
        logging.info("API: chmod")
        return self.storage.chmod(self._name(path), mode)

    def chown(self, path, uid, gid):
        logging.info("API: chown")
        return storage.chown(self._name(path), uid, gid)

    def getattr(self, path, fh=None):
        logging.info("API: getattr " + path)
        if path == "/..deleteme":
            raise FuseOSError(errno.ENOENT)
        perm = 0o777
        # we're going to lie about the number of hardlinks we have to path (st_nlinks). 
        # internally, we should be able to get away with it because deleting tags should never delete media.

        full_path = self._store_path(path)
        if path[-1] == '/' or file_name(path) not in self._files():
            # we (may) have a directory
            for tag in dir_tags(path):
                if tag not in self._tags():
                    logging.debug(tag + " not in " + str(self._tags()))
                    raise FuseOSError(errno.ENOENT)
            st = os.lstat(self.store)
            return {key: getattr(st, key) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                     'st_mtime', 'st_nlink', 'st_size', 'st_uid')}

        st = os.lstat(full_path)
        if not self._consistent_file_path(path):
            logging.debug(path + " deemed inconsistent")
            raise FuseOSError(errno.ENOENT)
        return {key: getattr(st, key) for key in
                ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}

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
            if self.hidden_limit == -1:
                logging.debug("No hidden limit.")
                file_ents = self.con.execute("""SELECT CASE
                WHEN tag_id IS NULL THEN name
                ELSE '.' ||  name END
                FROM files LEFT JOIN file_tags ON files.id = file_tags.file_id
                """).fetchall()
            else:
                logging.debug("Hidden limit: " + str(self.hidden_limit))
                file_ents = self.con.execute("""SELECT name
                FROM files LEFT JOIN file_tags ON files.id = file_tags.file_id
                WHERE tag_id IS NULL
                UNION
                SELECT '.' || name FROM 
                files LEFT JOIN file_tags ON files.id = file_tags.file_id
                WHERE tag_id IS NOT NULL
                LIMIT ?""", (self.hidden_limit,))
            logging.debug("file ents: " + str(file_ents))
            dirents.extend([x[0] for x in file_ents])
        else:
            file_select = """(
SELECT path_tag_count.file AS file, other_tags.tag AS tag
FROM ( SELECT file, COUNT(tag) AS count
       FROM taggings WHERE tag IN (""" + ', '.join(["?"]*len(tags)) + """ )
       GROUP BY file
     ) path_tag_count
LEFT JOIN ( SELECT tag, file
            FROM taggings
            WHERE tag NOT IN (
       """ + ', '.join(["?"]*len(tags)) + """ )
          ) other_tags
ON path_tag_count.file = other_tags.file
WHERE path_tag_count.count = ? )""" # , tags + tags + [len(tags)])
            

            with self.con as c:
                test_query = c.execute(file_select[1:-2],
                                       tags + tags + [len(tags)]).fetchall()
                logging.debug("test query: " + str(test_query))

                file_ents = c.execute("""SELECT CASE
                WHEN tag IS NULL THEN file
                ELSE  '.' || file END
                FROM ( SELECT file, tag FROM
                """+ file_select + """
                GROUP BY file) AS unique_files""",
                                      tags + tags + [len(tags)]).fetchall()
                logging.info("file ents: " + str(file_ents))
                dirents.extend([x[0] for x in file_ents])
                
                tag_ents = c.execute("""SELECT CASE
                WHEN file IS NULL THEN '.' || other_tags.tag
                ELSE other_tags.tag END
                FROM (SELECT name AS tag FROM tags WHERE name NOT IN (
                """+ ', '.join(["?"]*len(tags)) + """ )) AS other_tags
                LEFT JOIN ( SELECT tag, file FROM
                """+ file_select +""" AS file_select
                          GROUP BY tag) AS file_join
                ON other_tags.tag = file_join.tag""", tags + tags + tags + [len(tags)]).fetchall()
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
        raw = path.split('/')
        if raw[-1] == '':
            raw.pop()
        raw = raw[-1]
        if raw[0] == 0:
            raise FuseOSError(errno.EPERM)
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
            #if c.execute("SELECT 1 FROM tags t INNER JOIN file_tags d ON t.id = d.tag_id").fetchone() is not None:
            if (x := c.execute("SELECT 1 FROM taggings WHERE tag = ?",
                              (tag,)).fetchone()) is not None:
                logging.debug("tag contains: " + str(x))
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
            c = c.cursor()
            c.execute("INSERT INTO files (name) VALUES (?)", (name,))
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
        with self.session() as c:
            if old[-1] == '/' or not c.execute(exists().where(File.name == old_name)).scalar():
                # we are dealing with a (potentially bad) directory
                logging.debug("renaming as directory")
                old_tags = dir_tags(old)
                new_tags = dir_tags(new)
                # check to make sure all of these tags exist
                old_tag_count = len(c.query(Tag.id).filter(Tag.name.in_(old_tags)).all())
                logging.debug("old_tag_count: " + str(old_tag_count) + ", given old tags: " + str(old_tags))
                if old_tag_count < len(old_tags):
                    raise FuseOSError(errno.ENOENT)
                # if someone adds extra dirs after the one they want to change, that's not covered
                for t_old, t_new in zip(old_tags[:-1], new_tags[:-1]):
                    if t_old != t_new:
                        raise FuseOSError(errno.ENOSYS)
                old_tag = old_tags[-1]
                new_tag = new_tags[-1]
                if len(old_tags) == 1 and new == "/..deleteme": # magic dir name to delete a tag easily
                    self.rmdir(old)
                    return
                if not c.execute(exists().where(Tag.name == new_tag)).scalar():
                    raise FuseOSError(errno.EEXIST)
                c.execute(update(Tag).where(Tag.name == new_tag).values(name=new_tag))
            else:
                logging.debug("renaming as file")
                # handle taglist change
                if set(from_tags := file_tags(old)) != set(to_tags := file_tags(new)):
                    if not self._consistent_file_path(old):
                        logging.debug(old + " deemed inconsistent")
                        raise FuseOSError(errno.ENOENT)
                    if len(from_tags) == 0 or old.split('/')[-1][0] == ".": # add only
                        file_obj = c.scalars(select(File).filter(File.name == old_name)).first()
                        file_obj.add_tags_by_name(to_tags)
                    else:
                        file_obj = c.scalars(select(File).filter(File.name == old_name)).first()
                        file_obj.del_tags_by_name(set(from_tags) - set(to_tags))
                        file_obj.add_tags_by_name(set(to_tags) - set(from_tags))

                # handle filename change
                if old_name != new_name:
                    old_path = self._store_path(old)
                    new_path = self._store_path(new)
                    file_obj = c.scalars(select(File).filter(File.name == old_name)).first()
                    file_obj.name = new_name
                    os.rename(old_path, new_path)

    def link(self, target, name):
        logging.info("API: link " + target + " to " + name)
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
            logging.debug("RowID: " + str(id))
            cur.executemany("""INSERT INTO file_tags (file_id, tag_id)
            SELECT ?,id FROM tags WHERE name = ?""",
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
