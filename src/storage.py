import logging
import errno
from fuse import FuseOSError
import os
from sqlalchemy import create_engine, select, not_, func
from sqlalchemy.orm import Session
from .models.models import Base, File, Tag

class Storage():
    """Facade class for reads and writes.
    Allows different path-organizations."""

    def __init__(self, root: str) -> None:
        self.store = os.path.join(self.root, 'store')
        self.engine = create_engine("sqlite://" + os.path.join(self.root, ".sqlite"))
        self.session = Session(self.engine)
        # check to make sure we have a valid store structure
        if not os.path.exists(self.store):
            logging.info("Could not find actual store directory. Creating directory " + self.store)
            self._setup_db()

    def _setup_db(self):
        """initial setup of database"""
        os.mkdir(self.store)
        Base.metadata.create_all()
        with self.engine.connect() as c:
            c.execute("PRAGMA journal_mode = WAL")
    
    def _store_path(self, name: str):
        return os.path.join(self.store, name)

    def _check_file_path(self, name: str, tags: list[str], tags_exclude: list[str])-> bool:
        s = self.session 
        # grab the associated file object
        query = select(File.tags).where(File.name==name)
        file = s.execute(query).scalar()
        if file is None:
            return False
        true_tags = {tag_obj.name for tag_obj in file.tags}
        if tags_exclude is not None and not set(tags_exclude).isdisjoint(true_tags):
            return False
        return true_tags.issuperset(set(tags)) 
    
    def getxattr(self, name: str, tags: list[str], tags_exclude: list[str], attribute:os.PathLike, *args):
        # we could check if we need to raise ENOENT,
        # if we get ghost files we could try that
        return os.getxattr(self._store_path(name), attribute, *args)

    def access(self, name: str, tags: list[str], tags_exclude: list[str], mode: int):
        # check this is a real file and not a directory
        if not self._check_file_path(name, tags, tags_exclude):
            raise FuseOSError(errno.ENOENT)
        if not os.access(self._store_path(name), mode):
            raise FuseOSError(errno.EACCES)
        # we could check the tags but let's skip that unless we get problems
    
    def chmod(self, name: str, mode: int):
        # access was already checked
        return os.chmod(self._store_path(name), mode)


    def get_files(self, tags: list[str], tags_exclude: list[str]):
        """get the files inside the given tags from the database"""
        s = self.session
        tag_query = select(Tag
        ).where(Tag.name.in_(tags)
        ).subquery()
        included_files = select(Tag.files
        ).where(
            
        ).select_from(tag_query)
        without_excluded = select()

        match = select(File.name
        ).select_from(Tag 
        ).where(Tag.name.value.in_(tags), 
                not_(Tag.name.in_(tags_exclude))
        ).join(Tag.files)


    def get_tags(self, tags: list[str], tags_exclude: list[str]):
        """get the tags inside the given tags from the database"""
    
    def set_tags(self, name: str, tags: list[str]):
        """set the tags of a file in the database"""
    
    def create_file(self, name: str, tags: list[str], mode, dev):
        """create a (non-directory) file 
        in the database and storage folder""" 
    
