from typing import Iterable
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, Session
from sqlalchemy import Table, Column, select
from sqlalchemy import UniqueConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

class Base(DeclarativeBase):
    pass

file_tags = Table(
    "file_tags",
    Base.metadata,
    Column("id", primary_key=True),
    Column("file_id", ForeignKey("files.id")),
    Column("tag_id", ForeignKey("tags.id")),
    UniqueConstraint("file_id", "tag_id")
)

class Tag(Base):
    __tablename__ = "tags"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    files: Mapped[list["File"]] = relationship(
        secondary=file_tags, back_populates="tags")

    def __repr__(self) -> str:
        return f"Tag {self.name!r}"
    
        


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    tags: Mapped[list[Tag]] = relationship(
        secondary=file_tags, back_populates="files")

    def add_tags_by_name(self, new_tags: Iterable[str]):
        """new_tags should be valid names of tags 
        not yet in this file's tags"""
        # if this fails we may need a session object
        self.tags.append(select(Tag).filter(Tag.name.in_(new_tags)))
    
    def del_tags_by_name(self, tag_names: Iterable[str]):
        # unoptimized
        for i in range(len(self.tags)):
            if (tag := self.tags[i]).name in tag_names:
                self.tags.remove(tag)


    def __repr__(self) -> str:
        return f"File {self.name!r}"
