import os
from typing import Optional
from sqlalchemy import *
from sqlalchemy.orm import *


class Base(DeclarativeBase):
    pass


class Directory(Base):
    __tablename__ = "directories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column(String(1024))
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("directories.id"))

    __table_args__ = (UniqueConstraint("path"),)

    files: Mapped[list["File"]] = relationship(
        back_populates="directory",
        cascade="all, delete",
    )
    parent: Mapped["Directory"] = relationship(
        back_populates="children", remote_side=id
    )
    children: Mapped[list["Directory"]] = relationship(
        back_populates="parent",
        cascade="all, delete",
    )

    def __repr__(self):
        return f"Directory(path={self.path!r})"


class Object(Base):
    __tablename__ = "objects"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    size: Mapped[int] = mapped_column(BigInteger)
    modified: Mapped[int] = mapped_column(BigInteger)
    hash: Mapped[Optional[bytes]] = mapped_column(BINARY(32), nullable=True)

    files: Mapped[list["File"]] = relationship(
        back_populates="object",
    )


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path_id: Mapped[int] = mapped_column(
        ForeignKey("directories.id", ondelete="CASCADE")
    )
    name: Mapped[str] = mapped_column(String(1024))
    type: Mapped[int] = mapped_column(Integer)
    object_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("objects.id"), nullable=True
    )

    directory: Mapped["Directory"] = relationship(
        back_populates="files",
    )
    object: Mapped["Object"] = relationship(
        back_populates="files",
    )

    __table_args__ = (UniqueConstraint("path_id", "name"),)

    def __repr__(self):
        return f"File(path_id={self.path_id!r}, name={self.name!r}, type={self.type!r}, object_id={self.object_id!r})"

    def get_path(self):
        if self.directory.path == ".":
            return self.name
        return os.path.join(self.directory.path, self.name)
