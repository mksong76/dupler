import enum
import os
import stat
from typing import Callable, Generator, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from sqlalchemy import delete, func, orm, select, update

from . import config, database, model


class TaskType(enum.Enum):
    ROOT = 0
    DIRECTORY = 1
    FILE = 2


class Task:
    def __init__(
        self,
        parent: "Task|TaskManager",
        pgtype: "TaskType",
        name: str,
        total: int,
        *,
        transient: bool = False,
    ):
        if isinstance(parent, TaskManager):
            self.parent = None
            self.tm = parent
        else:
            self.parent = parent
            self.tm = parent.tm
        self.progress = self.tm.progress(pgtype)
        self.pgtype = pgtype
        self.name = name
        self.total = total
        self.transient = transient

    def advance(self, cnt: float = 1):
        self.handle_advance(self.pgtype, cnt)

    def handle_advance(self, pgtype: TaskType, cnt: float = 1):
        if self.pgtype == TaskType.ROOT or self.pgtype == pgtype:
            self.progress.advance(self.task_id, cnt)
        if self.parent is not None:
            self.parent.handle_advance(pgtype, cnt)

    def add_total(self, dirs: float, files: float):
        if self.pgtype == TaskType.ROOT:
            self.total += dirs + files
            self.progress.update(self.task_id, total=self.total)
        elif self.parent is not None:
            self.parent.add_total(dirs, files)

    def __enter__(self):
        if self.pgtype == TaskType.ROOT:
            self.tm.__enter__()
        self.task_id = self.progress.add_task(self.name, total=self.total)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.transient:
            self.progress.remove_task(self.task_id)
        if self.parent is not None:
            self.parent.advance()
        if self.pgtype == TaskType.ROOT:
            self.tm.__exit__(exc_type, exc_val, exc_tb)

    def create_task(
        self, pgtype: "TaskType", name: str, total: int, *, transient: bool = False
    ) -> "Task":
        return self.tm.create_task(
            pgtype, name, total, parent=self, transient=transient
        )


class TaskManager(Live):
    def __init__(self, *, console: Console):
        self.progresses = {
            TaskType.ROOT: Progress(
                SpinnerColumn(),
                TextColumn("{task.description} {task.completed} / {task.total}"),
            ),
            TaskType.DIRECTORY: Progress(
                SpinnerColumn(),
                TextColumn(
                    "{task.description}",
                ),
                BarColumn(),
                TextColumn("{task.completed:>6d}/{task.total:<6d}", justify="left"),
                TimeRemainingColumn(),
            ),
            TaskType.FILE: Progress(
                SpinnerColumn(),
                TextColumn("{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TimeRemainingColumn(),
            ),
        }
        group = Group(*self.progresses.values())
        self.tasks: dict[TaskType, list[Task]] = {}
        super().__init__(group, console=console)

    def progress(self, pgtype: TaskType) -> Progress:
        return self.progresses[pgtype]

    def __enter__(self) -> "TaskManager":
        super().__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

    def create_task(
        self,
        pgtype: TaskType,
        name: str,
        total: int,
        *,
        parent: Optional[Task] = None,
        transient: bool = False,
    ) -> Task:
        progress = Task(parent or self, pgtype, name, total, transient=transient)
        if pgtype not in self.tasks:
            self.tasks[pgtype] = []
        self.tasks[pgtype].append(progress)
        return progress

    def remove_task(self, progress: Task):
        self.tasks[progress.pgtype].remove(progress)

    def task_of(self, pgtype: TaskType, index: int = -1) -> Task:
        return self.tasks[pgtype][index]


class FileManager:
    def __init__(self, out: Console, conn: orm.Session, config: config.Config):
        self.out = out
        self.conn = conn
        self.config = config
        self.base_dir = config.base_dir

    def progress(self) -> TaskManager:
        return TaskManager(console=self.out)

    def is_valid_directory(self, name: str) -> bool:
        return self.config.is_valid_directory(name)

    def is_valid_file(self, name: str) -> bool:
        return self.config.is_valid_file(name)

    BLOCK_SIZE = 1024 * 1024

    def calculate_hash(self, ptask: Task, root: str, file: str) -> bytes:
        import hashlib

        hasher = hashlib.sha256()
        with open(os.path.join(root, file), "rb") as f:
            st = os.fstat(f.fileno())
            with ptask.tm.create_task(
                TaskType.FILE,
                f"Hash [blue]{escape(file)}[/]",
                st.st_size,
                transient=True,
            ) as progress:
                while read := f.read(1024 * 1024):
                    hasher.update(read)
                    progress.advance(len(read))
                return hasher.digest()

    def get_directory(self, path: str) -> Optional[model.Directory]:
        return self.conn.scalar(
            select(model.Directory).where(
                model.Directory.path == path,
            )
        )

    def ensure_directory(
        self, path: str, parent: Optional[model.Directory]
    ) -> model.Directory:
        dir = self.get_directory(path)
        if dir is None:
            dir = model.Directory(path=path, parent_id=parent.id if parent else None)
            self.conn.add(dir)
        return dir

    @staticmethod
    def oid_of(st: os.stat_result) -> str:
        return f"{st.st_dev}:{st.st_ino}"

    def ensure_object(
        self,
        st: os.stat_result,
        get_hash: Optional[Callable[[], bytes]] = None,
        obj: Optional[model.Object] = None,
    ) -> model.Object | None:
        if not stat.S_ISREG(st.st_mode):
            return None
        oid = self.oid_of(st)
        if obj is None or obj.id != oid:
            obj = self.conn.get(model.Object, oid)
        if obj is None:
            hash = get_hash() if get_hash else None
            obj = model.Object(
                id=oid,
                modified=st.st_mtime,
                size=st.st_size,
                hash=hash,
            )
            self.conn.add(obj)
            if hash is not None:
                self.conn.commit()
        else:
            if (
                obj.modified != st.st_mtime
                or obj.size != st.st_size
                or obj.hash is None
            ):
                if get_hash:
                    hash = get_hash()
                    self.out.print(f"Calculated id={obj.id} hash={hash.hex()}")
                else:
                    hash = None
                obj = self.conn.scalars(
                    update(model.Object)
                    .where(
                        model.Object.id == obj.id,
                    )
                    .values(
                        modified=st.st_mtime,
                        size=st.st_size,
                        hash=hash,
                    )
                    .returning(model.Object)
                ).one()
                if hash is not None:
                    self.conn.commit()
        return obj

    def update_file(self, file: model.File, object_id: str | None):
        if file.object_id != object_id:
            file = self.conn.scalars(
                update(model.File)
                .where(
                    model.File.id == file.id,
                )
                .values(
                    object_id=object_id,
                )
                .returning(model.File)
            ).one()
        return file

    def delete_directory(self, dirname: str):
        self.conn.query(model.Directory).filter(
            model.Directory.path == dirname,
        ).delete()

    def delete_dangled_objects(self):
        self.conn.flush()
        n = (
            self.conn.query(model.Object)
            .filter(model.Object.id.not_in(select(model.File.object_id)))
            .delete()
        )
        if n > 0:
            self.out.print(f"Deleted {n} dangled Objects")

    def delete_dangled_files(self):
        self.conn.flush()
        n = (
            self.conn.query(model.File)
            .filter(model.File.object_id.not_in(select(model.Object.id)))
            .delete()
        )
        n += (
            self.conn.query(model.File)
            .filter(model.File.path_id.not_in(select(model.Directory.id)))
            .delete()
        )
        if n > 0:
            self.out.print(f"Deleted {n} dangled Files")

    def delete_dangled_directories(self):
        self.conn.flush()
        while True:
            n = (
                self.conn.query(model.Directory)
                .filter(model.Directory.parent_id.not_in(select(model.Directory.id)))
                .delete()
            )
            if n == 0:
                break
            self.out.print(f"Deleted {n} dangled Directories")

    def scan_directory(self, ptask: Task, root: str, dirs: list[str], files: list[str]):
        dirname = os.path.relpath(root, self.base_dir)

        if dirname == ".":
            pdirname = None
        else:
            pdirname = os.path.relpath(os.path.dirname(root), self.base_dir)
            if config.Config.CONFIG_DIR in dirs:
                try:
                    cfg = config.Config(root)
                    db = database.Database(cfg.database_url)
                    self.import_objects(ptask, db, dirname)
                    db.dispose()
                except BaseException as e:
                    self.out.print(f"Failed to import config: {e!r}")

        dirs[:] = [x for x in dirs if self.is_valid_directory(x)]
        files = [x for x in files if self.is_valid_file(x)]

        ptask.add_total(len(dirs), len(files))

        if pdirname is not None:
            pdir = self.get_directory(pdirname)
            assert pdir is not None
        else:
            pdir = None

        dir = self.ensure_directory(dirname, pdir)

        for cdir in dir.children:
            pname = os.path.relpath(cdir.path, dirname)
            if pname not in dirs:
                self.conn.delete(cdir)
        self.conn.commit()

        if len(files) == 0:
            ptask.advance()
            return

        files.sort()
        with ptask.create_task(
            TaskType.DIRECTORY,
            f"Scan files at [blue]{escape(dirname)}[/]",
            len(files),
            transient=True,
        ) as task:
            self.scan_files(task, root, dir, files)
            self.conn.commit()
            self.out.print(
                f"Scan {task.total:>,d} files at [blue]{escape(dirname)}[/] - [right][green]DONE[/][/]"
            )

    def scan_files(self, task: Task, root: str, dir: model.Directory, files: list[str]):
        db_files = self.conn.scalars(
            select(model.File)
            .where(model.File.path_id == dir.id)
            .order_by(model.File.name)
        ).all()

        for db_file in db_files:
            if db_file.name not in files:
                self.conn.delete(db_file)
            else:
                st = os.lstat(os.path.join(root, db_file.name))
                obj = self.ensure_object(st, obj=db_file.object)
                self.update_file(db_file, obj.id if obj else None)
                files.remove(db_file.name)
                task.advance()

        for filename in files:
            st = os.lstat(os.path.join(root, filename))
            obj = self.ensure_object(st)
            file = model.File(
                path_id=dir.id,
                name=filename,
                type=st.st_mode,
                object_id=(obj.id if obj else None),
            )
            self.conn.add(file)
            task.advance()

    def scan(self):
        with self.progress().create_task(
            TaskType.ROOT,
            f"Scan objects in [blue]{escape(self.base_dir)}[/]",
            1,
            transient=True,
        ) as task:
            try:
                for root, dirs, files in os.walk(self.base_dir):
                    self.scan_directory(task, root, dirs, files)
                self.delete_dangled_objects()
                self.conn.commit()
                self.out.rule(
                    f"Scan {task.total:>,d} objects in [blue]{escape(self.base_dir)}[/] - [green]DONE[/]"
                )
            except KeyboardInterrupt:
                self.conn.rollback()
                raise

    def find_duplicates(self) -> dict[bytes, list[model.File]]:
        items = self.conn.execute(
            select(model.File, model.Object)
            .join(model.Object.files.of_type(model.File))
            .where(
                model.Object.size.in_(
                    select(model.Object.size)
                    .group_by(model.Object.size)
                    .having(func.count(model.Object.id) > 1)
                )
            )
            .order_by(model.Object.size, model.Object.hash)
        ).all()

        with self.progress().create_task(
            TaskType.ROOT,
            f"Scan duplicates in [blue]{escape(self.base_dir)}[/]",
            len(items),
            transient=True,
        ) as task:
            duplicates: dict[bytes, list[model.File]] = {}
            for file, obj in items:
                hash = obj.hash
                if hash is None:
                    hash = self.calculate_hash(
                        task,
                        self.base_dir,
                        file.get_path(),
                    )
                    self.conn.execute(
                        update(model.Object)
                        .where(model.Object.id == obj.id)
                        .values(hash=hash)
                    )
                    self.conn.commit()
                if hash not in duplicates:
                    duplicates[hash] = []
                duplicates[hash].append(file)
            duplicates = {k: v for k, v in duplicates.items() if len(v) > 1}
            return duplicates

    def delete_file(self, file: model.File):
        self.conn.commit()
        self.conn.execute(delete(model.File).where(model.File.id == file.id))
        try:
            full_path = os.path.join(self.base_dir, file.get_path())
            os.remove(full_path)
        except FileNotFoundError:
            pass
        except:
            self.conn.rollback()
            raise
        self.conn.commit()

    def remove_duplicates(
        self, duplicates: dict[bytes, list[model.File]], selection: dict[bytes, int]
    ):
        with self.progress().create_task(
            TaskType.ROOT, f"Remove Duplicates", len(selection)
        ) as task:
            for hash, id in selection.items():
                for file in duplicates[hash]:
                    if file.id != id:
                        self.delete_file(file)
                        task.advance()
            self.delete_dangled_objects()
            self.conn.commit()

    def import_objects(self, task: Task, db: database.Database, dirname: str):
        self.conn.flush()
        with db.session() as conn:
            object_ids = self.conn.scalars(
                select(model.File.object_id).where(
                    model.File.path_id.in_(
                        select(model.Directory.id).where(
                            model.Directory.path.like(f"{dirname}%")
                        )
                    )
                )
            ).all()
            total = conn.scalars(
                select(func.count(model.Object.id)).where(
                    model.Object.id.not_in(object_ids)
                )
            ).one()
            if total == 0:
                return
            self.out.print(f"Importing {total} Objects")
            objects = conn.scalars(
                select(model.Object).where(model.Object.id.not_in(object_ids))
            )
            with task.tm.create_task(
                TaskType.DIRECTORY, "Import Object:", total, transient=True
            ) as task:
                for obj in objects:
                    nobj = model.Object(
                        id=obj.id,
                        size=obj.size,
                        modified=obj.modified,
                        hash=obj.hash,
                    )
                    self.conn.add(nobj)
                    task.advance()

    def find_files(self, key: str) -> Generator[tuple[str, str, int], None, None]:
        skey = key if "%" in key else f"%{key}%"
        items = self.conn.scalars(
            select(model.File)
            .join(model.Object)
            .join(model.Directory)
            .where(model.File.name.like(skey))
            .order_by(model.Directory.path, model.File.name)
        )
        for file in items:
            name = file.name
            path = file.directory.path
            size = file.object.size
            yield name, path, size
