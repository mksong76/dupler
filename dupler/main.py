import fnmatch
import os

import click
from rich import traceback
from rich.console import Console

from . import config, database, filemanager, app

traceback.install(
    show_locals=True,
    suppress=[click],
)


@click.group()
def main():
    pass


@main.command("init")
def init(dir: str | None = None):
    """
    Initialize the database for the directory
    """
    config.init(dir)


@main.command("scan")
def scan():
    """
    Scan objects under the directory
    """
    try:
        cfg = config.get_instance()
    except config.NoConfigError:
        yn = click.prompt("Not initialized, init here?", default="yes", type=click.BOOL)
        if not yn:
            raise
        config.init(".")
        cfg = config.get_instance()

    out = Console(stderr=True)
    db = database.get_database()
    with db.session() as conn:
        s = filemanager.FileManager(out, conn, cfg)
        s.scan()


@main.command("dedup")
def deduplicates():
    """
    Remove duplicates under the directory
    """
    cfg = config.get_instance()
    out = Console(stderr=True)
    db = database.get_database()
    with db.session() as conn:
        fm = filemanager.FileManager(out, conn, cfg)
        with out.status("Finding Duplicates"):
            duplicates = fm.find_duplicates()
        ui = app.DeDeplicate(fm, duplicates)
        ui.run()
        if not ui.apply:
            return
        fm.remove_duplicates(duplicates, ui.selection)


@main.command("exclude")
@click.option("--dir", "-d", is_flag=True, help="For directory (default: file)")
@click.option("--remove", "-r", is_flag=True, help="To remove patterns (default: add)")
@click.option("--test", "-t", is_flag=True, help="To test patterns (default: modify)")
@click.argument("patterns", metavar="<pattern>...", nargs=-1)
def exclude(patterns: list[str], dir: bool, remove: bool, test: bool):
    """
    Add or remove or test exclude patterns
    """
    cfg = config.get_instance()
    out = Console(stderr=True)

    if len(patterns) == 0:
        out.print(f"Files      : {cfg.settings.exclude_files!r}")
        out.print(f"Directories: {cfg.settings.exclude_directories!r}")
        return

    if test:
        validate_item = cfg.is_valid_directory if dir else cfg.is_valid_file
        for name in patterns:
            yn = validate_item(name)
            out.print("Valid" if yn else "Invalid", name)
        return

    if remove:
        handle_pattern = cfg.remove_exclude_dir if dir else cfg.remove_exclude_file
    else:
        handle_pattern = cfg.add_exclude_dir if dir else cfg.add_exclude_file
    for pat in patterns:
        handle_pattern(pat)
    cfg.save()


@main.command("gc")
def collect_garbage():
    """
    Delete dangled directories, files and objects
    """
    cfg = config.get_instance()
    out = Console(stderr=True)
    db = database.get_database()
    with db.session() as conn:
        fm = filemanager.FileManager(out, conn, cfg)
        fm.delete_dangled_directories()
        fm.delete_dangled_files()
        fm.delete_dangled_objects()
        conn.commit()


@main.command("find")
@click.argument("pattern", metavar="<pattern>")
def find(pattern: str):
    """
    Find files in the storage
    """
    cfg = config.get_instance()
    out = Console(stderr=True)
    db = database.get_database()
    with db.session() as conn:
        fm = filemanager.FileManager(out, conn, cfg)
        for name, path, size in fm.find_files(pattern):
            file = os.path.normpath(os.path.join(".", path, name))
            print(file)


if __name__ == "__main__":
    main()
