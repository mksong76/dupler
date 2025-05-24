import fnmatch
import os
from os import path
import re
from typing import Optional

import click
from pydantic import BaseModel

from . import context


class Settings(BaseModel):
    exclude_files: list[str] = []
    exclude_directories: list[str] = []


class Config:
    CONFIG_DIR = ".dupler"
    DATABASE_FILE = "database.sqlite3"
    SETTINGS_FILE = "settings.json"

    def __init__(self, base_dir):
        self.__base_dir = base_dir
        self.__data_dir = path.join(base_dir, self.CONFIG_DIR)
        setting_file = self.path_for(self.SETTINGS_FILE)
        if os.path.exists(setting_file):
            with open(setting_file, "rt") as f:
                self.settings = Settings.model_validate_json(f.read())
        else:
            self.settings = Settings()
        self.__ex_files = None
        self.__ex_dirs = None

    @property
    def base_dir(self) -> str:
        return self.__base_dir

    @property
    def data_dir(self) -> str:
        return self.__data_dir

    @property
    def database_url(self) -> str:
        return f"sqlite+pysqlite:///{self.path_for(self.DATABASE_FILE)}"

    def path_for(self, *names) -> str:
        p = path.join(self.__data_dir, *names)
        dir = path.dirname(p)
        if not path.exists(dir):
            os.makedirs(dir)
        return p

    @staticmethod
    def regex_for_patterns(patterns: list[str]) -> re.Pattern:
        regexs = [fnmatch.translate(pat) for pat in patterns]
        return re.compile("^(" + "|".join(regexs) + ")$")

    def regex_ex_files(self) -> re.Pattern:
        if self.__ex_files is None:
            self.__ex_files = self.regex_for_patterns(self.settings.exclude_files)
        return self.__ex_files

    def regex_ex_dirs(self) -> re.Pattern:
        if self.__ex_dirs is None:
            self.__ex_dirs = self.regex_for_patterns(self.settings.exclude_directories)
        return self.__ex_dirs

    def add_exclude_file(self, pattern: str):
        self.settings.exclude_files.append(pattern)
        self.__ex_files = None

    def remove_exclude_file(self, pattern: str):
        self.settings.exclude_files.remove(pattern)
        self.__ex_files = None

    def add_exclude_dir(self, pattern: str):
        self.settings.exclude_directories.append(pattern)
        self.__ex_dirs = None

    def remove_exclude_dir(self, pattern: str):
        self.settings.exclude_directories.remove(pattern)
        self.__ex_dirs = None

    def is_valid_file(self, name: str) -> bool:
        if name.startswith("."):
            return False
        if self.regex_ex_files().match(name):
            return False
        return True

    def is_valid_directory(self, name: str) -> bool:
        if name == Config.CONFIG_DIR:
            return False
        if name.startswith("."):
            return False
        if self.regex_ex_dirs().match(name):
            return False
        return True

    def save(self):
        with open(self.path_for(self.SETTINGS_FILE), "wt") as f:
            f.write(self.settings.model_dump_json(indent=2, exclude_defaults=True))

    @classmethod
    def has_config(cls, dir: str) -> bool:
        data_dir = path.join(dir, cls.CONFIG_DIR)
        return path.isdir(data_dir)

    @classmethod
    def init_dir(cls, dir: str):
        data_dir = path.join(dir, cls.CONFIG_DIR)
        os.makedirs(data_dir)


class NoConfigError(click.UsageError):
    pass


def init(dir: Optional[str] = None):
    Config.init_dir(dir or ".")


def get_instance() -> Config:
    def create_config() -> Config:
        base_dir = path.realpath(path.curdir)
        while True:
            if Config.has_config(base_dir):
                try:
                    return Config(base_dir)
                except:
                    pass
            if base_dir == "/":
                break
            base_dir = path.dirname(base_dir)
        raise NoConfigError("No config found")

    return context.get_value("CONFIG", factory=create_config)
