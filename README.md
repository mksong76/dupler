# De-duplication tool

**Features**
* Index files and directories
* Provide UI for removing duplicates

## Install

> Install from local repository
```shell
uv tool install .
```

## Usage

> Scan the directory and make index of files
```shell
dupler init
dupler scan
```

> Remove duplicated files through UI
```shell
dupler dedup
```

> Find files by name
```shell
dupler find "%.py"
```
