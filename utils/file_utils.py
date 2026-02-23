import contextlib
import json
import os
import pathlib
import sys
from typing import TextIO, cast

from pymongo.errors import InvalidOperation


def read_json(file_path: str):
    with open(file_path, "r") as f:
        return json.load(f)


def write_json(file_path: str, data: object):
    # Ensure parent directory exists before writing JSON
    pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


@contextlib.contextmanager
def smart_open(
    filename: str | None = None,
    mode: str = "w",
    binary: bool = False,
    create_parent_dirs: bool = True,
):
    fh = get_file_handle(filename, mode, binary, create_parent_dirs)
    try:
        yield fh
    finally:
        fh.close()


def get_file_handle(
    filename: str | None,
    mode: str = "w",
    binary: bool = False,
    create_parent_dirs: bool = True,
) -> TextIO:
    """
    - If create_parent_dirs=True and writing/append mode, create parent directories.
    - Supports '-' for stdin/stdout.
    - Never replace filename with dirname (avoid opening a directory by mistake).
    """
    if filename is None:
        raise ValueError("Filename cannot be None.")

    # Detect if mode is for writing/appending
    writing = any(ch in mode for ch in ("w", "a", "+"))

    if create_parent_dirs and writing and filename != "-":
        pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)

    full_mode = mode + ("b" if binary else "")

    if filename == "-":
        fd = sys.stdout.fileno() if "w" in mode else sys.stdin.fileno()
        return cast(TextIO, os.fdopen(fd, full_mode))

    return cast(TextIO, open(filename, full_mode))


def write_to_file(file: str, content: InvalidOperation | str):
    # Ensure content is string before writing
    data = content if isinstance(content, str) else str(content)
    with smart_open(file, "w") as file_handle:
        _ = file_handle.write(data)


# ========= Cursor (last_synced_file) =========


def init_last_synced_file(value: int, last_synced_file: str):
    """
    Create a cursor file if it does not exist; if it already exists, raise error.
    """
    if os.path.isfile(last_synced_file):
        raise ValueError(
            (
                f"{last_synced_file} should not exist if any --start option is specified. "
                f"Either remove the {last_synced_file} file or the --start-block option."
            )
        )
    write_last_synced_file(last_synced_file, value)


def write_last_synced_file(file: str, last_synced_value: int):
    # Ensure parent directory exists
    pathlib.Path(file).parent.mkdir(parents=True, exist_ok=True)
    write_to_file(file, f"{last_synced_value}\n")


def read_last_synced_file(file: str) -> int:
    with smart_open(file, "r", create_parent_dirs=False) as file_handle:
        # int() handles trailing newline/whitespace
        return int(file_handle.read().strip())


# ========= Log files =========


def init_log_file(log_file: str):
    # Ensure parent directory exists
    pathlib.Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(log_file):
        os.remove(log_file)


def append_log_file(line: str, log_file: str):
    # Append a line to the log file, ensure parent directory exists
    pathlib.Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a+", encoding="utf-8") as f:
        _ = f.write(f"{line}\n")
