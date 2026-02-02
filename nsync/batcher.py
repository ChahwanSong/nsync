from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple

from .common import Batch, utc_timestamp


@dataclass
class FileInfo:
    path: str
    size: int


def scan_paths(root: str, depth: int) -> List[FileInfo]:
    root = os.path.abspath(root)
    files: List[FileInfo] = []
    for current_root, dirs, filenames in os.walk(root, followlinks=False):
        rel_root = os.path.relpath(current_root, root)
        current_depth = 0 if rel_root == "." else rel_root.count(os.sep) + 1
        if current_depth >= depth:
            dirs[:] = []
        for filename in filenames:
            full_path = os.path.join(current_root, filename)
            rel_path = os.path.relpath(full_path, root)
            stat = os.lstat(full_path)
            files.append(FileInfo(path=rel_path, size=stat.st_size))
    return files


def bucketize(files: List[FileInfo], num_buckets: int) -> List[List[FileInfo]]:
    buckets: List[List[FileInfo]] = [[] for _ in range(num_buckets)]
    for index, info in enumerate(files):
        buckets[index % num_buckets].append(info)
    return buckets


def _list_files_under(root: str, rel_dir: str) -> Set[str]:
    base = os.path.join(root, rel_dir)
    file_set: Set[str] = set()
    for current_root, dirs, filenames in os.walk(base, followlinks=False):
        for filename in filenames:
            full_path = os.path.join(current_root, filename)
            rel_path = os.path.relpath(full_path, root)
            file_set.add(rel_path)
    return file_set


def compress_paths(root: str, files: List[str]) -> List[str]:
    root = os.path.abspath(root)
    file_set = set(files)
    dir_candidates: Set[str] = set()
    for path in files:
        parent = os.path.dirname(path)
        while parent and parent != ".":
            dir_candidates.add(parent)
            parent = os.path.dirname(parent)

    sorted_dirs = sorted(dir_candidates, key=lambda p: p.count(os.sep), reverse=True)
    included = set(file_set)
    cache: Dict[str, Set[str]] = {}

    for directory in sorted_dirs:
        if not any(p == directory or p.startswith(directory + os.sep) for p in included):
            continue
        if directory not in cache:
            cache[directory] = _list_files_under(root, directory)
        dir_files = cache[directory]
        if not dir_files:
            continue
        if dir_files.issubset(file_set):
            for item in list(included):
                if item == directory or item.startswith(directory + os.sep):
                    included.discard(item)
            included.add(directory)

    return sorted(included)


def build_batches(
    src_base: str,
    dst_base: str,
    files: List[FileInfo],
    max_files: int,
    max_bytes: int,
) -> List[Batch]:
    batches: List[Batch] = []
    current: List[FileInfo] = []
    total_size = 0

    def flush() -> None:
        nonlocal current, total_size
        if not current:
            return
        file_paths = [item.path for item in current]
        directory_count = len({os.path.dirname(path) for path in file_paths if os.path.dirname(path) not in {"", "."}})
        compressed = compress_paths(src_base, file_paths)
        batches.append(
            Batch(
                task_id=0,
                paths=compressed,
                file_count=len(current),
                directory_count=directory_count,
                estimated_bytes=total_size,
                src_base=src_base,
                dst_base=dst_base,
                created_ts=utc_timestamp(),
            )
        )
        current = []
        total_size = 0

    for info in files:
        if current and (len(current) + 1 > max_files or total_size + info.size > max_bytes):
            flush()
        current.append(info)
        total_size += info.size
    flush()
    return batches
