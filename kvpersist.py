"""KV-backed persistence for Lunel (for hosts with no volume / PostgreSQL).

Platforms such as Mojave give a service an ephemeral disk but offer a managed
Valkey/Redis "KV Store" (connection string injected as ``KV_URL``). This module
keeps Lunel's data directory alive across restarts by:

  * restoring a snapshot from KV on boot (only when the local disk is empty)
  * writing a fresh snapshot every ``LUNEL_KV_INTERVAL`` seconds (default 30)
    and again on shutdown, skipping writes when nothing changed

The snapshot is one gzip-compressed tar of the data directory containing the
SQLite database (copied with SQLite's online-backup API, so it is consistent),
the generated secret key, the worker registry and every instance's
``state.json``. Log files are excluded.

It is a no-op when ``KV_URL`` is not set or the ``redis`` package is missing,
and it never raises into the caller.
"""
from __future__ import annotations

import atexit
import gzip
import hashlib
import io
import os
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import threading
        url = "rediss://" + url[len("valkeys://"):]
    elif url.startswith("valkey://"):
        url = "redis://" + url[len("valkey://"):]
    try:
        import redis  # type: ignore
    except ImportError:
        _log("package 'redis' is not installed - persistence disabled")
        return None
    _cli = redis.Redis.from_url(url, socket_timeout=15, socket_connect_timeout=15)
    return _cli


def _add(tar: tarfile.TarFile, path: Path, arcname: str) -> None:
    try:
        data = path.read_bytes()
    except OSError:
        return  # file vanished or is mid-write; the next cycle picks it up
    info = tarfile.TarInfo(arcname)
    info.size = len(data)
    info.mtime = 0
    info.mode = 0o600
    tar.addfile(info, io.BytesIO(data))


def _pack(base: Path) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, compresslevel=6) as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            db = base / "lunel.db"
            if db.exists():
