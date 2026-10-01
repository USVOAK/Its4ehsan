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
import time
from pathlib import Path

KEY = os.environ.get("LUNEL_KV_KEY", "lunel:snapshot:v1")
MAX_BYTES = 64 * 1024 * 1024
_DB_FILES = {"lunel.db", "lunel.db-wal", "lunel.db-shm"}
_SKIP_SUFFIXES = (".log", ".tmp", ".restoring")

_lock = threading.Lock()
_base: Path | None = None
_cli = None
_safe_to_write = True
_last_hash: str | None = None
_last_prev_rotation = 0.0


def _log(msg: str) -> None:
    print(f"[lunel][kv] {msg}", file=sys.stderr, flush=True)


def _client():
    """Return a cached Redis/Valkey client, or None when unavailable."""
    global _cli
    if _cli is not None:
        return _cli
    url = os.environ.get("KV_URL", "").strip()
    if not url:
        return None
    if url.startswith("valkeys://"):
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
                tmpdir = tempfile.mkdtemp(prefix="lunel-snap-")
                try:
                    copy = Path(tmpdir) / "lunel.db"
                    src = sqlite3.connect(str(db), timeout=10)
                    try:
                        dst = sqlite3.connect(str(copy))
                        try:
                            src.backup(dst)
                        finally:
                            dst.close()
                    finally:
                        src.close()
                    _add(tar, copy, "lunel.db")
                finally:
                    shutil.rmtree(tmpdir, ignore_errors=True)
            for path in sorted(base.rglob("*")):
                if path.is_symlink() or not path.is_file():
                    continue
                rel = path.relative_to(base).as_posix()
                if rel in _DB_FILES or rel.endswith(_SKIP_SUFFIXES):
                    continue
                _add(tar, path, rel)
    return raw.getvalue()


def restore(base: Path) -> bool:
    """Restore the data dir from KV if the local disk has no Lunel data."""
    global _base, _safe_to_write
    _base = base
    try:
        if (base / "lunel.db").exists() or (base / "instances" / "registry.json").exists():
            _log("local data found - not restoring from KV")
            return False
        client = _client()
        if client is None:
            return False
        try:
            blob = client.get(KEY)
        except Exception as exc:  # network/auth problem: never overwrite a good snapshot
            _safe_to_write = False
            _log(f"cannot read KV ({exc.__class__.__name__}) - snapshots disabled this run")
            return False
        if not blob:
            _log("no snapshot in KV yet - starting fresh")
            return False
        count = 0
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
            for member in tar.getmembers():
                parts = Path(member.name).parts
                if not member.isfile() or member.name.startswith("/") or ".." in parts:
                    continue
                dest = base.joinpath(*parts)
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_name(dest.name + ".restoring")
                tmp.write_bytes(tar.extractfile(member).read())
                os.chmod(tmp, 0o600)
                os.replace(tmp, dest)
                count += 1
        _log(f"restored {count} files from KV snapshot ({len(blob)} bytes)")
        return True
    except Exception as exc:
        _safe_to_write = False
        _log(f"restore failed ({exc.__class__.__name__}: {exc}) - snapshots disabled this run")
        return False


def snapshot(force: bool = False) -> bool:
    """Write a snapshot to KV if anything changed. Returns True when written."""
    global _last_hash, _last_prev_rotation
    if _base is None or not _safe_to_write:
        return False
    with _lock:
        try:
            client = _client()
            if client is None:
                return False
            blob = _pack(_base)
            if len(blob) > MAX_BYTES:
                _log(f"snapshot too large ({len(blob)} bytes) - skipped")
                return False
            digest = hashlib.sha256(blob).hexdigest()
            if digest == _last_hash and not force:
                return False
            # keep an hourly copy of the previous snapshot as a safety net
            if time.time() - _last_prev_rotation > 3600:
                old = client.get(KEY)
                if old:
                    client.set(KEY + ":prev", old)
                _last_prev_rotation = time.time()
            client.set(KEY, blob)
            _last_hash = digest
            return True
        except Exception as exc:
            _log(f"snapshot failed ({exc.__class__.__name__}: {exc})")
            return False


def start() -> None:
    """Start the periodic snapshot thread (call after restore())."""
    if _base is None or _client() is None:
        return
    try:
        interval = max(10, int(os.environ.get("LUNEL_KV_INTERVAL", "30")))
    except ValueError:
        interval = 30

    def loop() -> None:
        time.sleep(10)
        while True:
            snapshot()
            time.sleep(interval)

    threading.Thread(target=loop, name="lunel-kv-snapshot", daemon=True).start()
    atexit.register(snapshot)
    _log(f"snapshot thread running (every {interval}s, key '{KEY}')")
