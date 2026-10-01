"""Lunel — unified service entrypoint (fork-and-go deployment).

One deployable unit contains the whole platform:

    python main.py        # or:  uvicorn main:app

  * Lunel Console API + frontend on ``$PORT`` (default 8080) — the public
    endpoint on the platform's domain (WebSocket capable)
  * an embedded Lunel Worker on an internal loopback port
  * Lunel Core instances as isolated child processes (process driver, OS
    resource limits) on the same node

The module exposes ``app`` (the Console ASGI application), so builders that
detect ``uvicorn main:app`` (railpack et al.) work with zero configuration.

Zero-config defaults:
  * ``DATABASE_URL`` (provided by managed platforms) is
    used when ``LUNEL_DATABASE_URL`` is not set
  * ``LUNEL_SECRET_KEY`` auto-generates and persists to a 0600 file on
    first boot
  * the internal worker token auto-generates per boot (console and worker
    share one process tree; set ``LUNEL_WORKER_TOKEN`` explicitly when
    running split deployments)

Explicitly required for login: ``LUNEL_GITHUB_CLIENT_ID`` and
``LUNEL_GITHUB_CLIENT_SECRET`` (plus ``LUNEL_PUBLIC_URL`` matching the
platform domain so the OAuth callback resolves).
"""
from __future__ import annotations

    """Return a usable session secret, persisting a generated one if needed."""
    val = os.environ.get("LUNEL_SECRET_KEY", "").strip()
    if len(val) >= 32:
        return val
    base = _writable_dir([Path("/data"), ROOT / ".lunel-data"])
    if base is not None:
        key_file = base / ".lunel_secret_key"
        try:
            if key_file.exists():
                val = key_file.read_text(encoding="utf-8").strip()
            if len(val) < 32:
                val = secrets.token_urlsafe(48)
                key_file.write_text(val, encoding="utf-8")
            os.chmod(key_file, 0o600)
            print(f"[lunel] LUNEL_SECRET_KEY not set — persisted generated key to {key_file}",
                  file=sys.stderr)
            return val
        except OSError:
            pass
    print("[lunel] WARNING: LUNEL_SECRET_KEY not set and not persistable — "
          "using an ephemeral key (sessions reset on restart)", file=sys.stderr)
    return secrets.token_urlsafe(48)


def pick_free_port(start: int, end: int | None = None) -> int:
    end = end if end is not None else start + 99
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
