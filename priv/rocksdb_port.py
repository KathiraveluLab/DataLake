"""External RocksDB persistence worker communicating over an Erlang Port
using 4-byte big-endian length-prefixed JSON frames on binary stdio.
"""

from __future__ import annotations

import base64
import json
import os
import struct
import sys
from typing import Any

import rocksdict


def read_exact(stream: Any, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = stream.read(size - len(buf))
        if not chunk:
            return b""
        buf.extend(chunk)
    return bytes(buf)


def read_packet(stream: Any) -> dict[str, Any] | None:
    header = read_exact(stream, 4)
    if not header:
        return None
    (length,) = struct.unpack(">I", header)
    payload = read_exact(stream, length)
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def write_packet(stream: Any, message: dict[str, Any]) -> None:
    encoded = json.dumps(message, separators=(",", ":")).encode("utf-8")
    header = struct.pack(">I", len(encoded))
    stream.write(header + encoded)
    stream.flush()


def collect_storage_info(db_path: str) -> dict[str, Any]:
    entries = sorted(os.listdir(db_path)) if os.path.isdir(db_path) else []
    files: list[str] = []
    sst_files: list[str] = []
    wal_files: list[str] = []
    manifest_files: list[str] = []
    total_bytes = 0

    for name in entries:
        full_path = os.path.join(db_path, name)
        if os.path.isfile(full_path):
            files.append(name)
            total_bytes += os.path.getsize(full_path)
            if name.endswith(".sst"):
                sst_files.append(name)
            elif name.endswith(".log"):
                wal_files.append(name)
            elif name.startswith("MANIFEST-"):
                manifest_files.append(name)

    return {
        "db_path": db_path,
        "files": files,
        "sst_files": sst_files,
        "wal_files": wal_files,
        "manifest_files": manifest_files,
        "total_disk_bytes": total_bytes,
    }


def handle_request(db: rocksdict.Rdict, db_path: str, req: dict[str, Any]) -> dict[str, Any]:
    op = req.get("op")

    if op == "put":
        key = base64.b64decode(req["key"])
        val = base64.b64decode(req["val"])
        db[key] = val
        return {"ok": True}

    if op == "put_batch":
        wb = rocksdict.WriteBatch(raw_mode=True)
        for item in req["items"]:
            wb.put(base64.b64decode(item[0]), base64.b64decode(item[1]))
        db.write(wb)
        return {"ok": True}

    if op == "get":
        key = base64.b64decode(req["key"])
        try:
            val = db[key]
            return {"ok": True, "val": base64.b64encode(val).decode("ascii")}
        except KeyError:
            return {"ok": True, "val": None}

    if op == "delete_batch":
        wb = rocksdict.WriteBatch(raw_mode=True)
        for k_b64 in req["keys"]:
            wb.delete(base64.b64decode(k_b64))
        db.write(wb)
        return {"ok": True}

    if op == "prefix_scan":
        prefix = base64.b64decode(req["prefix"])
        limit = req.get("limit")
        items: list[list[str]] = []
        it = db.iter()
        try:
            it.seek(prefix)
            while it.valid():
                k = it.key()
                if not k.startswith(prefix):
                    break
                v = it.value()
                items.append(
                    [
                        base64.b64encode(k).decode("ascii"),
                        base64.b64encode(v).decode("ascii"),
                    ]
                )
                if limit is not None and len(items) >= limit:
                    break
                it.next()
        finally:
            del it
        return {"ok": True, "items": items}

    if op == "flush":
        db.flush(wait=True)
        return {"ok": True}

    if op == "compact":
        db.compact_range(None, None)
        return {"ok": True}

    if op == "info":
        return {"ok": True, "info": collect_storage_info(db_path)}

    if op == "clear_all":
        keys_to_delete: list[bytes] = []
        it = db.iter()
        try:
            it.seek_to_first()
            while it.valid():
                keys_to_delete.append(it.key())
                it.next()
        finally:
            del it

        if keys_to_delete:
            wb = rocksdict.WriteBatch(raw_mode=True)
            for k in keys_to_delete:
                wb.delete(k)
            db.write(wb)
        db.flush(wait=True)
        return {"ok": True}

    return {"ok": False, "error": f"unknown operation: {op}"}


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: rocksdb_port.py <db_path>\n")
        return 2

    db_path = sys.argv[1]
    os.makedirs(db_path, exist_ok=True)

    options = rocksdict.Options(raw_mode=True)
    options.create_if_missing(True)
    db = rocksdict.Rdict(db_path, options=options)

    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    try:
        while True:
            req = read_packet(stdin)
            if req is None:
                break
            try:
                resp = handle_request(db, db_path, req)
            except Exception as exc:  # noqa: BLE001
                resp = {"ok": False, "error": str(exc)}
            write_packet(stdout, resp)
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
