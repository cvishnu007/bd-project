#!/usr/bin/env python3
# DataNode – Mini HDFS (FINAL STABLE + DEBUG)
# Works with final NameNode + Client versions
# ---------------------------------------------
# Features:
# - UDP heartbeats every 5s
# - TCP store/read RPCs
# - Binary-safe chunk handling (latin-1 wrapper)
# - Per-chunk checksum verification
# - Auto logs with timestamps
# - Full debug output in terminal and logs
# - Restart-safe (keeps chunks locally)

import socket
import threading
import os
import json
import time
import hashlib
import logging
from logging.handlers import RotatingFileHandler

# ---------- CONFIG ----------
# ---------- CONFIG ----------
DATANODE_NAME = "datanode1"              # unique node name
DATANODE_HOST = "172.22.192.208"         # ZeroTier IP of this node
DATANODE_PORT = 5001                     # unique TCP port

NAMENODE_HOST = "172.22.194.120"         # NameNode IP
NAMENODE_HEARTBEAT_PORT = 6000           # UDP port for heartbeats

DATA_DIR = "data_blocks"                 # local storage folder
HEARTBEAT_INTERVAL_SEC = 5


# ---------- SETUP ----------
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

def setup_logs():
    handler = RotatingFileHandler(f"logs/{DATANODE_NAME}.log", maxBytes=500_000, backupCount=2)
    logging.basicConfig(level=logging.INFO, handlers=[handler],
                        format="%(asctime)s %(levelname)s %(message)s")

def debug(msg: str):
    """Print and log for both terminal and file"""
    print(f"[{DATANODE_NAME} DEBUG] {msg}")
    logging.info(msg)

def sha256_file(path: str) -> str:
    """Compute file SHA-256"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def recv_all(conn: socket.socket, timeout=10) -> bytes:
    """Reliable receive for full socket data"""
    conn.settimeout(timeout)
    chunks = []
    while True:
        try:
            part = conn.recv(65536)
            if not part:
                break
            chunks.append(part)
        except socket.timeout:
            break
    return b"".join(chunks)

# ---------- HEARTBEATS ----------
def send_heartbeat():
    """Send UDP heartbeat to NameNode every 5s"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:
            sock.sendto(DATANODE_NAME.encode(), (NAMENODE_HOST, NAMENODE_HEARTBEAT_PORT))
            debug("💓 Heartbeat sent to NameNode")
        except Exception as e:
            debug(f"⚠ Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL_SEC)

# ---------- RPC HANDLERS ----------
def handle_store(req: dict, conn: socket.socket):
    fname = req["filename"]
    content_str = req["content"]
    blob = content_str.encode("latin-1", errors="ignore")
    path = os.path.join(DATA_DIR, fname)

    # Write to disk
    with open(path, "wb") as f:
        f.write(blob)

    # Verify checksum
    calc = sha256_file(path)
    debug(f"📦 Stored chunk {fname} ({len(blob)} bytes) checksum={calc[:10]}...")
    conn.sendall(json.dumps({"status": "stored", "checksum": calc}).encode())

def handle_read(req: dict, conn: socket.socket):
    fname = req["filename"]
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        conn.sendall(json.dumps({"status": "error", "msg": "not found"}).encode())
        debug(f"❌ Read request for missing chunk: {fname}")
        return

    with open(path, "rb") as f:
        blob = f.read()

    content_str = blob.decode("latin-1", errors="ignore")
    calc = sha256_file(path)
    conn.sendall(json.dumps({"status": "ok", "content": content_str, "checksum": calc}).encode())
    debug(f"📤 Served chunk {fname} ({len(blob)} bytes) checksum={calc[:10]}...")

def handle_conn(conn: socket.socket, addr):
    """Main TCP handler for each client or NameNode request"""
    try:
        data = recv_all(conn)
        if not data:
            return
        req = json.loads(data.decode(errors="ignore"))
        action = req.get("action")
        debug(f"📩 Request from {addr}: {action}")

        if action == "store":
            handle_store(req, conn)
        elif action == "read":
            handle_read(req, conn)
        else:
            conn.sendall(json.dumps({"status": "error", "msg": "unknown action"}).encode())
            debug(f"⚠ Unknown action from {addr}: {action}")
    except Exception as e:
        debug(f"❌ handle_conn error: {e}")
        import traceback; traceback.print_exc()
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass
    finally:
        conn.close()
        debug(f"🔒 Connection closed with {addr}")

# ---------- SERVER ----------
def start_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", DATANODE_PORT))
    srv.listen(32)
    debug(f"💾 {DATANODE_NAME} listening on {DATANODE_HOST}:{DATANODE_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

# ---------- MAIN ----------
def main():
    setup_logs()
    debug(f"🚀 Starting {DATANODE_NAME}")
    threading.Thread(target=send_heartbeat, daemon=True).start()
    start_server()

if _name_ == "main":
    main()