#!/usr/bin/env python3
# DataNode – Mini HDFS (FIXED FOR LARGE FILES)
# Handles large chunk uploads properly

import socket
import threading
import os
import json
import time
import hashlib
import logging
from logging.handlers import RotatingFileHandler

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
    print(f"[{DATANODE_NAME}] {msg}")
    logging.info(msg)

def sha256_file(path: str) -> str:
    """Compute file SHA-256"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def recv_all(conn: socket.socket, timeout=60) -> bytes:
    """Reliable receive for full socket data - handles large payloads"""
    conn.settimeout(timeout)
    chunks = []
    total = 0
    
    while True:
        try:
            part = conn.recv(65536)
            if not part:
                break
            chunks.append(part)
            total += len(part)
            
            # Log progress for large transfers
            if total % (1024 * 1024) == 0:  # Every 1MB
                debug(f"📥 Received {total // (1024*1024)} MB so far...")
                
        except socket.timeout:
            debug(f"⏱ Socket timeout after receiving {total} bytes")
            break
        except Exception as e:
            debug(f"⚠ recv_all error: {e}")
            break
    
    result = b"".join(chunks)
    debug(f"📥 Total received: {len(result)} bytes")
    return result

# ---------- HEARTBEATS ----------
def send_heartbeat():
    """Send UDP heartbeat to NameNode every 5s"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:
            sock.sendto(DATANODE_NAME.encode(), (NAMENODE_HOST, NAMENODE_HEARTBEAT_PORT))
            debug("💓 Heartbeat sent")
        except Exception as e:
            debug(f"⚠ Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL_SEC)

# ---------- RPC HANDLERS ----------
def handle_store(req: dict, conn: socket.socket):
    """Store a chunk to disk"""
    try:
        fname = req["filename"]
        content_str = req["content"]
        
        debug(f"📦 Storing {fname} (content size: {len(content_str)} chars)")
        
        blob = content_str.encode("latin-1", errors="ignore")
        path = os.path.join(DATA_DIR, fname)

        # Write to disk
        with open(path, "wb") as f:
            f.write(blob)

        # Verify checksum
        calc = sha256_file(path)
        debug(f"✅ Stored {fname} ({len(blob)} bytes) checksum={calc[:10]}...")
        
        response = json.dumps({"status": "stored", "checksum": calc}).encode()
        conn.sendall(response)
        
    except Exception as e:
        debug(f"❌ Store error: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass

def handle_read(req: dict, conn: socket.socket):
    """Read a chunk from disk"""
    try:
        fname = req["filename"]
        path = os.path.join(DATA_DIR, fname)
        
        if not os.path.exists(path):
            conn.sendall(json.dumps({"status": "error", "msg": "not found"}).encode())
            debug(f"❌ Chunk not found: {fname}")
            return

        with open(path, "rb") as f:
            blob = f.read()

        content_str = blob.decode("latin-1", errors="ignore")
        calc = sha256_file(path)
        
        response = json.dumps({"status": "ok", "content": content_str, "checksum": calc}).encode()
        conn.sendall(response)
        
        debug(f"📤 Served {fname} ({len(blob)} bytes) checksum={calc[:10]}...")
        
    except Exception as e:
        debug(f"❌ Read error: {e}")
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass

def handle_conn(conn: socket.socket, addr):
    """Main TCP handler for each client or NameNode request"""
    try:
        # Increase buffer sizes for large chunks
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4_000_000)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4_000_000)
        
        debug(f"📞 Connection from {addr}")
        
        # Receive request with generous timeout
        data = recv_all(conn, timeout=60)
        
        if not data:
            debug(f"⚠ Empty request from {addr}")
            return
            
        # Parse JSON request
        try:
            req = json.loads(data.decode(errors="ignore"))
        except json.JSONDecodeError as e:
            debug(f"❌ JSON decode error: {e}")
            return
            
        action = req.get("action")
        debug(f"📩 Action: {action}")

        if action == "store":
            handle_store(req, conn)
        elif action == "read":
            handle_read(req, conn)
        else:
            conn.sendall(json.dumps({"status": "error", "msg": "unknown action"}).encode())
            debug(f"⚠ Unknown action: {action}")
            
    except Exception as e:
        debug(f"❌ Connection handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass
    finally:
        try:
            conn.close()
        except:
            pass
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

if _name_ == "_main_":
    main()