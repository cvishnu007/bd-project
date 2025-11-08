#!/usr/bin/env python3
# NameNode – Mini HDFS (FIXED VERSION)
# All functionality working: Upload, Download, Status, Heartbeats, Replication

import socket
import threading
import json
import os
import time
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Tuple, List

# ---------- CONFIG ----------
NAMENODE_HOST = "172.22.194.120"
NAMENODE_PORT = 5000

DATANODES: Dict[str, Tuple[str, int]] = {
    "datanode1": ("172.22.192.208", 5001),
    "datanode2": ("172.22.194.94", 5002),
}

HEARTBEAT_UDP_PORT = 6000
HEARTBEAT_TIMEOUT_SEC = 15
HEARTBEAT_CHECK_EVERY = 5

CHUNK_SIZE = 2 * 1024 * 1024  # 2MB
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50MB
METADATA_FILE = "metadata.json"

metadata = {"files": {}, "datanode_status": {}}
lock = threading.Lock()

# ---------- LOGGING ----------
def setup_logs():
    os.makedirs("logs", exist_ok=True)
    handler = RotatingFileHandler("logs/namenode.log", maxBytes=1_000_000, backupCount=3)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        format="%(asctime)s %(levelname)s %(message)s"
    )

def log_console(msg: str):
    print(f"[NameNode] {msg}")
    logging.info(msg)

# ---------- UTILS ----------
def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def save_metadata():
    with lock:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)

def load_metadata():
    global metadata
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            metadata = json.load(f)
    else:
        save_metadata()

# ---------- SOCKET HELPERS ----------
def recv_all(conn: socket.socket, timeout=30) -> bytes:
    """Receive all data from socket until EOF or timeout."""
    conn.settimeout(timeout)
    chunks = []
    while True:
        try:
            part = conn.recv(65536)
            if not part:
                break
            chunks.append(part)
        except socket.timeout:
            break  # ✅ FIXED: Exit on timeout instead of continue
        except Exception as e:
            log_console(f"recv_all exception: {e}")
            break
    data = b"".join(chunks)
    log_console(f"📥 Received {len(data)} bytes total")
    return data

def alive_nodes() -> List[str]:
    with lock:
        return list(metadata["datanode_status"].keys())

# ---------- HEARTBEATS ----------
def heartbeat_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", HEARTBEAT_UDP_PORT))
    log_console(f"💓 Heartbeat listener active on 0.0.0.0:{HEARTBEAT_UDP_PORT}")
    while True:
        try:
            data, _ = sock.recvfrom(512)
            node_name = data.decode(errors="ignore").strip()
            log_console(f"✅ Heartbeat from {node_name}")
            with lock:
                metadata["datanode_status"][node_name] = time.time()
            save_metadata()
        except Exception as e:
            log_console(f"⚠ Heartbeat error: {e}")

def monitor_nodes():
    while True:
        time.sleep(HEARTBEAT_CHECK_EVERY)
        now = time.time()
        changed = False
        with lock:
            for node, last in list(metadata["datanode_status"].items()):
                if now - last > HEARTBEAT_TIMEOUT_SEC:
                    log_console(f"⚠ {node} is DOWN (no heartbeat)")
                    del metadata["datanode_status"][node]
                    changed = True
        if changed:
            save_metadata()

# ---------- DATANODE RPC ----------
def store_chunk(node: str, chunk_name: str, content_str: str) -> bool:
    """Store a chunk on a DataNode."""
    if node not in alive_nodes():
        log_console(f"⚠ Skipping {node} (not alive)")
        return False

    ip, port = DATANODES[node]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30)  # ✅ Increased for large chunks
        s.connect((ip, port))
        payload = {"action": "store", "filename": chunk_name, "content": content_str}
        s.sendall(json.dumps(payload).encode())
        
        # Shutdown write side to signal end of request
        try:
            s.shutdown(socket.SHUT_WR)
        except:
            pass
        
        data = recv_all(s, timeout=30)  # ✅ Increased timeout
        s.close()
        
        if data:
            res = json.loads(data.decode())
            if res.get("status") == "stored":
                log_console(f"✅ Stored {chunk_name} on {node}")
                return True
        
        log_console(f"⚠ Store failed on {node}")
        return False
    except Exception as e:
        log_console(f"❌ Store on {node} failed: {e}")
        return False

def read_chunk(node: str, chunk_name: str) -> dict:
    """Read a chunk from a DataNode."""
    ip, port = DATANODES[node]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((ip, port))
        payload = {"action": "read", "filename": chunk_name}
        s.sendall(json.dumps(payload).encode())
        data = recv_all(s, timeout=10)
        s.close()
        
        if data:
            return json.loads(data.decode())
        return {}
    except Exception as e:
        log_console(f"❌ Read from {node} failed: {e}")
        return {}

# ---------- CLIENT HANDLERS ----------
def handle_upload(filename: str, content_str: str) -> dict:
    """Handle file upload with chunking and replication."""
    try:
        # Convert from latin-1 string to bytes
        raw = content_str.encode("latin-1", errors="ignore")
        
        if len(raw) > MAX_UPLOAD_BYTES:
            return {"status": "error", "msg": "File too large (> 50 MB)"}

        alive = alive_nodes()
        if not alive:
            return {"status": "error", "msg": "No DataNodes alive"}

        # Split into chunks
        parts = [raw[i:i + CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
        if not parts:
            parts = [b""]  # Empty file

        chunks_meta = []

        for i, part in enumerate(parts):
            chunk_name = f"{filename}_part{i}"
            payload = part.decode("latin-1", errors="ignore")
            chunk_hash = sha256_bytes(part)

            # Replicate to all available nodes
            replicas = []
            for node in DATANODES.keys():
                if store_chunk(node, chunk_name, payload):
                    replicas.append(node)

            if not replicas:
                return {"status": "error", "msg": f"Failed to store {chunk_name}"}

            chunks_meta.append({
                "chunk_name": chunk_name,
                "replicas": replicas,
                "checksum": chunk_hash
            })

        # Save metadata
        with lock:
            metadata["files"][filename] = {
                "chunks": chunks_meta,
                "file_checksum": sha256_bytes(raw)
            }
        save_metadata()
        
        log_console(f"✅ Uploaded {filename} in {len(chunks_meta)} chunks")
        return {"status": "uploaded", "chunks": len(chunks_meta)}

    except Exception as e:
        log_console(f"❌ Upload error: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}

def handle_download(filename: str) -> dict:
    """Return chunk manifest for download."""
    with lock:
        entry = metadata["files"].get(filename)
        if not entry:
            return {"status": "error", "msg": "File not found"}
        return {"status": "ok", "chunks": entry["chunks"]}

def handle_status() -> dict:
    """Return cluster status."""
    with lock:
        now = time.time()
        nodes = {}
        
        for n in DATANODES:
            ts = metadata["datanode_status"].get(n)
            nodes[n] = {
                "state": "alive" if ts else "down",
                "last_heartbeat_age_sec": (None if ts is None else round(now - ts, 1))
            }
        
        # Check for under-replicated chunks
        under = []
        for fname, entry in metadata["files"].items():
            for ch in entry["chunks"]:
                alive_replicas = [n for n in ch["replicas"] if n in metadata["datanode_status"]]
                if len(alive_replicas) < len(ch["replicas"]):
                    under.append({
                        "file": fname,
                        "chunk": ch["chunk_name"],
                        "have": alive_replicas,
                        "want": ch["replicas"]
                    })
        
        res = {
            "status": "ok",
            "nodes": nodes,
            "files": metadata["files"],
            "under_replicated": under
        }
        
        log_console(f"✅ Status: {len(metadata['files'])} files, {len(nodes)} nodes")
        return res

# ---------- CLIENT CONNECTION HANDLER ----------
def client_handler(conn: socket.socket, addr):
    try:
        raw = recv_all(conn, timeout=60)
        
        if not raw:
            log_console(f"⚠ Empty request from {addr}")
            return

        try:
            req = json.loads(raw.decode())
        except json.JSONDecodeError as e:
            log_console(f"❌ JSON decode error from {addr}: {e}")
            return

        action = req.get("action")
        log_console(f"📩 Request: {action} from {addr}")

        # Route to handlers
        if action == "upload":
            res = handle_upload(req["filename"], req["content"])
        elif action == "download":
            res = handle_download(req["filename"])
        elif action == "status":
            res = handle_status()
        else:
            res = {"status": "error", "msg": "Invalid action"}

        # Send response
        response_data = json.dumps(res).encode()
        conn.sendall(response_data)
        
        log_console(f"📤 Sent {action} response ({len(response_data)} bytes) to {addr}")

    except Exception as e:
        log_console(f"❌ Handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except:
            pass
        conn.close()
        log_console(f"🔒 Connection closed with {addr}")

# ---------- MAIN ----------
def start_namenode():
    setup_logs()
    load_metadata()
    
    # Start background threads
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=monitor_nodes, daemon=True).start()

    log_console(f"🧠 NameNode listening on 0.0.0.0:{NAMENODE_PORT}")
    
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", NAMENODE_PORT))
    srv.listen(32)
    
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=client_handler, args=(conn, addr), daemon=True).start()

if _name_ == "_main_":
    start_namenode()