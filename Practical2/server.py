import xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import Binary
from pathlib import Path
import os
import threading
import base64

# Directory to store server files
SERVER_DIR = "server_files"
os.makedirs(SERVER_DIR, exist_ok=True)

# --------------------------
#  SERVER FUNCTIONS
# --------------------------

def list_files():
    return os.listdir(SERVER_DIR)

def upload_file(filename, file_content):
    """Single-shot upload"""
    path = Path(SERVER_DIR) / filename
    with open(path, 'wb') as f:
        f.write(file_content.data)
    print(f"[Server] Uploaded file: {filename}")
    return True

def download_file(filename):
    """Single-shot download"""
    path = Path(SERVER_DIR) / filename
    if not path.exists():
        print(f"[Server] File {filename} not found")
        return Binary(b"")
    with open(path, 'rb') as f:
        data = f.read()
    print(f"[Server] Downloaded file: {filename}")
    return Binary(data)

def add_file(filename, content_str):
    """Add text file on server"""
    path = Path(SERVER_DIR) / filename
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content_str)
    print(f"[Server] Created file: {filename}")
    return True

def send_message(msg):
    """Receive message"""
    print(f"[Server] Client says: {msg}")
    return f"Server received: {msg}"

# --------------------------
#  XML-RPC SERVER SETUP
# --------------------------

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

server = SimpleXMLRPCServer(("localhost", 8000),
                            requestHandler=RequestHandler,
                            allow_none=True)
server.register_function(list_files, 'list_files')
server.register_function(upload_file, 'upload_file')
server.register_function(download_file, 'download_file')
server.register_function(add_file, 'add_file')
server.register_function(send_message, 'send_message')

print("[Server] XML-RPC Server running on port 8000...")
server.serve_forever()
