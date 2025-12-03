import xmlrpc.client
from xmlrpc.client import Binary
from pathlib import Path

SERVER_URL = "http://localhost:8000"
rpc = xmlrpc.client.ServerProxy(SERVER_URL, allow_none=True)

def upload_file_client():
    filename = input("Enter file to upload: ")
    path = Path(filename)
    if not path.exists():
        print("File not found")
        return
    with open(path, 'rb') as f:
        data = f.read()
    rpc.upload_file(filename, Binary(data))
    print("Upload complete")

def download_file_client():
    filename = input("Enter file to download: ")
    data = rpc.download_file(filename)
    if not data.data:
        print("File not found on server")
        return
    outname = "downloaded_" + filename
    with open(outname, 'wb') as f:
        f.write(data.data)
    print("Downloaded:", outname)

def add_file_on_server_client():
    filename = input("Enter new file name on server: ")
    content = input("Enter file content: ")
    rpc.add_file(filename, content)
    print("File created on server")

def send_message_client():
    msg = input("Enter message to server: ")
    response = rpc.send_message(msg)
    print("Server:", response)

# --------------------------
# MENU LOOP
# --------------------------
while True:
    print("\n=== XML-RPC File Transfer Client ===")
    print("1. Upload file")
    print("2. Download file")
    print("3. Add file on server")
    print("4. Send message")
    print("5. Exit")

    choice = input("Choose option: ")
    if choice == "1": upload_file_client()
    elif choice == "2": download_file_client()
    elif choice == "3": add_file_on_server_client()
    elif choice == "4": send_message_client()
    elif choice == "5": break
    else: print("Invalid option!")
