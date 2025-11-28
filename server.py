import socket
import os

def recv_text(conn):
    """Receive a line of text ending with newline using recv()."""
    data = conn.recv(1024).decode()
    return data.strip()

def recv_file(conn, filename):
    """Receive a file using recv() until client stops sending."""
    with open(filename, "wb") as f:
        while True:
            chunk = conn.recv(1024)
            if not chunk:
                break
            f.write(chunk)

def send_file(conn, filename):
    """Send a file using send()."""
    with open(filename, "rb") as f:
        chunk = f.read(1024)
        while chunk:
            conn.send(chunk)
            chunk = f.read(1024)

if __name__ == '__main__':
    hostname = input("Enter server hostname/IP: ")
    port = int(input("Enter server port: "))

    host = socket.gethostbyname(hostname)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(5)

    print("Server listening on", host, "port", port)

    conn, addr = sock.accept()
    print("Client connected:", addr)

    while True:
        command = recv_text(conn)
        if not command:
            break

        print("Client command:", command)

       
        if command == "addfile":
            filename = recv_text(conn)
            print("Creating new file:", filename)

            server_filename = "server_" + filename
            with open(server_filename, "wb") as f:
                pass  

            conn.send(b"file recv\n")
            print("Sent: file recv")
            print("Receiving file data for:", filename)
            data = conn.recv(4096)

            with open(server_filename, "ab") as f:
                f.write(data)

            conn.send(b"file data recv\n")
            print("Sent: file data recv")

        elif command == "UPLOAD":
            filename = recv_text(conn)
            print("Receiving file:", filename)
            recv_file(conn, "server_" + filename)
            print("Upload complete.")
            conn.send(b"OK\n")

        elif command == "DOWNLOAD":
            filename = recv_text(conn)
            print("Client requests file:", filename)

            path = "server_" + filename
            if os.path.exists(path):
                conn.send(b"FOUND\n")
                send_file(conn, path)
            else:
                conn.send(b"NOTFOUND\n")

        elif command == "MESSAGE":
            msg = recv_text(conn)
            print("Client says:", msg)
            response = "Server received: " + msg + "\n"
            conn.send(response.encode())

        else:
            conn.send(b"INVALID\n")

    conn.close()
    sock.close()
