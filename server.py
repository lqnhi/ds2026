import socket
import os

def recv_text(conn):
    """Receive a line of text ending with newline using recv()."""
    data = conn.recv(1024).decode()   # recv(): read data
    return data.strip()

def recv_file(conn, filename):
    """Receive a file using recv() until client stops sending."""
    with open(filename, "wb") as f:
        while True:
            chunk = conn.recv(1024)   # recv(): read data
            if not chunk:
                break
            f.write(chunk)

def send_file(conn, filename):
    """Send a file using send()."""
    with open(filename, "rb") as f:
        chunk = f.read(1024)
        while chunk:
            conn.send(chunk)          # send(): send data
            chunk = f.read(1024)

if __name__ == '__main__':
    hostname = input("Enter server hostname/IP: ")
    port = int(input("Enter server port: "))

    host = socket.gethostbyname(hostname)

    # socket(): Create a TCP socket
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

        # File Upload
        if command == "UPLOAD":
            filename = recv_text(conn)
            print("Receiving file:", filename)
            recv_file(conn, "server_" + filename)
            print("Upload complete.")
            conn.send(b"OK\n")       # send(): write response

        # File Download
        elif command == "DOWNLOAD":
            filename = recv_text(conn)
            print("Client requests file:", filename)

            path = "server_" + filename
            if os.path.exists(path):
                conn.send(b"FOUND\n")
                send_file(conn, path)
            else:
                conn.send(b"NOTFOUND\n")

        # Message
        elif command == "MESSAGE":
            msg = recv_text(conn)
            print("Client says:", msg)
            response = "Server received: " + msg + "\n"
            conn.send(response.encode())   # send(): write text

        # Invalid Command
        else:
            conn.send(b"INVALID\n")

    conn.close()
    sock.close()
