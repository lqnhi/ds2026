import socket
import os

def send_text(sock, text):
    # send(): Send data over the connection
    sock.send((text + "\n").encode())

def send_file(sock, filename):
    with open(filename, "rb") as f:
        chunk = f.read(1024)
        while chunk:
            # send(): Send file chunks
            sock.send(chunk)
            chunk = f.read(1024)

def recv_file(sock, filename):
    with open(filename, "wb") as f:
        while True:
            # recv(): Read data from the connection
            chunk = sock.recv(1024)
            if not chunk:
                break
            f.write(chunk)

if __name__ == '__main__':

    hostname = input("Enter server hostname/IP: ")
    port = int(input("Enter server port: "))

    host = socket.gethostbyname(hostname)


    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    sock.connect((host, port))

    while True:
        print("\n=== MENU ===")
        print("1. Upload file")
        print("2. Download file")
        print("3. Send message")
        print("4. Exit")

        choice = input("Choose option: ")

        # 1. FILE UPLOAD
        if choice == "1":
            filename = input("Enter filename: ")

            if not os.path.exists(filename):
                print("File does not exist!")
                continue

            send_text(sock, "UPLOAD")
            send_text(sock, filename)
            send_file(sock, filename)

            # Shutdown write-end so server knows file is done
            sock.shutdown(socket.SHUT_WR)

            response = sock.recv(1024).decode().strip()
            print("Server:", response)

            break  # reconnect after upload

        # 2. FILE DOWNLOAD
        elif choice == "2":
            filename = input("Enter filename to download: ")

            send_text(sock, "DOWNLOAD")
            send_text(sock, filename)

            status = sock.recv(1024).decode().strip()

            if status == "FOUND":
                recv_file(sock, "downloaded_" + filename)
                print("File downloaded successfully!")
            else:
                print("Server: File not found.")

        # 3. SEND MESSAGE
        elif choice == "3":
            msg = input("Enter message: ")
            send_text(sock, "MESSAGE")
            send_text(sock, msg)

            response = sock.recv(1024).decode().strip()
            print("Server says:", response)

        # EXIT
        elif choice == "4":
            break

        else:
            print("Invalid option!")

    sock.close()
