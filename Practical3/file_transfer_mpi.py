from mpi4py import MPI
import os
import sys
import hashlib
import threading
import time

CHUNK_SIZE = 65536
METADATA_TAG, DATA_TAG, CONTROL_TAG, MESSAGE_TAG = 0, 1, 2, 3
TERMINATE, TRANSFER, COMPLETE, WORKER_SEND, BROADCAST = -1, 1, 2, 3, 4

class FileTransfer:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.running = True
        self.is_master = (self.rank == 0)
        self.messages = []
        
        # Start message listener thread for workers
        if not self.is_master:
            self.msg_thread = threading.Thread(target=self.worker_message_listener, daemon=True)
            self.msg_thread.start()
    
    def checksum(self, filepath):
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()
    
    def get_file_info(self, filepath):
        if not os.path.exists(filepath):
            return None
        size = os.path.getsize(filepath)
        chunks = size // CHUNK_SIZE
        last = size % CHUNK_SIZE
        if last > 0:
            chunks += 1
        else:
            last = CHUNK_SIZE
        return {
            'name': os.path.basename(filepath),
            'size': size,
            'checksum': self.checksum(filepath),
            'chunks': chunks,
            'last': last
        }
        
    def master_interface(self):
        print(f"\n{'='*60}")
        print(f"MPI FILE TRANSFER - MASTER (Rank {self.rank})")
        print(f"{'='*60}")
        print(f"Workers: {list(range(1, self.size))}")
        print(f"{'-'*60}")
        
        while self.running:
            try:
                # Check for incoming messages from workers
                self.master_check_messages()
                
                print(f"\nCommands:")
                print("  send <file> <worker>        - Send file to worker")
                print("  get <worker> <file>         - Request file from worker")
                print("  w2w <src> <dst> <file>      - Worker to worker file transfer")
                print("  broadcast <message>         - Broadcast message to all workers")
                print("  msg <worker> <message>      - Send message to specific worker")
                print("  list                        - List local files")
                print("  status                      - Show system status")
                print("  workers                     - Show worker status")
                print("  quit                        - Shutdown system")
                
                cmd = input(f"\nmaster> ").strip().split()
                
                if not cmd:
                    continue
                
                action = cmd[0].lower()
                
                if action == "send" and len(cmd) == 3:
                    self.master_send(cmd[1], int(cmd[2]))
                
                elif action == "get" and len(cmd) == 3:
                    self.master_request(int(cmd[1]), cmd[2])
                
                elif action == "w2w" and len(cmd) == 4:
                    self.master_initiate_worker_transfer(int(cmd[1]), int(cmd[2]), cmd[3])
                
                elif action == "broadcast" and len(cmd) > 1:
                    message = " ".join(cmd[1:])
                    self.master_broadcast(message)
                
                elif action == "msg" and len(cmd) > 2:
                    try:
                        worker = int(cmd[1])
                        message = " ".join(cmd[2:])
                        self.master_send_message(worker, message)
                    except:
                        print("Error: Invalid worker number")
                
                elif action == "list":
                    print(f"\n[Master] Files:")
                    for f in os.listdir('.'):
                        if os.path.isfile(f):
                            size = os.path.getsize(f)
                            print(f"  {f} ({size:,} bytes)")
                
                elif action == "status":
                    self.master_show_status()
                
                elif action == "workers":
                    self.master_show_workers()
                
                elif action == "quit":
                    self.master_shutdown()
                    break
                
                else:
                    print("Unknown command")
                    
            except KeyboardInterrupt:
                print(f"\n[Master] Shutting down...")
                self.running = False
            except Exception as e:
                print(f"[Master] Error: {e}")
    
    def master_check_messages(self):
        """Check for messages from workers"""
        for src in range(1, self.size):
            if self.comm.Iprobe(source=src, tag=MESSAGE_TAG):
                msg = self.comm.recv(source=src, tag=MESSAGE_TAG)
                print(f"\n[Master] Message from Worker {src}: {msg}")
    
    def master_send(self, filepath, worker_rank):
        """Master sends file to worker"""
        if not 1 <= worker_rank < self.size:
            print(f"Error: Invalid worker rank")
            return
        
        if not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found")
            return
        
        info = self.get_file_info(filepath)
        print(f"\n[Master] Sending '{info['name']}' to Worker {worker_rank}")
        
        self.comm.send(TRANSFER, dest=worker_rank, tag=CONTROL_TAG)
        self.comm.send({'from': 0, 'info': info}, dest=worker_rank, tag=METADATA_TAG)
        
        with open(filepath, 'rb') as f:
            for i in range(info['chunks']):
                chunk_size = info['last'] if i == info['chunks']-1 else CHUNK_SIZE
                self.comm.send(f.read(chunk_size), dest=worker_rank, tag=DATA_TAG)
                if (i+1) % 5 == 0:
                    print(f"  Progress: {i+1}/{info['chunks']} chunks")
        
        self.comm.send(COMPLETE, dest=worker_rank, tag=CONTROL_TAG)
        print(f"[Master] Transfer complete!")
    
    def master_request(self, worker_rank, filename):
        """Master requests file from worker"""
        print(f"\n[Master] Requesting '{filename}' from Worker {worker_rank}")
        self.comm.send(TRANSFER, dest=worker_rank, tag=CONTROL_TAG)
        self.comm.send({'request': True, 'filename': filename, 'to': 0}, dest=worker_rank, tag=METADATA_TAG)
    
    def master_initiate_worker_transfer(self, src_worker, dst_worker, filename):
        """Master initiates worker-to-worker file transfer"""
        if not (1 <= src_worker < self.size and 1 <= dst_worker < self.size):
            print("Error: Invalid worker ranks")
            return
        
        if src_worker == dst_worker:
            print("Error: Cannot send file to self")
            return
        
        print(f"\n[Master] Initiating transfer: Worker {src_worker} -> Worker {dst_worker}")
        print(f"File: {filename}")
        
        # Tell source worker to send file
        self.comm.send(WORKER_SEND, dest=src_worker, tag=CONTROL_TAG)
        self.comm.send({'to': dst_worker, 'filename': filename}, dest=src_worker, tag=METADATA_TAG)
    
    def master_broadcast(self, message):
        """Master broadcasts message to all workers"""
        print(f"\n[Master] Broadcasting: {message}")
        for i in range(1, self.size):
            self.comm.send(BROADCAST, dest=i, tag=CONTROL_TAG)
            self.comm.send(f"Master: {message}", dest=i, tag=MESSAGE_TAG)
        print(f"[Master] Broadcast sent to {self.size-1} workers")
    
    def master_send_message(self, worker_rank, message):
        """Master sends message to specific worker"""
        if not 1 <= worker_rank < self.size:
            print(f"Error: Invalid worker rank")
            return
        
        self.comm.send(BROADCAST, dest=worker_rank, tag=CONTROL_TAG)
        self.comm.send(f"Master (private): {message}", dest=worker_rank, tag=MESSAGE_TAG)
        print(f"[Master] Message sent to Worker {worker_rank}")
    
    def master_show_status(self):
        print(f"\n[Master] System Status:")
        print(f"  Total Processes: {self.size}")
        print(f"  Active Workers: {self.size - 1}")
        print(f"  Chunk Size: {CHUNK_SIZE:,} bytes")
        print(f"  Master Rank: {self.rank}")
    
    def master_show_workers(self):
        print(f"\n[Master] Worker Status:")
        print(f"  Worker  Tasks  Status")
        print(f"  ------  -----  ------")
        for i in range(1, self.size):
            # Check if worker log file exists and get status
            log_file = f"worker_{i}.log"
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        last_line = lines[-1].strip() if lines else "Unknown"
                        print(f"  {i:6}  -      {last_line}")
                except:
                    print(f"  {i:6}  -      Active")
            else:
                print(f"  {i:6}  -      No log file")
    
    def master_shutdown(self):
        """Shutdown all workers"""
        print(f"\n[Master] Shutting down workers...")
        for i in range(1, self.size):
            self.comm.send(TERMINATE, dest=i, tag=CONTROL_TAG)
        self.running = False
        print("[Master] System shutdown complete")
        
    def worker_message_listener(self):
        """Background thread to listen for messages from other workers"""
        while self.running:
            try:
                for src in range(1, self.size):
                    if src != self.rank and self.comm.Iprobe(source=src, tag=MESSAGE_TAG):
                        msg = self.comm.recv(source=src, tag=MESSAGE_TAG)
                        self.messages.append((src, msg))
                        self.worker_log(f"Message from Worker {src}: {msg}")
                time.sleep(0.1)
            except:
                pass
    
    def worker_log(self, message):
        """Log message to worker file"""
        log_file = f"worker_{self.rank}.log"
        with open(log_file, 'a') as f:
            f.write(f"{message}\n")
        print(f"[Worker {self.rank}] {message}")
    
    def worker_interface(self):
        """Enhanced worker interface with communication capabilities"""
        # Create initial log file
        log_file = f"worker_{self.rank}.log"
        with open(log_file, 'w') as f:
            f.write(f"Worker {self.rank} started\n")
        
        self.worker_log(f"Started - Type commands:")
        self.worker_log("  send <worker> <file>   - Send file to another worker")
        self.worker_log("  msg <worker> <message> - Send message to worker")
        self.worker_log("  messages               - Show recent messages")
        self.worker_log("  files                  - List local files")
        self.worker_log("  status                 - Show worker status")
        self.worker_log("  help                   - Show commands")
        self.worker_log("  exit                   - Exit (only this worker)")
        
        while self.running:
            try:
                # Check for incoming transfers/messages from master
                if self.comm.Iprobe(source=0, tag=CONTROL_TAG):
                    sig = self.comm.recv(source=0, tag=CONTROL_TAG)
                    
                    if sig == TERMINATE:
                        self.worker_log("Received shutdown signal from Master")
                        self.running = False
                        break
                    
                    elif sig == TRANSFER:
                        self.worker_receive_from_master()
                    
                    elif sig == WORKER_SEND:
                        self.worker_handle_master_initiated_send()
                    
                    elif sig == BROADCAST:
                        msg = self.comm.recv(source=0, tag=MESSAGE_TAG)
                        self.worker_log(f"Broadcast from Master: {msg}")
                
                # Check for incoming file transfers from other workers
                for src in range(1, self.size):
                    if src != self.rank and self.comm.Iprobe(source=src, tag=CONTROL_TAG):
                        sig = self.comm.recv(source=src, tag=CONTROL_TAG)
                        if sig == TRANSFER:
                            self.worker_receive_from_worker(src)
                
                # Check for user input (non-blocking)
                try:
                    import select
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        cmd = sys.stdin.readline().strip()
                        if cmd:
                            self.process_worker_command(cmd)
                except:
                    # Simple input fallback
                    pass
                
                time.sleep(0.1)
                    
            except KeyboardInterrupt:
                self.worker_log("Exiting...")
                self.running = False
            except Exception as e:
                self.worker_log(f"Error: {e}")
    
    def worker_receive_from_master(self):
        """Worker receives file from master"""
        try:
            data = self.comm.recv(source=0, tag=METADATA_TAG)
            
            if data.get('request'):
                filename = data['filename']
                if os.path.exists(filename):
                    self.worker_log(f"Master requested file: {filename}")
                    self.worker_send_to_master(filename)
                else:
                    self.worker_log(f"Error: File '{filename}' not found")
                return
            
            info = data['info']
            self.worker_log(f"Receiving '{info['name']}' from Master")
            
            filename = f"from_master_{info['name']}"
            total = 0
            
            with open(filename, 'wb') as f:
                while True:
                    if self.comm.Iprobe(source=0, tag=CONTROL_TAG):
                        sig = self.comm.recv(source=0, tag=CONTROL_TAG)
                        if sig == COMPLETE:
                            break
                    
                    if self.comm.Iprobe(source=0, tag=DATA_TAG):
                        chunk = self.comm.recv(source=0, tag=DATA_TAG)
                        f.write(chunk)
                        total += len(chunk)
                        progress = (total / info['size']) * 100
                        if int(progress) % 20 == 0:
                            self.worker_log(f"  Progress: {progress:.0f}%")
            
            # Verify
            if os.path.getsize(filename) == info['size']:
                if self.checksum(filename) == info['checksum']:
                    self.worker_log(f"Saved: {filename} ")
                else:
                    self.worker_log(f"Saved: {filename} (checksum mismatch)")
            else:
                self.worker_log("Size mismatch!")
                
        except Exception as e:
            self.worker_log(f"Receive error: {e}")
    
    def worker_receive_from_worker(self, src_rank):
        """Worker receives file from another worker"""
        try:
            data = self.comm.recv(source=src_rank, tag=METADATA_TAG)
            info = data['info']
            
            self.worker_log(f"Receiving '{info['name']}' from Worker {src_rank}")
            
            filename = f"from_worker{src_rank}_{info['name']}"
            total = 0
            
            with open(filename, 'wb') as f:
                while True:
                    if self.comm.Iprobe(source=src_rank, tag=CONTROL_TAG):
                        sig = self.comm.recv(source=src_rank, tag=CONTROL_TAG)
                        if sig == COMPLETE:
                            break
                    
                    if self.comm.Iprobe(source=src_rank, tag=DATA_TAG):
                        chunk = self.comm.recv(source=src_rank, tag=DATA_TAG)
                        f.write(chunk)
                        total += len(chunk)
                        progress = (total / info['size']) * 100
                        if int(progress) % 25 == 0:
                            self.worker_log(f"  Progress: {progress:.0f}%")
            
            # Verify
            if os.path.getsize(filename) == info['size']:
                if self.checksum(filename) == info['checksum']:
                    self.worker_log(f"Saved: {filename}")
                else:
                    self.worker_log(f"Saved: {filename} (checksum mismatch)")
            else:
                self.worker_log("Size mismatch!")
                
        except Exception as e:
            self.worker_log(f"Receive error: {e}")
    
    def worker_handle_master_initiated_send(self):
        """Handle master-initiated worker-to-worker transfer"""
        data = self.comm.recv(source=0, tag=METADATA_TAG)
        dst_worker = data['to']
        filename = data['filename']
        
        if os.path.exists(filename):
            self.worker_log(f"Master requested to send '{filename}' to Worker {dst_worker}")
            self.worker_send_to_worker(filename, dst_worker)
        else:
            self.worker_log(f"Error: File '{filename}' not found")
    
    def worker_send_to_master(self, filename):
        """Worker sends file to master"""
        if not os.path.exists(filename):
            self.worker_log(f"Error: File '{filename}' not found")
            return
        
        info = self.get_file_info(filename)
        self.worker_log(f"Sending '{info['name']}' to Master")
        
        self.comm.send(TRANSFER, dest=0, tag=CONTROL_TAG)
        self.comm.send({'from': self.rank, 'info': info}, dest=0, tag=METADATA_TAG)
        
        with open(filename, 'rb') as f:
            for i in range(info['chunks']):
                chunk_size = info['last'] if i == info['chunks']-1 else CHUNK_SIZE
                self.comm.send(f.read(chunk_size), dest=0, tag=DATA_TAG)
        
        self.comm.send(COMPLETE, dest=0, tag=CONTROL_TAG)
        self.worker_log("File sent to Master")
    
    def worker_send_to_worker(self, filename, dst_worker):
        """Worker sends file to another worker"""
        if not os.path.exists(filename):
            self.worker_log(f"Error: File '{filename}' not found")
            return
        
        if dst_worker == self.rank:
            self.worker_log("Error: Cannot send file to self")
            return
        
        if not 1 <= dst_worker < self.size:
            self.worker_log(f"Error: Invalid destination worker")
            return
        
        info = self.get_file_info(filename)
        self.worker_log(f"Sending '{info['name']}' to Worker {dst_worker}")
        
        # Send start signal
        self.comm.send(TRANSFER, dest=dst_worker, tag=CONTROL_TAG)
        self.comm.send({'from': self.rank, 'info': info}, dest=dst_worker, tag=METADATA_TAG)
        
        # Send file
        with open(filename, 'rb') as f:
            for i in range(info['chunks']):
                chunk_size = info['last'] if i == info['chunks']-1 else CHUNK_SIZE
                self.comm.send(f.read(chunk_size), dest=dst_worker, tag=DATA_TAG)
        
        self.comm.send(COMPLETE, dest=dst_worker, tag=CONTROL_TAG)
        self.worker_log(f"File sent to Worker {dst_worker}")
    
    def worker_send_message(self, dst_worker, message):
        """Worker sends message to another worker"""
        if dst_worker == self.rank:
            self.worker_log("Error: Cannot send message to self")
            return
        
        if not 1 <= dst_worker < self.size:
            self.worker_log(f"Error: Invalid destination worker")
            return
        
        self.comm.send(f"[Worker {self.rank}]: {message}", dest=dst_worker, tag=MESSAGE_TAG)
        self.worker_log(f"Message sent to Worker {dst_worker}")
    
    def process_worker_command(self, cmd):
        """Process worker commands"""
        parts = cmd.strip().split()
        if not parts:
            return
        
        action = parts[0].lower()
        
        if action == "send" and len(parts) == 3:
            try:
                dst = int(parts[1])
                if 1 <= dst < self.size and dst != self.rank:
                    self.worker_send_to_worker(parts[2], dst)
                else:
                    self.worker_log("Error: Invalid destination worker")
            except:
                self.worker_log("Error: Invalid worker number")
        
        elif action == "msg" and len(parts) >= 3:
            try:
                dst = int(parts[1])
                message = " ".join(parts[2:])
                self.worker_send_message(dst, message)
            except:
                self.worker_log("Error: Invalid command")
        
        elif action == "messages":
            self.worker_log(f"Recent messages:")
            for src, msg in self.messages[-5:]:
                self.worker_log(f"  From Worker {src}: {msg}")
            if not self.messages:
                self.worker_log("  No messages")
        
        elif action == "files":
            self.worker_log(f"Files in directory:")
            files = [f for f in os.listdir('.') if os.path.isfile(f)]
            if files:
                for f in files:
                    size = os.path.getsize(f)
                    self.worker_log(f"  {f} ({size:,} bytes)")
            else:
                self.worker_log("  No files")
        
        elif action == "status":
            self.worker_log(f"Worker Status:")
            self.worker_log(f"  Rank: {self.rank}")
            self.worker_log(f"  Total Workers: {self.size - 1}")
            self.worker_log(f"  Messages received: {len(self.messages)}")
            self.worker_log(f"  Running: {'Yes' if self.running else 'No'}")
        
        elif action == "help":
            self.worker_log(f"Commands:")
            self.worker_log("  send <worker> <file>   - Send file to another worker")
            self.worker_log("  msg <worker> <message> - Send message to worker")
            self.worker_log("  messages               - Show recent messages")
            self.worker_log("  files                  - List local files")
            self.worker_log("  status                 - Show worker status")
            self.worker_log("  help                   - Show commands")
            self.worker_log("  exit                   - Exit (only this worker)")
        
        elif action == "exit":
            self.worker_log("Exiting worker...")
            self.running = False
        
        else:
            self.worker_log("Unknown command. Type 'help' for commands")
        
    def run(self):
        if self.is_master:
            self.master_interface()
        else:
            self.worker_interface()

def main():
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    
    if size < 2:
        if rank == 0:
            print("Need at least 2 processes: mpiexec -n 4 python file_transfer_mpi.py")
        return
    
    system = FileTransfer()
    system.run()

if __name__ == "__main__":
    main()