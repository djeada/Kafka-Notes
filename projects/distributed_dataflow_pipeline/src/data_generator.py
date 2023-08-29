import socket
import time
import threading

class DataGenerator:
    def __init__(
        self, host="127.0.0.1", port=65432, message_interval=1, num_messages=None
    ):
        self.host = host
        self.port = port
        self.message_interval = message_interval
        self.num_messages = num_messages

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Data Generator started on {self.host}:{self.port}")

            while True:
                conn, addr = s.accept()
                print("Connected by", addr)
                client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                client_thread.start()

    def handle_client(self, conn, addr):
        with conn:
            message_count = 0
            try:
                while True:
                    if self.num_messages and message_count >= self.num_messages:
                        break
                    message = f"This is a mock message from Data Generator {message_count}.\n"
                    print(message)
                    conn.sendall(message.encode("utf-8"))
                    time.sleep(self.message_interval)
                    message_count += 1

                # Add a slight delay after sending the last message.
                time.sleep(10)
                # Gracefully shutdown the connection.
                conn.shutdown(socket.SHUT_WR)

            except socket.error as e:
                print(f"Socket error: {e}")

if __name__ == "__main__":
    # Example usage:
    mock_queue = DataGenerator(message_interval=0.01, num_messages=1000)
    mock_queue.start()
