import socket
import time


class DataGenerator:
    def __init__(
        self, host="127.0.0.1", port=65432, message_interval=2, num_messages=None
    ):
        self.host = host
        self.port = port
        self.message_interval = message_interval
        self.num_messages = num_messages

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Message Queue started on {self.host}:{self.port}")
            conn, addr = s.accept()
            with conn:
                print("Connected by", addr)
                message_count = 0
                while True:
                    if self.num_messages and message_count >= self.num_messages:
                        break
                    message = f"This is a mock message {message_count}.\n"
                    conn.sendall(message.encode("utf-8"))
                    time.sleep(self.message_interval)
                    message_count += 1


if __name__ == "__main__":
    # Example usage:
    mock_queue = DataGenerator(message_interval=1, num_messages=1000)
    mock_queue.start()
