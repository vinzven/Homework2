import grpc
import app.user_pb2 as user_pb2
import app.user_pb2_grpc as user_pb2_grpc
import time

class UserManagerClient:
    def __init__(self, host="user_manager", port=50051, retries=5, delay=3):
        self.host = host
        self.port = port
        self.retries = retries
        self.delay = delay
        self.stub = self._connect_with_retry()

    def _connect_with_retry(self):
        for i in range(self.retries):
            try:
                channel = grpc.insecure_channel(f"{self.host}:{self.port}")
                stub = user_pb2_grpc.UserServiceStub(channel)
                # prova a fare una chiamata di test al server
                # puoi fare una chiamata dummy o lasciarla al primo check_user
                return stub
            except grpc.RpcError as e:
                print(f"Errore connessione gRPC ({i+1}/{self.retries}): {e}")
                time.sleep(self.delay)
        raise ConnectionError(f"Impossibile connettersi a gRPC server {self.host}:{self.port}")

    def check_user(self, email):
        req = user_pb2.UserRequest(email=email)
        for i in range(self.retries):
            try:
                res = self.stub.CheckUser(req)
                return res.exists
            except grpc.RpcError as e:
                print(f"Errore gRPC durante check_user ({i+1}/{self.retries}): {e}")
                time.sleep(self.delay)
        return False
