import grpc
from concurrent import futures
from app.database import get_connection
import user_pb2
import user_pb2_grpc

class UserService(user_pb2_grpc.UserServiceServicer):
    def CheckUser(self, request, context):
        email = request.email

        conn = get_connection()
        cur = conn.cursor()

        # QUERY CORRETTA
        cur.execute("""
            SELECT email, fiscal_code, bank_account
            FROM users
            WHERE email = %s
        """, (email,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            return user_pb2.UserResponse(
                exists=True,
                email=row["email"],
                fiscal_code=row["fiscal_code"],
                bank_account=row["bank_account"]
            )

        return user_pb2.UserResponse(exists=False)

def start_grpc_server(host="0.0.0.0", port=50051, block=True):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"gRPC server listening on {host}:{port}")

    if block:
        server.wait_for_termination()

    return server
