import grpc
from concurrent import futures
import services_pb2 as services_pb2
import services_pb2_grpc as services_pb2_grpc

from MapReduce.mapper import Mapper


def start_map_service(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    services_pb2_grpc.add_MapperReducerServicer_to_server(
        Mapper("127.0.0.0", port), server
    )
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    port_no = int(input("Enter port: "))
    start_map_service(port_no)
