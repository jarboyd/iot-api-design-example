
import grpc

from concurrent.futures import ThreadPoolExecutor
from charge_point_service import ChargePointService

import charge_point_pb2_grpc


def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))

    charge_point_pb2_grpc.add_ChargePointServiceServicer_to_server(
        ChargePointService(), server
    )

    server.add_insecure_port("[::]:50052")
    server.start()

    print("ChargePointService running on port 50052")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
