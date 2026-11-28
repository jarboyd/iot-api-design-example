import grpc
import charge_point_pb2
import charge_point_pb2_grpc

from charge_point_repository import ChargePointRepository

class ChargePointService(charge_point_pb2_grpc.ChargePointServiceServicer):

    def __init__(self):
        self.repository = ChargePointRepository()


    def GetChargePoint(self, request, context):

        if not request.charge_point_id:
            context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "charge point ID required"
                    )

        status = self.repository.get_charge_point(request.charge_point_id)

        if status is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Charge point not found"
            )

        return charge_point_pb2.ChargePoint(
            charge_point_id = request.charge_point_id,
            status = status
        )

    def ReserveChargePoint(self, request, context):

        if not request.charge_point_id:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Charge point ID is required"
            )

        try:
            status = self.repository.reserve_charge_point(
                request.charge_point_id
            )

        except KeyError:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Charge point not found"
            )

        except ValueError:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Charge point is not available"
            )

        return charge_point_pb2.ChargePoint(
            charge_point_id=request.charge_point_id,
            status=status
        )

    