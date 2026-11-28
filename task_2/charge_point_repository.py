import charge_point_pb2

class ChargePointRepository:

    def __init__(self):
        #Testdaten
        self._charge_points = {
            "CP-001": charge_point_pb2.CHARGE_POINT_STATUS_AVAILABLE,
            "CP-002": charge_point_pb2.CHARGE_POINT_STATUS_AVAILABLE,
            "CP-003": charge_point_pb2.CHARGE_POINT_STATUS_OCCUPIED,
            "CP-004": charge_point_pb2.CHARGE_POINT_STATUS_OUT_OF_SERVICE
        }