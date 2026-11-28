import charge_point_pb2
import threading

class ChargePointRepository:

    def __init__(self):

        self._lock = threading.Lock()

        #Testdaten
        self._charge_points = {
            "CP-001": charge_point_pb2.CHARGE_POINT_STATUS_AVAILABLE,
            "CP-002": charge_point_pb2.CHARGE_POINT_STATUS_AVAILABLE,
            "CP-003": charge_point_pb2.CHARGE_POINT_STATUS_OCCUPIED,
            "CP-004": charge_point_pb2.CHARGE_POINT_STATUS_OUT_OF_SERVICE
        }

    def get_charge_point(self, charge_point_id: str) -> int | None:
        return self._charge_points.get(charge_point_id)    

    def reserve_charge_point(self, charge_point_id: str) -> int:

        with self._lock:

            status = self.get_charge_point(charge_point_id)

            if status is None:
                raise KeyError("Charge point not found")

            if status != charge_point_pb2.CHARGE_POINT_STATUS_AVAILABLE:
                raise ValueError("Charge point is not available")

            self._charge_points[charge_point_id] = (
                charge_point_pb2.CHARGE_POINT_STATUS_OCCUPIED
            )

            return self._charge_points[charge_point_id]