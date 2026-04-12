from pydantic import BaseModel
from simple_dag import transform, JsonInput, JsonOutput
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SensorReading(BaseModel):
    sensor_id: str
    value: float


class Alert(BaseModel):
    sensor_id: str
    triggered: bool


@transform(
    reading=JsonInput(
        os.path.join(BASE_DIR, "data/reading.json"),
        schema_validation=SensorReading,
        name="raw_reading",
    ),
    output=JsonOutput(
        os.path.join(BASE_DIR, "data/alert.json"),
        name="alert",
    ),
)
def evaluate_reading(reading: SensorReading, output: JsonOutput):
    alert = Alert(sensor_id=reading.sensor_id, triggered=reading.value > 100)
    output.write_data(alert)


if __name__ == "__main__":
    evaluate_reading()
