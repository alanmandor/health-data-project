"""Airflow adapters for reusable delivery checks."""

from airflow.providers.standard.sensors.python import PythonSensor


class DeliverySensor(PythonSensor):
    """Apply the manually supplied timeout on each rescheduled execution."""

    def execute(self, context):
        self.timeout = int(context["params"]["sensor_timeout_seconds"])
        return super().execute(context)
