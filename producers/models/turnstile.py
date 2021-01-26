"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""

        super().__init__(
            topic_name="com.cta.turnstile",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,  # A first guess. Needs fine-tuning
            num_replicas=1,  # We are running a one-node cluster on docker
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value=dict(
                    station_id=self.station_id,
                    station_name=self.station_name,
                    line=self.color,
                ),
            )
