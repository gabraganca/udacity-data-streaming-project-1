"""Producer base-class providing common utilites and functionality"""
import logging
import time
import concurrent.futures

# from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            config={
                **self.broker_properties,
                "schema.registry.url": "http://localhost:8081",
                "group.id": "0",
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic"""
        client = AdminClient(self.broker_properties)
        if self.topic_name in client.list_topics().topics.keys():
            logger.info("topic %s already exist", self.topic_name)
            return

        logger.debug("creating topic %s", self.topic_name)
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )
        logger.debug('waiting result for topic %s creation', self.topic_name)
        for _, future in futures.items():
            try:
                future.result(timeout=0.05)
                logger.info("topic %s created", self.topic_name)
            except concurrent.futures.TimeoutError:
                logger.error("creation of topic %s timed out", self.topic_name)
            except Exception as e:
                logger.error("failed to create topic %s: %s", self.topic_name, e)

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    # def time_millis(self):
    #     """Use this function to get the key for Kafka Events"""
    #     return int(round(time.time() * 1000))
