import importlib.util
import pathlib
import sys
import types
import unittest
from unittest.mock import patch


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_script_module(module_name: str, relative_path: str):
    kafka_module = types.ModuleType("kafka")
    kafka_admin_module = types.ModuleType("kafka.admin")
    kafka_errors_module = types.ModuleType("kafka.errors")
    kafka_structs_module = types.ModuleType("kafka.structs")
    matplotlib_module = types.ModuleType("matplotlib")
    pyplot_module = types.ModuleType("matplotlib.pyplot")

    class KafkaAdminClient:
        pass

    class KafkaConsumer:
        pass

    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __eq__(self, other):
            return (self.topic, self.partition) == (other.topic, other.partition)

    class KafkaError(Exception):
        pass

    class NewPartitions:
        def __init__(self, total_count):
            self.total_count = total_count

    class OffsetAndMetadata:
        def __init__(self, offset, metadata):
            self.offset = offset
            self.metadata = metadata

    kafka_module.KafkaAdminClient = KafkaAdminClient
    kafka_module.KafkaConsumer = KafkaConsumer
    kafka_module.TopicPartition = TopicPartition
    kafka_admin_module.KafkaAdminClient = KafkaAdminClient
    kafka_admin_module.NewPartitions = NewPartitions
    kafka_errors_module.KafkaError = KafkaError
    kafka_structs_module.OffsetAndMetadata = OffsetAndMetadata

    for method_name in ("plot", "axhline", "xlabel", "ylabel", "title", "legend", "grid", "show", "figure"):
        setattr(pyplot_module, method_name, lambda *args, **kwargs: None)

    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)

    with patch.dict(
        sys.modules,
        {
            "kafka": kafka_module,
            "kafka.admin": kafka_admin_module,
            "kafka.errors": kafka_errors_module,
            "kafka.structs": kafka_structs_module,
            "matplotlib": matplotlib_module,
            "matplotlib.pyplot": pyplot_module,
        },
    ):
        spec.loader.exec_module(module)

    return module


class ScriptArgumentTests(unittest.TestCase):
    def test_alter_partitions_defaults_and_overrides(self):
        module = load_script_module("alter_partitions", "scripts/alter_partitions.py")

        defaults = module.parse_args([])
        self.assertEqual(defaults.broker, "localhost:9092")
        self.assertEqual(defaults.topic, "your_topic")
        self.assertEqual(defaults.num_partitions, 3)

        custom = module.parse_args(["--broker", "kafka:9093", "--topic", "orders", "--num-partitions", "8"])
        self.assertEqual(custom.broker, "kafka:9093")
        self.assertEqual(custom.topic, "orders")
        self.assertEqual(custom.num_partitions, 8)

    def test_broker_latency_defaults_and_overrides(self):
        module = load_script_module("broker_latency", "scripts/broker_latency.py")

        defaults = module.parse_args([])
        self.assertEqual(defaults.brokers, ["localhost:9092"])
        self.assertEqual(defaults.timeout, 1)
        self.assertEqual(defaults.ping_interval, 5)
        self.assertEqual(defaults.monitor_duration, 60)
        self.assertEqual(defaults.high_latency_threshold, 200)

        custom = module.parse_args(
            [
                "--brokers",
                "kafka-1:9092",
                "kafka-2:9092",
                "--timeout",
                "3",
                "--ping-interval",
                "10",
                "--monitor-duration",
                "90",
                "--high-latency-threshold",
                "250.5",
            ]
        )
        self.assertEqual(custom.brokers, ["kafka-1:9092", "kafka-2:9092"])
        self.assertEqual(custom.timeout, 3)
        self.assertEqual(custom.ping_interval, 10)
        self.assertEqual(custom.monitor_duration, 90)
        self.assertEqual(custom.high_latency_threshold, 250.5)

    def test_consumer_lag_alert_arguments(self):
        module = load_script_module("consumer_lag_alert", "scripts/consumer_lag_alert.py")

        defaults = module.parse_args([])
        self.assertEqual(defaults.consumer_group, "your_consumer_group")
        self.assertEqual(defaults.topic, "your_topic")
        self.assertEqual(defaults.lag_threshold, 100)

        custom = module.parse_args(
            [
                "--broker",
                "kafka:9093",
                "--consumer-group",
                "analytics",
                "--topic",
                "orders",
                "--lag-threshold",
                "500",
            ]
        )
        self.assertEqual(custom.broker, "kafka:9093")
        self.assertEqual(custom.consumer_group, "analytics")
        self.assertEqual(custom.topic, "orders")
        self.assertEqual(custom.lag_threshold, 500)

    def test_consumer_offset_reset_arguments(self):
        module = load_script_module("consumer_offset_reset", "scripts/consumer_offset_reset.py")

        defaults = module.parse_args([])
        self.assertEqual(defaults.reset_to, "earliest")

        custom = module.parse_args(
            [
                "--broker",
                "kafka:9094",
                "--consumer-group",
                "backfill",
                "--topic",
                "payments",
                "--reset-to",
                "12345",
            ]
        )
        self.assertEqual(custom.broker, "kafka:9094")
        self.assertEqual(custom.consumer_group, "backfill")
        self.assertEqual(custom.topic, "payments")
        self.assertEqual(custom.reset_to, "12345")

    def test_metrics_visualization_arguments(self):
        module = load_script_module("metrics_visualization", "scripts/metrics_visualization.py")

        defaults = module.parse_args([])
        self.assertEqual(defaults.broker, "localhost:9092")
        self.assertEqual(defaults.consumer_group, "your_consumer_group")
        self.assertEqual(defaults.topic, "your_topic")
        self.assertEqual(defaults.lag_threshold, 100)
        self.assertEqual(defaults.monitor_duration, 60)
        self.assertEqual(defaults.poll_interval, 5)

        custom = module.parse_args(
            [
                "--broker",
                "kafka:9095",
                "--consumer-group",
                "dashboard",
                "--topic",
                "events",
                "--lag-threshold",
                "75",
                "--monitor-duration",
                "300",
                "--poll-interval",
                "15",
            ]
        )
        self.assertEqual(custom.broker, "kafka:9095")
        self.assertEqual(custom.consumer_group, "dashboard")
        self.assertEqual(custom.topic, "events")
        self.assertEqual(custom.lag_threshold, 75)
        self.assertEqual(custom.monitor_duration, 300)
        self.assertEqual(custom.poll_interval, 15)


if __name__ == "__main__":
    unittest.main()
