import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sensor_producer import make_reading  # noqa: E402


class MakeReadingTest(unittest.TestCase):
    def test_reading_has_required_schema_and_ranges(self) -> None:
        reading = make_reading("sensor-test", (41.0, 42.0), (20.0, 21.0))

        self.assertEqual(
            set(reading),
            {"id", "temperature", "humidity", "timestamp"},
        )
        self.assertEqual(reading["id"], "sensor-test")
        self.assertGreaterEqual(reading["temperature"], 41.0)
        self.assertLessEqual(reading["temperature"], 42.0)
        self.assertGreaterEqual(reading["humidity"], 20.0)
        self.assertLessEqual(reading["humidity"], 21.0)
        self.assertTrue(str(reading["timestamp"]).endswith("+00:00"))


if __name__ == "__main__":
    unittest.main()
