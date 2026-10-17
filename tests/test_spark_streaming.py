import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Row, SparkSession


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from spark_streaming import ALERTS_SCHEMA, build_alerts  # noqa: E402


class BuildAlertsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("goit-de-hw-06-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.conditions = cls.spark.createDataFrame(
            [
                ("1", 0.0, 40.0, -999.0, -999.0, "101", "It's too dry"),
                ("2", 60.0, 100.0, -999.0, -999.0, "102", "It's too wet"),
                ("3", -999.0, -999.0, -300.0, 30.0, "103", "It's too cold"),
                ("4", -999.0, -999.0, 40.0, 300.0, "104", "It's too hot"),
            ],
            ALERTS_SCHEMA,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def averages(self, temperature: float, humidity: float):
        start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        end = datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc)
        return self.spark.createDataFrame(
            [(Row(start=start, end=end), temperature, humidity)],
            ["window", "temperature_avg", "humidity_avg"],
        )

    def alert_codes(self, temperature: float, humidity: float) -> set[str]:
        alerts = build_alerts(
            self.averages(temperature, humidity),
            self.conditions,
        )
        return {row.code for row in alerts.select("code").collect()}

    def test_hot_and_dry_average_matches_two_csv_conditions(self) -> None:
        self.assertEqual(self.alert_codes(45.0, 30.0), {"101", "104"})

    def test_normal_temperature_and_wet_humidity_match_one_condition(self) -> None:
        self.assertEqual(self.alert_codes(35.0, 70.0), {"102"})

    def test_condition_boundaries_are_inclusive(self) -> None:
        self.assertEqual(self.alert_codes(40.0, 40.0), {"101", "104"})


if __name__ == "__main__":
    unittest.main()
