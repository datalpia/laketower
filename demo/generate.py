import shutil
from pathlib import Path

import pandas as pd
import requests
from deltalake import DeltaTable, Field, Schema, write_deltalake


def generate_weather(city: str, latitude: float, longitude: float):
    weather_table_path = Path(__file__).parent / "weather"
    shutil.rmtree(weather_table_path, ignore_errors=True)
    DeltaTable.create(
        weather_table_path,
        schema=Schema(
            [
                Field("time", "timestamp"),
                Field("city", "string"),
                Field("temperature_2m", "float"),
                Field("relative_humidity_2m", "float"),
                Field("wind_speed_10m", "float"),
            ]
        ),
        name="Weather",
        description="Historical and forecast weather data from open-meteo.com",
    )

    weather_history_json = requests.get(
        f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&timezone=GMT&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    ).json()
    weather_history_df = pd.DataFrame(weather_history_json["hourly"])
    weather_history_df["time"] = pd.to_datetime(weather_history_df["time"], utc=True)
    weather_history_df["city"] = city
    write_deltalake(weather_table_path, weather_history_df, mode="append")

    weather_forecast_json = requests.get(
        f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&timezone=GMT&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    ).json()
    weather_forecast_df = pd.DataFrame(weather_forecast_json["hourly"])
    weather_forecast_df["time"] = pd.to_datetime(weather_forecast_df["time"], utc=True)
    weather_forecast_df["city"] = city
    write_deltalake(weather_table_path, weather_forecast_df, mode="append")


def generate() -> None:
    deltatable_path = Path(__file__).parent / "sample_table"
    shutil.rmtree(deltatable_path, ignore_errors=True)
    DeltaTable.create(
        deltatable_path,
        schema=Schema([Field("num", "integer"), Field("letter", "string")]),
        name="Demo table",
        description="A sample demo Delta table",
    )

    data = [
        {"num": 1, "letter": "a"},
        {"num": 2, "letter": "b"},
        {"num": 3, "letter": "c"},
    ]
    table = pd.DataFrame(data)
    write_deltalake(deltatable_path, table, mode="append")

    data = [
        {"num": 4, "letter": "d"},
        {"num": 5, "letter": "e"},
    ]
    table = pd.DataFrame(data)
    write_deltalake(deltatable_path, table, mode="append")

    data = [
        {"num": 6, "letter": "f"},
        {"num": 7, "letter": "g"},
        {"num": 8, "letter": "h"},
    ]
    table = pd.DataFrame(data)
    write_deltalake(deltatable_path, table, mode="overwrite")

    print(DeltaTable(deltatable_path).history())


if __name__ == "__main__":
    generate()
    generate_weather("Grenoble", 45.1842259, 5.6743405)
