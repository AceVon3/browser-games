"""
weather.py — Fetch wind/temp for each stadium at game time.

Uses OpenWeatherMap API for live data, Open-Meteo for historical (backtest).
"""

import os
import logging
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("pipeline")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Stadium coordinates (lat, lon) for weather lookups
STADIUM_COORDS = {
    "Angel Stadium": (33.8003, -117.8827),
    "Busch Stadium": (38.6226, -90.1928),
    "Chase Field": (33.4455, -112.0667),
    "Citi Field": (40.7571, -73.8458),
    "Citizens Bank Park": (39.9061, -75.1665),
    "Comerica Park": (42.3390, -83.0485),
    "Coors Field": (39.7559, -104.9942),
    "Dodger Stadium": (34.0739, -118.2400),
    "Fenway Park": (42.3467, -71.0972),
    "Globe Life Field": (32.7512, -97.0832),
    "Great American Ball Park": (39.0974, -84.5082),
    "Guaranteed Rate Field": (41.8299, -87.6338),
    "Kauffman Stadium": (39.0517, -94.4803),
    "LoanDepot Park": (25.7781, -80.2196),
    "Minute Maid Park": (29.7572, -95.3555),
    "Nationals Park": (38.8730, -77.0074),
    "Oakland Coliseum": (37.7516, -122.2005),
    "Oracle Park": (37.7786, -122.3893),
    "Oriole Park at Camden Yards": (39.2838, -76.6218),
    "PNC Park": (40.4468, -80.0057),
    "Petco Park": (32.7076, -117.1570),
    "Progressive Field": (41.4962, -81.6852),
    "Rogers Centre": (43.6414, -79.3894),
    "T-Mobile Park": (47.5914, -122.3325),
    "Target Field": (44.9818, -93.2775),
    "Tropicana Field": (27.7682, -82.6534),
    "Truist Park": (33.8911, -84.4681),
    "Wrigley Field": (41.9484, -87.6553),
    "Yankee Stadium": (40.8296, -73.9262),
    "loanDepot park": (25.7781, -80.2196),
}

# Stadiums with retractable or fixed roofs (weather less relevant)
DOMED_STADIUMS = {
    "Globe Life Field",
    "LoanDepot Park",
    "loanDepot park",
    "Minute Maid Park",
    "Rogers Centre",
    "Tropicana Field",
    "Chase Field",
    "T-Mobile Park",
}


def fetch_weather(venue: str, game_time_utc: Optional[str] = None) -> dict:
    """Fetch weather data for a stadium.

    Returns dict with keys: temp_f, wind_mph, wind_dir, wind_description,
    is_dome. Returns defaults if API fails.
    """
    default = {
        "temp_f": 72,
        "wind_mph": 0,
        "wind_dir": "calm",
        "wind_description": "calm",
        "is_dome": venue in DOMED_STADIUMS,
    }

    if venue in DOMED_STADIUMS:
        default["wind_description"] = "dome — no weather effect"
        return default

    if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY == "your_key_here":
        logger.warning(
            "[ALERT] Weather API key not configured — weather adjustment "
            "skipped for %s",
            venue,
        )
        return default

    coords = STADIUM_COORDS.get(venue)
    if not coords:
        logger.warning(
            "[ALERT] No coordinates for stadium '%s' — weather adjustment skipped",
            venue,
        )
        return default

    lat, lon = coords
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": lat,
            "lon": lon,
            "appid": OPENWEATHER_API_KEY,
            "units": "imperial",
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        wind_speed = data.get("wind", {}).get("speed", 0)
        wind_deg = data.get("wind", {}).get("deg", 0)
        temp = data.get("main", {}).get("temp", 72)

        return {
            "temp_f": round(temp),
            "wind_mph": round(wind_speed),
            "wind_dir": _degrees_to_direction(wind_deg),
            "wind_description": _wind_description(wind_speed, wind_deg, venue),
            "is_dome": False,
        }
    except Exception as e:
        logger.warning(
            "[ALERT] Weather unavailable for %s — weather adjustment skipped. Error: %s",
            venue, e,
        )
        return default


def _degrees_to_direction(deg: float) -> str:
    """Convert wind degrees to compass direction."""
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = round(deg / 22.5) % 16
    return dirs[idx]


def _wind_description(speed: float, deg: float, venue: str) -> str:
    """Generate human-readable wind description relative to field orientation.

    This is a simplification — ideally each stadium's orientation would be
    mapped to determine 'out to CF' vs 'in from CF'. For now, we use a
    generic description based on wind speed.
    """
    if speed < 5:
        return "calm"
    direction = _degrees_to_direction(deg)
    if speed >= 15:
        return f"strong {direction} at {round(speed)} mph"
    return f"{direction} at {round(speed)} mph"


def calculate_weather_adjustment(weather: dict) -> dict:
    """Calculate weather adjustments for scoring.

    Returns dict with:
        edge_adj: adjustment to pitching edge score (+ = favors pitcher)
        run_adj: adjustment for O/U model total (+ = more runs)
    """
    if weather.get("is_dome"):
        return {"edge_adj": 0, "run_adj": 0.0}

    edge_adj = 0
    run_adj = 0.0
    wind_mph = weather.get("wind_mph", 0)
    wind_desc = weather.get("wind_description", "calm")
    temp = weather.get("temp_f", 72)

    # Wind adjustments (simplified — full implementation needs per-stadium orientation)
    if wind_mph >= 15:
        if "out" in wind_desc.lower():
            edge_adj -= 5  # favors batters
            run_adj += 0.5
        elif "in" in wind_desc.lower():
            edge_adj += 5  # favors pitchers
            run_adj -= 0.5
        else:
            # Cross wind
            edge_adj += 2 if "strong" in wind_desc.lower() else 0
            run_adj += 0.1

    # Temperature adjustments
    if temp < 45:
        edge_adj += 3  # cold favors pitchers
        run_adj -= 0.3
    elif temp > 90:
        edge_adj -= 2  # heat favors batters
        run_adj += 0.2

    return {"edge_adj": edge_adj, "run_adj": run_adj}


def fetch_historical_weather(lat: float, lon: float, date_str: str) -> dict:
    """Fetch historical weather from Open-Meteo (free, no API key). For backtesting."""
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "daily": "temperature_2m_max,windspeed_10m_max,winddirection_10m_dominant",
            "temperature_unit": "fahrenheit",
            "windspeed_unit": "mph",
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        return {
            "temp_f": round(daily.get("temperature_2m_max", [72])[0]),
            "wind_mph": round(daily.get("windspeed_10m_max", [0])[0]),
            "wind_dir": _degrees_to_direction(
                daily.get("winddirection_10m_dominant", [0])[0]
            ),
            "wind_description": "historical",
            "is_dome": False,
        }
    except Exception as e:
        logger.warning("[ALERT] Historical weather fetch failed — %s", e)
        return {"temp_f": 72, "wind_mph": 0, "wind_dir": "calm",
                "wind_description": "unavailable", "is_dome": False}
