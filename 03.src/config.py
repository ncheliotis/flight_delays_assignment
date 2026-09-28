from pathlib import Path

# Define the project root directory and data directories


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_ROOT = PROJECT_ROOT / "01.data"

SOURCE_PATH = DATA_ROOT / "00.Source"

BRONZE_PATH = DATA_ROOT / "01.Bronze"

SILVER_PATH = DATA_ROOT / "02.Silver"

GOLD_PATH = DATA_ROOT / "03.Gold"

# Define the source file paths for airlines, flights, and airports data

AIRLINES_SOURCE = SOURCE_PATH / "airlines.csv"
FLIGHTS_SOURCE = SOURCE_PATH / "flights.csv"
AIRPORTS_SOURCE = SOURCE_PATH / "airports.csv"

# Define the destination file paths for the bronze datasets
BRONZE_FLIGHTS_PATH = BRONZE_PATH / "flights"
BRONZE_AIRLINES_PATH = BRONZE_PATH / "airlines"
BRONZE_AIRPORTS_PATH = BRONZE_PATH / "airports"


# Define the destination file paths for the silver flights data
SILVER_FLIGHTS_PATH = SILVER_PATH / "flights"
SILVER_REJECTED_PATH = SILVER_PATH / "flights_rejected"

# Define the destination file paths for the gold flights data
GOLD_AVAILABILITY_PATH = GOLD_PATH / "flights_availability"

# Create given business key
FLIGHTS_BK = [
    "YEAR",
    "MONTH",
    "DAY",
    "AIRLINE",
    "FLIGHT_NUMBER",
    "TAIL_NUMBER",
    "SCHEDULED_DEPARTURE",
]


# Define the correction and feed window days
CORRECTION_WINDOW_DAYS = 7

FEED_WINDOW_DAYS = 10  # 7 days of correction + possible buffer in case we missed runs
