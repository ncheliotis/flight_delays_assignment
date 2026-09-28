from pathlib import Path

# Define the project root directory and data directories


PROJECT_ROOT = Path(__file__).parent.parent

DATA_ROOT = PROJECT_ROOT / "01.Data"

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

TASK_2_VALIDATION_COLUMNS = [
    "YEAR",
    "MONTH",
    "DAY",
    "AIRLINE",
    "TAIL_NUMBER",
    "DEPARTURE_TIME",
    "ARRIVAL_TIME",
    "ELAPSED_TIME",
]

# Define the correction and feed window days
CORRECTION_WINDOW_DAYS = 7

FEED_WINDOW_DAYS = 10  # 7 days of correction + possible buffer, could easily be 7 days

# If this script is run directly, print the paths and check if the source files exist
# if __name__ == "__main__":
#     print("Project Root:", PROJECT_ROOT)
#     print("Data Root:", DATA_ROOT)
#     print("Bronze Path:", bronze_path)
#     print("Silver Path:", silver_path)
#     print("Gold Path:", gold_path)
#     print("Flights Source:", FLIGHTS_SOURCE)
#     print("Airports Source:", AIRPORTS_SOURCE)
#     print("Silver Flights Path:", SILVER_FLIGHTS_PATH)
#     print("\nFlights Source Exists:", FLIGHTS_SOURCE.exists())
#     print("Airlines Source Exists:", AIRLINES_SOURCE.exists())
#     print("Airports Source Exists:", AIRPORTS_SOURCE.exists())
#     print("Gold Availability Path:", GOLD_AVAILABILITY_PATH)
