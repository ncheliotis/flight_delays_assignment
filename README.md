# Flight delays assignment

A Bronze/Silver/Gold pipeline over the 2015 US flight delays dataset, built with PySpark.
It loads the raw files, masters them into a deduplicated `flights` dataset, and produces
`flights_availability`: daily ground time per airline.

## How to run

Requires Python: 3.14.3, Java: OpenJDK 17.0.20.1. and PySpark: 4.2.0


```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Writing Parquet locally on Windows also needs `winutils.exe` and `hadoop.dll` with
`HADOOP_HOME` set.

Put `flights.csv`, `airlines.csv` and `airports.csv` in `01.data/00.Source/`, then:

```
python 02.Notebooks/01_Bronze_Ingestion.py
python 02.Notebooks/02_Silver_Flights.py
python 02.Notebooks/03_Gold_Availability.py
```

`00_Data_Exploration.py` is not part of the pipeline. It was the staging step used to
understand the source before writing the Silver logic, and the findings below come from
it. It uses `# %%` markers so it can be run cell by cell.

## Layout

```
01.data/
  00.Source/    the three CSV files as delivered
  01.Bronze/    same data as parquet, no business logic
  02.Silver/    mastered flights, plus rows rejected during deduplication
  03.Gold/      flights_availability
02.Notebooks/   pipeline steps
03.src/         config, transformations, data quality helpers
```

Paths, business keys and pipeline parameters are all in `03.src/config.py`.

## The data

5,819,079 flights over 365 days of 2015, 14 airlines, 322 airports. Daily volume runs
from 10,068 to 17,574 flights, averaging 15,943.

Four things from the exploration drove the design.

**The business key is incomplete.** `TAIL_NUMBER` is empty on 14,721 rows. All of them
are cancelled flights, though most cancelled flights do have one.

**The business key is not unique.** Two keys appear twice, and the rows behind them
differ. Both pairs have a different `ORIGIN_AIRPORT`, which is impossible for one
aircraft at one moment, so it looks like a source error.

**`DEPARTURE_TIME` has no day attached.** AS98 on 1 January is scheduled at 00:05, left
11 minutes early, and shows `2354` — the evening of 31 December. Read literally it puts
the flight a day off. Same problem in reverse for flights delayed past midnight.

**`ARRIVAL_TIME` is on a different clock.** It is local at the destination, so
`ARRIVAL_TIME - DEPARTURE_TIME` is not the duration. AS98 leaves Anchorage 23:54, flies
194 minutes, lands 03:08 Anchorage time = 04:08 Seattle, which matches the recorded
`ARRIVAL_TIME`. Duration comes from `ELAPSED_TIME`.

## Task 1: storing and updating `flights` daily

### Choice of n

Corrections arrive for the 7 most recent days, so `n = 7` is the floor. Anything less and
a correction lands on a day the feed no longer covers.

Exactly 7 leaves no room for failure. If a run is missed and not rerun the same day, the
corrections that were only in that feed are gone, because the next feed has rolled past
them.

**n = 10** covers three missed runs. At 15,943 flights a day that is ~159,000 rows per
feed instead of ~112,000, which is not a meaningful cost.

`CORRECTION_WINDOW_DAYS = 7` and `FEED_WINDOW_DAYS = 10` live in `config.py`.

### The flow

`flights` is partitioned by `FLIGHT_DATE` and keyed on the seven columns from the
assignment. Each daily run:

1. Lands the feed as-is in Bronze, with an ingestion timestamp and source file name.
2. Prepares and deduplicates it with the same functions used for the historical load.
3. Merges it with `upsert_flights`: drop the rows the feed covers, append the feed. The
   feed wins, since that is where corrections come from.
4. Rebuilds Gold for those days plus one on either side, since a flight can cross
   midnight in either direction.

Only the feed's days are touched, as long as the anti-join is restricted to its date
range. `TAIL_NUMBER` is filled with `'UNKNOWN'` first, since it is a nullable key column
and `NULL` never matches `NULL`.

### Tools

Plain PySpark writing partitioned Parquet, so it runs anywhere.

In production the merge is what needs a real tool. On Fabric or Databricks the target
would be a Delta table and the merge collapses to a single atomic `MERGE`, replacing the
read-anti-join-union used here. The rest is a scheduled pipeline running the three steps
in order.

### Sanity checks

On the feed:

- Schema check via `validate_required_columns`, failing the run rather than merging.
- Exactly n distinct contiguous days, most recent being yesterday.
- Rows per day within the expected band. Observed range is 10,068 to 17,574, so
  thresholds near 8,000 and 20,000 catch a partial feed without firing on a holiday.
- Key complete after the `TAIL_NUMBER` fill and unique after deduplication.

On the target:

- Rows per day should barely move for days already present. A correction replaces a row
  rather than adding one, so a day that grows means the merge is not matching.
- Key still unique across the table, using the same `check_duplicates` assertion as
  Silver.
- Some rows in the correction window change, but not all of them. Either extreme
  suggests something changed upstream.
- Track which days each run processed, so a missed run is visible and can be replayed.

## Task 2: `flights_availability`

Ground time is the part of the day an aircraft is not flying, computed per aircraft-day
and then aggregated per airline.

### Placing a flight on the clock

Start is `SCHEDULED_DEPARTURE + DEPARTURE_DELAY`, in minutes from the start of the flight
day. Same instant as `DEPARTURE_TIME` but it keeps the day, so it can be negative or go
past 1440. End is start plus `ELAPSED_TIME`.

`ELAPSED_TIME` is gate-to-gate, which is the right measure for availability.

### Availability

The interval is cut at midnight and each piece charged to the day it happened on — the
day before, the flight day, or the day after. Per aircraft-day,
`availability = 1440 − minutes flown`.

### Night availability

The night runs from 23:00 to 07:00, so unlike availability it does not fit inside one
calendar day. The flights are therefore left whole instead of being cut at midnight, and
each one is checked against the night of its own day and the night before.

A row can show more night availability than total availability, since the night window
borrows hours from the next day.

### Airline attribution

Ground time belongs to the aircraft, the output is per airline. On 97 aircraft-days the
aircraft flew for more than one airline. Splitting would be a guess and crediting both
would double count, so the aircraft-day goes to the airline it flew the most minutes for,
with the airline code as tie-break for stability.

### Output

`03.Gold/flights_availability`, partitioned by flight date: 4,927 rows of `FLIGHT_DATE`,
`AIRLINE`, `AVAILABILITY`, `NIGHT_AVAILABILITY`. **Both measures are in minutes.**

## Assumptions and limitations

**Cancelled and diverted flights are excluded**, since ground time needs a start and a
duration and neither has an `ELAPSED_TIME` — a diverted flight never reached its
destination. That leaves 5.71m flights out of 5.82m. A diverted aircraft therefore counts
as grounded, which slightly overstates availability. Estimating the missing durations
from `SCHEDULED_TIME` would be inventing data.

**Only aircraft that flew appear.** An absent aircraft produces no row rather than 1440
minutes, since maintenance and missing reporting look the same in this data.

**Two rows were dropped as duplicates** and written to `02.Silver/flights_rejected` with
the reason. The key is given by the assignment, so Silver keeps one row per key using an
explicit deterministic rule; the alternative was adding `ORIGIN_AIRPORT` to the key. Both
rows were a cancelled and a diverted flight, so Task 2 is unaffected.

**Ground time is floored at zero.** On 107 aircraft-days the recorded flights overlap and
add up to more than 24 hours.

**The first and last day are slightly incomplete**, since flights crossing midnight at
the edges have no counterpart in the file. Days outside the range are dropped.