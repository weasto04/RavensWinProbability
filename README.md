# RavensWinProbability

Utilities for working with Baltimore Ravens 2024 play‑by‑play data.

## Contents

Files in this repository:

* `BAL_Ravens_2024.csv` – Source play-by-play dataset (wide CSV; 3304 rows currently).
* `build_db.py` – Helper script to generate a SQLite database from the CSV.
* `ravens_2024.db` – Generated SQLite database (created after you run the script).
* `queries.sql` – (Placeholder) Put your exploratory SQL queries here.
* `requirements.txt` – Python dependency list (currently just `pandas`).

## Quick start

### 1. Create and activate environment (optional but recommended)

You can use any Python 3.10+ environment. Example with `venv`:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

### 2. Build the SQLite database

Run the build script (defaults produce `ravens_2024.db` and table `ravens_2024`):

```bash
python build_db.py
```

Sample output:

```
Wrote 3304 rows to /path/to/repo/ravens_2024.db table 'ravens_2024'
```

Custom options:

```bash
python build_db.py --csv BAL_Ravens_2024.csv --db custom.db --table plays --if-exists replace
```

Options:

* `--csv` Path to input CSV (default: `BAL_Ravens_2024.csv`)
* `--db` Output SQLite DB filename (default: `ravens_2024.db`)
* `--table` Table name (default: `ravens_2024`)
* `--if-exists` One of `replace|append|fail` (defaults to `replace`)
* `--chunksize` Optional chunk size for very large future datasets

The script will add a synthetic primary key column `row_id` if it does not exist and attempt to coerce it into the table's primary key for performant querying.

### 3. Explore the data

Use the SQLite CLI:

```bash
sqlite3 ravens_2024.db
```

Inside the prompt:

```sql
.mode column
.headers on
PRAGMA table_info(ravens_2024);
SELECT COUNT(*) AS total_rows FROM ravens_2024;
SELECT row_id, game_id, play_id, posteam, defteam, play_type, desc
FROM ravens_2024
ORDER BY row_id
LIMIT 5;
```

Or with Python / pandas:

```python
import pandas as pd
import sqlite3
con = sqlite3.connect('ravens_2024.db')
df = pd.read_sql_query("SELECT play_type, AVG(epa) AS avg_epa FROM ravens_2024 WHERE play_type != '' GROUP BY play_type ORDER BY avg_epa DESC LIMIT 10", con)
print(df)
```

### 4. Add your queries

Place frequently used SQL in `queries.sql` so they can be versioned and reused.

### 5. Regenerating the DB after CSV updates

If the CSV changes or grows:

```bash
python build_db.py --if-exists replace
```

### Schema notes

* Data types are inferred by pandas at load time; some numeric columns with mixed data may become TEXT.
* Very wide dataset – consider creating indexes for performance (see below).

### Optional: create helpful indexes

After the DB is created you can speed up common filters:

```sql
CREATE INDEX idx_ravens_game_id ON ravens_2024 (game_id);
CREATE INDEX idx_ravens_play_type ON ravens_2024 (play_type);
CREATE INDEX idx_ravens_posteam_defteam ON ravens_2024 (posteam, defteam);
```

### Win probability reduced dataset & excitement metric

Run:

```bash
python extract_winprob.py
```

Outputs:
* `ravens_wp.csv` – per-play rows with Ravens win probability and timing
* `ravens_wp.json` – grouped by game for front-end use

Each game in the JSON now has a very simple internal excitement metric:

* `lead_changes` – how many times the Ravens win probability crosses the 50% line (>= 0.5 vs < 0.5)
* `amplitude` – max win prob minus min win prob in that game

The highlight game is chosen by highest `lead_changes`; ties are broken by higher `amplitude`. These values are not shown in the UI—only the chosen game gets a badge. Adjust or expose later if you want something fancier.

### License

No explicit license provided yet. Add one if you plan to distribute.
