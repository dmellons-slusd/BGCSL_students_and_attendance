"""
BGCSL Enrollment File Processor
================================
Watches ../../sftp_in/bgcsl/input_data for new enrollment_*.csv files,
matches each student against the Aeries database using fuzzy name + birthdate,
school ID, and course location, then writes the matched output to
../../sftp_in/bgcsl/output_data as an Excel file (matched_<original_name>.xlsx).

Attendance files (attendance_*.csv) are intentionally skipped — they will be
handled separately in a future phase.

NOTE on enrollment file types:
  - The first full file (enrollment_2025-09-05.csv) represents the entire district
    enrollment snapshot as of that date.
  - All subsequent files (enrollment_2025-09-01_2025-09-30.csv, etc.) are delta
    files containing only changes/additions for that period.
  - This distinction will matter when we eventually write enrollments back into
    the database — for now we just match and export all of them the same way.

Usage:
  python process_enrollments.py            # process all existing files, then watch
  python process_enrollments.py --once     # process all existing files and exit
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import chardet
from pandas import Series, concat, notna, read_sql_query, read_csv, to_datetime, to_numeric
from sqlalchemy import text
from slusdlib import aeries, core
from thefuzz import fuzz, process as fuzz_process
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent.resolve()
INPUT_DIR = (SCRIPT_DIR / "../../sftp_in/bgcsl/input_data").resolve()
OUTPUT_DIR = (SCRIPT_DIR / "../../sftp_in/bgcsl/output_data").resolve()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "process_enrollments.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grade mapping
# ---------------------------------------------------------------------------

GRADE_MAP = {
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "10": 10,
    "0K": 0,
    "00JK": -1,
    "-1JK": -1,
    "-1": -1,
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def load_student_data():
    """Pull all active students from Aeries and return a prepared DataFrame."""
    log.info("Loading student data from Aeries database...")
    cnxn = aeries.get_aeries_cnxn()
    sql_obj = core.build_sql_object()
    df = read_sql_query(text(sql_obj.all_students), cnxn)
    df["BD"] = to_datetime(df["BD"], errors="coerce")
    df["fullname"] = df["FN"] + " " + df["LN"]
    df["GR"] = to_numeric(df["GR"], errors="coerce").astype("Int64")
    df["ID"] = to_numeric(df["ID"], errors="coerce").astype("Int64")
    df["NM"] = df["NM"].str.strip()
    log.info(f"  Loaded {len(df):,} student records.")
    return df

# ---------------------------------------------------------------------------
# Core matching logic
# ---------------------------------------------------------------------------

def find_best_match(row, stu_data_df, fuzz_threshold=85):
    """
    Match a single enrollment row against the Aeries student DataFrame.

    Priority order:
      1. Fuzzy name + birthdate
      2. School ID (card number)
      3. Fuzzy name + course location (school name)
      4. No match

    Returns a pd.Series with 'match_type', 'notes', and all columns from stu_data_df.
    """
    # Strategy 1: Fuzzy Name + Birthdate
    if notna(row.get("Contact: Birthdate")):
        bd_matches = stu_data_df[stu_data_df["BD"] == row["Contact: Birthdate"]]
        if not bd_matches.empty:
            result = fuzz_process.extractOne(
                row["fullname"], bd_matches["fullname"], scorer=fuzz.token_set_ratio
            )
            if result and result[1] > fuzz_threshold:
                matched = bd_matches[bd_matches["fullname"] == result[0]].iloc[0]
                return Series(
                    {
                        "match_type": "Fuzzy Name + Birthdate",
                        "notes": f"Name match score: {result[1]}.",
                        **matched,
                    }
                )

    # Strategy 2: School ID (card number stored in 'School ID' column)
    if notna(row.get("School ID")):
        id_matches = stu_data_df[stu_data_df["ID"] == row["School ID"]]
        if not id_matches.empty:
            matched = id_matches.iloc[0]
            name_score = fuzz.token_set_ratio(row["fullname"], matched["fullname"])
            notes = (
                f"Name mismatch (score: {name_score}). "
                f"CSV: '{row['fullname']}', DB: '{matched['fullname']}'."
                if name_score <= fuzz_threshold
                else ""
            )
            return Series({"match_type": "School ID", "notes": notes, **matched})

    # Strategy 3: Fuzzy Name + Course Location
    location = row.get("Course Option Location")
    if notna(location) and str(location) != "nan":
        loc_matches = stu_data_df[stu_data_df["NM"] == location]
        if not loc_matches.empty:
            result = fuzz_process.extractOne(
                row["fullname"], loc_matches["fullname"], scorer=fuzz.token_set_ratio
            )
            if result and result[1] > fuzz_threshold:
                matched = loc_matches[loc_matches["fullname"] == result[0]].iloc[0]
                return Series(
                    {
                        "match_type": "Fuzzy Name + Location",
                        "notes": f"Name match score: {result[1]}.",
                        **matched,
                    }
                )

    # No match
    empty = {col: None for col in stu_data_df.columns}
    return Series({"match_type": "No Match", "notes": "", **empty})


def process_enrollment_file(csv_path: Path, df_stu_data, output_dir: Path):
    """
    Load a single enrollment CSV, run matching, and write the result to output_dir.

    Output filename: matched_<stem>.xlsx  (e.g. matched_enrollment_2025-09-05.xlsx)
    """
    log.info(f"Processing: {csv_path.name}")
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"matched_{csv_path.stem}.xlsx"

    try:
        # Detect encoding and load CSV
        with open(csv_path, "rb") as f:
            enc = chardet.detect(f.read())["encoding"]
        df = read_csv(csv_path, encoding=enc)

        # --- Normalize column names across full vs. delta file layouts ---
        # The original full file uses "School ID" and "Grade";
        # delta files use "Student ID" and sometimes "Grade fx".
        df.rename(
            columns={
                "Student ID": "School ID",  # delta files
                "Grade fx": "Grade",         # delta files (Nov 2025+)
            },
            inplace=True,
        )

        # Prepare enrollment data
        df["fullname"] = (
            df["Contact: First Name"].astype(str) + " " + df["Contact: Last Name"].astype(str)
        ).str.title()
        df["Contact: Birthdate"] = to_datetime(df["Contact: Birthdate"], errors="coerce")
        # School ID column may still be absent in future unknown layouts — handle gracefully
        if "School ID" in df.columns:
            df["School ID"] = to_numeric(df["School ID"], errors="coerce").astype("Int64")
        else:
            log.warning(f"  No 'School ID' / 'Student ID' column found in {csv_path.name}. ID matching will be skipped.")
            df["School ID"] = None
        df["Course Option Location"] = df["Course Option Location"].astype(str).str.strip()
        # Grade column may also be absent
        if "Grade" in df.columns:
            df["GR"] = df["Grade"].astype(str).map(GRADE_MAP)
        else:
            log.warning(f"  No 'Grade' column found in {csv_path.name}.")
            df["GR"] = None
        df["GR"] = to_numeric(df["GR"], errors="coerce").astype("Int64")

        # Match every row
        log.info(f"  Matching {len(df):,} enrollment rows...")
        match_results = df.apply(
            find_best_match, axis=1, stu_data_df=df_stu_data
        )

        # Combine and clean up duplicate column names
        final_df = concat(
            [df.reset_index(drop=True), match_results.reset_index(drop=True)],
            axis=1,
        )
        final_df.rename(
            columns={
                "fullname_x": "csv_fullname",
                "fullname_y": "db_fullname",
                "GR_x": "csv_GR",
                "GR_y": "db_GR",
            },
            inplace=True,
        )

        # Write output
        final_df.to_excel(out_path, index=False)
        matched_count = final_df["match_type"].ne("No Match").sum()
        log.info(
            f"  Done. {matched_count}/{len(final_df)} matched. "
            f"Output: {out_path.name}"
        )

    except Exception as exc:
        log.error(f"  Failed to process {csv_path.name}: {exc}", exc_info=True)


# ---------------------------------------------------------------------------
# Watchdog event handler
# ---------------------------------------------------------------------------

class EnrollmentHandler(FileSystemEventHandler):
    """Handles file-system events in the input directory."""

    def __init__(self, df_stu_data, output_dir: Path):
        super().__init__()
        self.df_stu_data = df_stu_data
        self.output_dir = output_dir

    def _should_process(self, path_str: str) -> bool:
        p = Path(path_str)
        return (
            p.suffix.lower() == ".csv"
            and p.stem.lower().startswith("enrollment")
            and not p.stem.startswith("~")  # skip temp files
        )

    def on_created(self, event):
        if not event.is_directory and self._should_process(event.src_path):
            # Brief pause to let the file finish writing before we open it
            time.sleep(2)
            process_enrollment_file(
                Path(event.src_path), self.df_stu_data, self.output_dir
            )

    def on_moved(self, event):
        # Handles files moved/renamed into the watched directory
        if not event.is_directory and self._should_process(event.dest_path):
            time.sleep(2)
            process_enrollment_file(
                Path(event.dest_path), self.df_stu_data, self.output_dir
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Process BGCSL enrollment CSVs and match against Aeries."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all existing files and exit without watching for new ones.",
    )
    args = parser.parse_args()

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Input  directory : {INPUT_DIR}")
    log.info(f"Output directory : {OUTPUT_DIR}")

    # Load student database once at startup
    df_stu_data = load_student_data()

    # --- Process all existing enrollment files ---
    enrollment_files = sorted(INPUT_DIR.glob("enrollment_*.csv"))
    if enrollment_files:
        log.info(f"Found {len(enrollment_files)} existing enrollment file(s) to process.")
        for csv_path in enrollment_files:
            process_enrollment_file(csv_path, df_stu_data, OUTPUT_DIR)
    else:
        log.info("No existing enrollment files found in input directory.")

    if args.once:
        log.info("--once flag set. Exiting.")
        return

    # --- Watch for new files ---
    log.info(f"Watching for new enrollment files in: {INPUT_DIR}")
    event_handler = EnrollmentHandler(df_stu_data=df_stu_data, output_dir=OUTPUT_DIR)
    observer = Observer()
    observer.schedule(event_handler, str(INPUT_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        log.info("Shutting down file watcher.")
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
