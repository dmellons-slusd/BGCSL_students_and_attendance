import argparse
import datetime
from pathlib import Path
from pandas import DataFrame, Series, concat, notna, read_sql_query, read_csv, to_datetime, to_numeric
from sqlalchemy import create_engine, text
from slusdlib import aeries, core
from thefuzz import fuzz, process
import chardet
import re
import pandas as pd
from decouple import config


sql = core.build_sql_object()
cnxn = aeries.get_aeries_cnxn(access_level='w') if not config('TEST_RUN', default=True, cast=bool) else aeries.get_aeries_cnxn(database=config('TEST_DATABASE'), access_level='w')
grade_map = {
        '8': 8,
        '3': 3,
        '2': 2,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '1': 1,
        '9': 9,
        '00JK': -1,
        '0K': 0
        }

def match_students(infile: str = None, df_stu_data:DataFrame = None) -> DataFrame:
    df_stu_data = read_sql_query(text(sql.all_students), cnxn) if df_stu_data is None else df_stu_data
    df_stu_data['BD'] = to_datetime(df_stu_data['BD'], errors='coerce')
    df_stu_data['fullname'] = df_stu_data['FN'] + ' ' + df_stu_data['LN']
    df_stu_data['GR'] = to_numeric(df_stu_data['GR'], errors='coerce').astype('Int64')
    df_stu_data['ID'] = to_numeric(df_stu_data['ID'], errors='coerce').astype('Int64')
    df_stu_data['NM'] = df_stu_data['NM'].str.strip()

    # Load and prepare the CSV data
    # If an infile path was provided, use it; otherwise fall back to the original file
    if infile is None:
        infile = './in/enrollment_2025-09-05.csv'
    with open(infile, 'rb') as f:
        result = chardet.detect(f.read())
    df_stusheet = read_csv(infile, encoding=result['encoding'])

    # Normalize common column names so we tolerate slight variations in CSV headers.
    expected_cols = {
        'School ID': r'school\s*id',
        'Grade': r'\bgrade\b',
        'Contact: First Name': r'contact[:\s]*first\s*name',
        'Contact: Last Name': r'contact[:\s]*last\s*name',
        'Contact: Birthdate': r'contact[:\s]*birthdate',
        'Course Option Location': r'course\s*option\s*location',
        'Enrollment Start Date': r'enrollment\s*start\s*date'
    }
    rename_map = {}
    for desired, pattern in expected_cols.items():
        found = next((c for c in df_stusheet.columns if re.search(pattern, c, re.I)), None)
        if found and found != desired:
            rename_map[found] = desired
    if rename_map:
        df_stusheet.rename(columns=rename_map, inplace=True)

    df_stusheet['fullname'] = df_stusheet['Contact: First Name'] + ' ' + df_stusheet['Contact: Last Name']
    df_stusheet['fullname'] = df_stusheet['fullname'].str.title()
    df_stusheet['Contact: Birthdate'] = to_datetime(df_stusheet['Contact: Birthdate'], errors='coerce')


    # Ensure `School ID` exists as Int64 NA column
    if 'School ID' in df_stusheet.columns:
        df_stusheet['School ID'] = to_numeric(df_stusheet['School ID'], errors='coerce').astype('Int64')
    else:
        df_stusheet['School ID'] = Series([None] * len(df_stusheet), dtype='Int64')

    # Ensure `Grade` exists and map to `GR` (safe if missing)
    if 'Grade' in df_stusheet.columns:
        df_stusheet['GR'] = df_stusheet['Grade'].map(grade_map)
        df_stusheet['GR'] = df_stusheet['GR'].astype('Int64')
    else:
        df_stusheet['Grade'] = Series([None] * len(df_stusheet))
        df_stusheet['GR'] = Series([None] * len(df_stusheet), dtype='Int64')

    # Normalize course/location column to string and strip
    if 'Course Option Location' in df_stusheet.columns:
        df_stusheet['Course Option Location'] = df_stusheet['Course Option Location'].astype(str).str.strip()
    else:
        df_stusheet['Course Option Location'] = Series([None] * len(df_stusheet))


    # Load the grade mapping file and apply it

    df_stusheet['GR'] = df_stusheet['Grade'].map(grade_map)
    df_stusheet['GR'] = df_stusheet['GR'].astype('Int64')

    print("Data loaded and prepared successfully.")
    df_stusheet.head()

    # --- Enhanced Matching Logic ---

    def find_best_match(row, stu_data_df, fuzz_threshold=85):
        """
        Attempts to find a match for a student row using a prioritized list of strategies.
        
        Args:
            row (pd.Series): A row from the student enrollment DataFrame.
            stu_data_df (pd.DataFrame): The DataFrame with student data from the database.
            fuzz_threshold (int): The minimum score for a fuzzy name match to be considered valid.
            
        Returns:
            pd.Series: A series containing the match type, notes, and data from the matched record.
        """
        # --- Strategy 1: Fuzzy Name + Birthdate ---
        if notna(row['Contact: Birthdate']):
            possible_matches_bd = stu_data_df[stu_data_df['BD'] == row['Contact: Birthdate']]
            if not possible_matches_bd.empty:
                match = process.extractOne(row['fullname'], possible_matches_bd['fullname'], scorer=fuzz.token_set_ratio)
                if match and match[1] > fuzz_threshold:
                    matched_row = possible_matches_bd[possible_matches_bd['fullname'] == match[0]].iloc[0]
                    notes = f"Name match score: {match[1]}."
                    # Return a series combining match info with the matched student data
                    return Series({'match_type': 'Fuzzy Name + Birthdate', 'notes': notes, **matched_row})

        # --- Strategy 2: School ID ---
        if notna(row['School ID']):
            possible_match_id = stu_data_df[stu_data_df['ID'] == row['School ID']]
            if not possible_match_id.empty:
                matched_row = possible_match_id.iloc[0]
                notes = ""
                name_score = fuzz.token_set_ratio(row['fullname'], matched_row['fullname'])
                if name_score <= fuzz_threshold:
                    notes = f"Name mismatch (score: {name_score}). CSV: '{row['fullname']}', DB: '{matched_row['fullname']}'."
                return Series({'match_type': 'School ID', 'notes': notes, **matched_row})

        # --- Strategy 3: Fuzzy Name + Course Location ---
        if notna(row['Course Option Location']) and row['Course Option Location'] != 'nan':
            possible_matches_loc = stu_data_df[stu_data_df['NM'] == row['Course Option Location']]
            if not possible_matches_loc.empty:
                match = process.extractOne(row['fullname'], possible_matches_loc['fullname'], scorer=fuzz.token_set_ratio)
                if match and match[1] > fuzz_threshold:
                    matched_row = possible_matches_loc[possible_matches_loc['fullname'] == match[0]].iloc[0]
                    notes = f"Name match score: {match[1]}."
                    return Series({'match_type': 'Fuzzy Name + Location', 'notes': notes, **matched_row})

        # --- No Match Found ---
        # Return a series with empty values for all columns from df_stu_data, plus match_type and notes
        empty_data = {col: None for col in stu_data_df.columns}
        return Series({'match_type': 'No Match', 'notes': '', **empty_data})

    # Apply the matching function to each row of the student sheet
    print("Starting matching process...")
    match_results = df_stusheet.apply(find_best_match, axis=1, stu_data_df=df_stu_data)
    print("Matching process complete.")


    # --- Final Merge and Output ---

    # Concatenate the original student sheet with the matching results
    # We reset the index to ensure a clean join
    final_df = concat([df_stusheet.reset_index(drop=True), match_results.reset_index(drop=True)], axis=1)

    # Rename the columns from the database that have conflicting names (like 'fullname' or 'GR')
    final_df.rename(columns={
        'fullname_x': 'csv_fullname',
        'fullname_y': 'db_fullname',
        'GR_x': 'csv_GR',
        'GR_y': 'db_GR'
    }, inplace=True)
    return final_df

def get_next_pgm_sq(id, aeries_cnxn) -> int:

    data = read_sql_query(text(sql.last_pgm_sq), aeries_cnxn,params={"id":id})
    if data.empty:
        core.log(f'No previous request data found for student, returning 1 - {sql}')
        return int(1)
    else:
        return int(data['sq'][0]+1)


def process_enrollment_folder(folder_path: str) -> None:
    p = Path(folder_path)
    if not p.exists():
        core.log(f'Input folder {folder_path} does not exist')
        return
    # Build output dir and processed-file tracking
    out_dir = Path('./out')
    out_dir.mkdir(parents=True, exist_ok=True)
    processed_file = out_dir / 'processed_files.txt'

    # Find enrollment CSVs
    files = sorted([f for f in p.iterdir() if f.is_file() and 'enrollment' in f.name.lower() and f.suffix.lower() in ('.csv',)])
    if not files:
        core.log(f'No enrollment files found in {folder_path}')
        return

    # Load list of already-processed file paths (resolved)
    processed = set()
    if processed_file.exists():
        try:
            processed = set(x.strip() for x in processed_file.read_text(encoding='utf-8').splitlines() if x.strip())
        except Exception:
            processed = set()

    # Filter out files we've already processed
    files = [f for f in files if str(f.resolve()) not in processed]
    if not files:
        core.log(f'No new enrollment files to process in {folder_path}')
        return

    for f in files:
        core.log(f'Processing file {f}')
        try:
            matched = match_students(infile=str(f))

            out_file = out_dir / f'matched_{f.name}'
            try:
                matched.to_csv(out_file, index=False)
                core.log(f'Wrote matched output to {out_file}')
            except Exception as e:
                core.log(f'Failed to write matched output for {f}: {e}')
                # continue to next file without marking as processed
                continue

            print(matched.head())
            print(f"Matched {len(matched['ID'].dropna().unique().tolist())} unique student IDs.")

            # Run batch insert (may raise)
            add_program_batch(matched)

            # If we reach here, mark file as processed
            try:
                with processed_file.open('a', encoding='utf-8') as pf:
                    pf.write(str(f.resolve()) + '\n')
            except Exception as e:
                core.log(f'Failed to record processed file {f}: {e}')

        except Exception as e:
            core.log(f'Error processing file {f}: {e}')
            continue
    
def add_program_batch(data:DataFrame, pgm_code:int = 194) -> None:
    rejected_rows:list = []
    for _,row in data.iterrows():
        id = int(row['ID']) if notna(row['ID']) else None
        if row.empty: continue
        
        if id == None : 
            rejected_rows.append(row.to_dict())
            core.log(f'Student ID is missing or invalid for row: {row.to_dict()}, skipping insertion.')
            continue
        pgm_check = read_sql_query(text(sql.pgm_code_check),cnxn,params={
            "id": id,
            "pgm_code": pgm_code
        }) 
        if not pgm_check.empty:
            core.log(f'Student ID {id} already has program code {pgm_code}, skipping insertion.')
            continue
        next_sq = get_next_pgm_sq(id, cnxn)
        start_date = to_datetime(row.get('Enrollment Start Date'))
        # grade = int(row['GR']) if notna(row['GR']) else None

        # Convert pandas NaT to None and ensure native Python datetime for DB driver
        if pd.isna(start_date):
            start_date_param = None
        else:
            start_date_param = start_date.to_pydatetime() if hasattr(start_date, 'to_pydatetime') else start_date

        id = int(id) if notna(id) else None

        with cnxn.connect() as conn:
            conn.execute(text(sql.insert_pgm),parameters={
                "id": id,
                "sq": next_sq,
                "pgm_code": pgm_code,
                "start_date": start_date_param,
                "pgm_start_date": start_date_param,
                # "grade": grade
            })
            conn.commit()
            core.log(f'Inserted program code {pgm_code} for student ID {id} with sequence {next_sq}.')
    DataFrame(rejected_rows).to_csv('./out/rejected_rows.csv', index=False)
            
def test(data:DataFrame ) -> None:
    for _,row in data.head().iterrows():
        id = int(row['ID']) if notna(row['ID']) else None
        print(id)
        
def parse_args():
    parser = argparse.ArgumentParser(description="Match enrollment CSVs to student database records.")
    parser.add_argument('-A', '--automation', action='store_true',
                        help='run in automation mode; skip interactive prompts (for scheduled tasks)')
    return parser.parse_args()


def main():
    args = parse_args()
    if not config('TEST_RUN', default=True, cast=bool) and not args.automation:
        input("Running in production mode. Press Enter to continue...")

    # Process all enrollment files in the configured input folder
    input_folder = config('INPUT_FOLDER', default=r'C:\sftp_in\bgcsl\input_data')
    process_enrollment_folder(input_folder)
    # test(data=matched_students)
    

if __name__ == "__main__":
    main()