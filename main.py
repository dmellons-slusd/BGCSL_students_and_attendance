import datetime
from pandas import DataFrame, Series, concat, notna, read_sql_query, read_csv, to_datetime, to_numeric
from sqlalchemy import create_engine, text
from slusdlib import aeries, core
from thefuzz import fuzz, process
import chardet
from decouple import config


sql = core.build_sql_object()
cnxn = aeries.get_aeries_cnxn() if config('TEST_RUN', default=True, cast=bool) else aeries.get_aeries_cnxn(database=config('TEST_DATABASE'))
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
def match_students() -> DataFrame:
    query = text(sql.all_students)
    df_stu_data = read_sql_query(query, cnxn)
    df_stu_data['BD'] = to_datetime(df_stu_data['BD'], errors='coerce')
    df_stu_data['fullname'] = df_stu_data['FN'] + ' ' + df_stu_data['LN']
    df_stu_data['GR'] = to_numeric(df_stu_data['GR'], errors='coerce').astype('Int64')
    df_stu_data['ID'] = to_numeric(df_stu_data['ID'], errors='coerce').astype('Int64')
    df_stu_data['NM'] = df_stu_data['NM'].str.strip()

    # Load and prepare the CSV data
    infile = './in/enrollment_2025-09-05.csv'
    with open(infile, 'rb') as f:
        result = chardet.detect(f.read())
    df_stusheet = read_csv(infile, encoding=result['encoding'])

    df_stusheet['fullname'] = df_stusheet['Contact: First Name'] + ' ' + df_stusheet['Contact: Last Name']
    df_stusheet['fullname'] = df_stusheet['fullname'].str.title()
    df_stusheet['Contact: Birthdate'] = to_datetime(df_stusheet['Contact: Birthdate'], errors='coerce')
    df_stusheet['School ID'] = to_numeric(df_stusheet['School ID'], errors='coerce').astype('Int64')
    df_stusheet['Course Option Location'] = df_stusheet['Course Option Location'].astype(str).str.strip()


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
        return 1
    else:
        return data['sq'][0]+1
    
def add_program(data:DataFrame, pgm_code:int = 149) -> None:
    for _,row in data.iterrows():
        id = row['ID']
        next_sq = get_next_pgm_sq(id, cnxn)
        start_date = to_datetime(row['Enrollment Start Date'])
        print('start_date:', start_date, type(start_date))
        with cnxn.connect() as conn:
            conn.execute(text(sql.insert_pgm),parameters={
                "id": id,
                "sq": next_sq,
                "pgm_code":pgm_code,
                "start_date": start_date
            })
            conn.commit()
        print(id, next_sq)

def main():
    if not config('TEST_RUN', default=True, cast=bool):
        input("Running in production mode. Press Enter to continue...")
    matched_students = match_students()
    print(matched_students.head())

    print(f"Matched {len(matched_students['ID'].dropna().unique().tolist())} unique student IDs.")
    add_program(matched_students)
    

if __name__ == "__main__":
    main()