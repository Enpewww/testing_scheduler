import gspread
import time
import hashlib
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os
from gspread_formatting import (
    CellFormat, Color,
    ConditionalFormatRule,
    BooleanRule, BooleanCondition,
    GridRange,
)

# CONFIGURATION

SHEET_URL = "https://docs.google.com/spreadsheets/d/1GiHAMx-K2APmNeMSq6TDgz3s8GdStWFHFYgbK9igFno/edit#gid=0"
CREDENTIALS_FILE = "credentials.json"
DUPLICATE_COLUMN = "Duplicate Data Checker"

SHEET_CONFIG = {
    'BSD': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'TJD': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'BGR': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'DPK': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'KLM': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'KGD': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'BKP': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'BHI': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'BTR': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'TCT': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'PJT': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'SBY': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'SKL': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'CKR': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)'],
    'TBT': ['Kontak Referral (Nama Kids)', 'Kontak Referral (no HP)']
}

# CONNECT TO GOOGLE SHEETS

def connect_to_google_sheet():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(SHEET_URL)
    print(f"Connected to: {spreadsheet.title}")
    return spreadsheet

# HELPER: HASH SHEET CONTENTS

def hash_sheet_data(values):
    return hashlib.md5(str(values).encode()).hexdigest()

# DUPLICATE CHECK FUNCTION

def mark_duplicates_for_sheet(sheet, duplicate_col_name, check_columns):
    data_range = "A:X"
    all_values = sheet.batch_get([data_range])[0]

    if len(all_values) < 2:
        print(f"'{sheet.title}' is empty or header only, skipping.")
        return

    headers = all_values[1]  # start from 2nd row as the column
    data_rows = all_values[2:] # start to get the value from 3rd row as the value

    if not data_rows:
        print(f"'{sheet.title}' has no data rows, skipping.")
        return
    
    max_len = max(len(row) for row in data_rows)
    headers = headers[:max_len]
    headers += [f"Unnamed_{i}" for i in range(len(headers), max_len)]

    df = pd.DataFrame(data_rows, columns=headers)
    df.columns = df.columns.str.strip()

    df = df.dropna(how='all')
    df = df[~(df.apply(lambda x: x.astype(str).str.strip() == '').all(axis=1))]

    target_col = next((col for col in df.columns if "Kontak Referral (Nama Kids)" in col), None)
    if target_col:
        df[target_col] = df[target_col].astype(str).str.capitalize()

    for col in check_columns:
        if col not in df.columns:
            print(f"Missing '{col}' in '{sheet.title}', skipping.")
            return

    # Ensure Duplicate column exists
    if duplicate_col_name not in df.columns:
        df[duplicate_col_name] = ""

    # Clean and mark only non-empty rows
    non_empty_mask = df[check_columns].apply(lambda x: x.str.strip().replace('', pd.NA)).notna().any(axis=1)
    duplicates_mask = df.loc[non_empty_mask].duplicated(subset=check_columns, keep=False)
    df.loc[non_empty_mask, duplicate_col_name] = duplicates_mask.map({True: "Duplicate", False: "Unique"})
    df.loc[~non_empty_mask, duplicate_col_name] = ""

    # Write only the Duplicate column back to sheet (duplicate)
    dup_values = df[duplicate_col_name].fillna("").tolist()
    start_row = 3  # because header is at row 2
    end_row = start_row + len(dup_values) - 1
    dup_col_index = df.columns.get_loc(duplicate_col_name) + 1
    dup_col_letter = gspread.utils.rowcol_to_a1(1, dup_col_index)[:-1]  # extract column letter
    update_range = f"{dup_col_letter}{start_row}:{dup_col_letter}{end_row}"
    sheet.update(update_range, [[v] for v in dup_values])
    print(f"'{sheet.title}' updated ({df[duplicate_col_name].eq('Duplicate').sum()} duplicates).")

    # Create color for the 'duplicate' and 'unique' column
    duplicate_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f"{dup_col_letter}2:{dup_col_letter}{len(df)+1}", sheet)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('TEXT_EQ', ['Duplicate']),
            format=CellFormat(
                textFormat={'bold': True, 'foregroundColor': Color(1, 0, 0)}
            )
        )
    )

    unique_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f"{dup_col_letter}2:{dup_col_letter}{len(df)+1}", sheet)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('TEXT_EQ', ['Unique']),
            format=CellFormat(
                textFormat={'bold': True, 'foregroundColor': Color(0, 0.6, 0)}
            )
        )
    )

    request_body = {
        'requests': [
            {
                'addConditionalFormatRule': {
                    'rule': {
                        'ranges': [duplicate_rule.ranges[0].__dict__],
                        'booleanRule': {
                            'condition': {
                                'type': 'TEXT_EQ',
                                'values': [{'userEnteredValue': 'Duplicate'}]
                            },
                            'format': {
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {'red': 1, 'green': 0, 'blue': 0}
                                }
                            }
                        }
                    },
                    'index': 0
                }
            },
            {
                'addConditionalFormatRule': {
                    'rule': {
                        'ranges': [unique_rule.ranges[0].__dict__],
                        'booleanRule': {
                            'condition': {
                                'type': 'TEXT_EQ',
                                'values': [{'userEnteredValue': 'Unique'}]
                            },
                            'format': {
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {'red': 0, 'green': 0.6, 'blue': 0}
                                }
                            }
                        }
                    },
                    'index': 1
                }
            }
        ]
    }

    sheet.spreadsheet.batch_update(request_body)

# AUTO MONITORING LOOP

def main():
    if os.getenv("GITHUB_ACTIONS"):
        max_cycles = 5
    else:
        max_cycles = float("inf")
    
    spreadsheet = connect_to_google_sheet()

    last_run_rows = {}
    print("\nMonitoring for new entries (real-time)...\n")

    cycle = 0
    while cycle < max_cycles:
        print("\nChecking for updates")
        for sheet_name, check_columns in SHEET_CONFIG.items():
            try:
                sheet = spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                print(f"Skipping, {sheet_name} not found in the sheets")
                continue
            except Exception as e:
                print(f"Error while opening {sheet_name}: {e}")

            try:
                # Batch get configuration
                data_range = "A:X"
                all_values = sheet.batch_get([data_range])[0]
                current_row_count = len(all_values)

                # Update only row count changes
                if last_run_rows.get(sheet) != current_row_count:
                    print(f"Detected change in {sheet_name} (rows: {current_row_count}), still updating...")
                    mark_duplicates_for_sheet(sheet, DUPLICATE_COLUMN, check_columns)
                    last_run_rows[sheet_name] = current_row_count
                else:
                    print(f"No new data in {sheet_name}, skipping sheet")

            except Exception as e:
                print(f"Unexpected error for {sheet_name}: {e}")

            time.sleep(10)

        cycle += 1
        # Loop checks sheet every 5 seconds
        print("\nWaiting 3 seconds before the next batch checking...")
        time.sleep(3)

    print("Finished all scheduled cycles")

if __name__ == "__main__":
    main()