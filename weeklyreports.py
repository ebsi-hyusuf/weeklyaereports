"""
Weekly OE Report Automation Script
Author: Hamza Yusuf
Description:
    Automates the 3-week report (Last, This, Next Week) for client activity and lives counts.
    Reads your weekly Excel export (Asof_YYYY-MM-DD.xlsx) and produces:
        - Clients Going Live
        - Clients Active
        - Clients Completed
        - Lives Active (Not Confirmed)
        - Lives Confirmed & Complete
    Works entirely off ControlId, Start/End Dates, and population logic.
"""

import pandas as pd
from datetime import datetime, timedelta
import os

# === CONFIGURATION ==========================================================
# Automatically detect the most recent Excel file in the same folder as this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

excel_files = [
    os.path.join(SCRIPT_DIR, f)
    for f in os.listdir(SCRIPT_DIR)
    if f.lower().endswith(".xlsx")
]

if not excel_files:
    raise FileNotFoundError("No Excel file (.xlsx) found in this folder!")

# Pick the most recently modified Excel file
EXCEL_FILE = max(excel_files, key=os.path.getmtime)

print(f"\nUsing Excel file: {EXCEL_FILE}\n")
# ============================================================================


def get_week_ranges(base_date=None):
    """Get last, this, and next week (Monday–Sunday) date ranges."""
    today = base_date or datetime.today()
    monday = today - timedelta(days=today.weekday())
    last_week_start = monday - timedelta(days=7)
    this_week_start = monday
    next_week_start = monday + timedelta(days=7)

    def week_range(start):
        return (start, start + timedelta(days=6))

    return {
        "Last Week": week_range(last_week_start),
        "This Week": week_range(this_week_start),
        "Next Week": week_range(next_week_start),
    }


def load_and_clean_excel(path):
    df = pd.read_excel(path, sheet_name="OE Counts")

    # Normalize text and handle missing data
    df.columns = [c.strip() for c in df.columns]
    df.replace("No date configured", pd.NA, inplace=True)

    # Convert to proper data types
    date_cols = ["Window Start from CDR", "Window End from CDR"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["Population Type"] = df["Population Type"].str.strip().fillna("Unknown")
    df["Population Size"] = pd.to_numeric(df["Population Size"], errors="coerce").fillna(0)
    df["Total OE Count"] = pd.to_numeric(df["Total OE Count"], errors="coerce").fillna(0)
    df["Confirmed OE Events"] = pd.to_numeric(df["Confirmed OE Events"], errors="coerce").fillna(0)
    return df


def get_clients_going_live(df, start, end):
    """Unique ControlIds starting within week range."""
    mask = (df["Window Start from CDR"] >= start) & (df["Window Start from CDR"] <= end)
    filtered = df[mask]
    # Keep only Active where duplicate Active/Retiree exist
    filtered = filtered.sort_values(by=["Population Type"], ascending=True)
    filtered = filtered.drop_duplicates(subset=["ControlId"], keep="first")
    return filtered


def get_active_clients(df, start, end):
    """Clients whose window overlaps week."""
    mask = (df["Window Start from CDR"] <= end) & (df["Window End from CDR"] >= start)
    filtered = df[mask]
    filtered = filtered.sort_values(by=["Population Type"], ascending=True)
    filtered = filtered.drop_duplicates(subset=["ControlId"], keep="first")
    return filtered


def get_completed_clients(df, start):
    """Clients whose window ended before current week."""
    mask = df["Window End from CDR"] < start
    filtered = df[mask]
    filtered = filtered.drop_duplicates(subset=["ControlId"])
    return filtered


def calc_lives_active(df):
    """Calculate lives active (Total OE - Confirmed; if OE=0 use Population Size)."""
    def logic(row):
        if row["Total OE Count"] == 0:
            return row["Population Size"]
        return row["Total OE Count"] - row["Confirmed OE Events"]

    return df.apply(logic, axis=1).sum()


def calc_lives_confirmed(df, end):
    """Sum confirmed events since 9/8 up to report end."""
    mask = (df["Window Start from CDR"] >= datetime(end.year, 9, 8)) & (df["Window Start from CDR"] <= end)
    return df.loc[mask, "Confirmed OE Events"].sum()


def generate_summary(df, week_ranges):
    summary = []
    for label, (start, end) in week_ranges.items():
        going_live = get_clients_going_live(df, start, end)
        active = get_active_clients(df, start, end)
        completed = get_completed_clients(df, start)

        lives_active = calc_lives_active(active)
        lives_confirmed = calc_lives_confirmed(active, end)

        summary.append({
            "Week": f"{start.strftime('%m/%d')} - {end.strftime('%m/%d')}",
            "Clients Going Live": len(going_live["ControlId"].unique()),
            "Clients Active": len(active["ControlId"].unique()),
            "Clients Completed": len(completed["ControlId"].unique()),
            "Lives Active (Not Confirmed)": lives_active,
            "Lives Confirmed & Complete": lives_confirmed,
        })

    # Predict next week lives: add this week's lives + next week's populations
    this_lives = summary[1]["Lives Active (Not Confirmed)"]
    next_clients = get_clients_going_live(df, *week_ranges["Next Week"])
    next_pred = this_lives + next_clients["Population Size"].sum()
    summary[2]["Lives Active (Not Confirmed)"] = next_pred

    return pd.DataFrame(summary)


def main():
    print("Loading Excel file...")
    df = load_and_clean_excel(EXCEL_FILE)
    print(f"Loaded {len(df)} rows.\n")

    week_ranges = get_week_ranges()
    summary_df = generate_summary(df, week_ranges)

    # === Terminal Output Only ===
    print("\n=== Weekly OE Summary ===\n")
    print(summary_df.to_string(index=False))
    print("\n(No Excel file exported — terminal output only.)\n")


if __name__ == "__main__":
    main()
