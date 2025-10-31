#!/usr/bin/env python3
"""
Weekly OE Report Automation Script (Date-normalized)
Author: Hamza Yusuf
"""

import argparse
import os
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

# ===================== CONFIG =====================

SHEET_NAME = "OE Counts"

START_FALLBACKS = [
    "Window Start from CDR",
    "OE Window Start from CDR",
    "Window Start from SYS",
    "Window Start from System",
    "Window Start (SYS)",
    "Window Start from Config",
    "Window Start (Config)",
    "Config Window Start",
    "Window Start",
]
END_FALLBACKS = [
    "Window End from CDR",
    "OE Window End from CDR",
    "Window End from SYS",
    "Window End from System",
    "Window End (SYS)",
    "Window End from Config",
    "Window End (Config)",
    "Config Window End",
    "Window End",
]

CONTROL_ID_COL = "ControlId"
POP_TYPE_COL   = "Population Type"
POP_SIZE_COL   = "Population Size"
OE_TOTAL_COL   = "Total OE Count"
CONFIRMED_COL  = "Confirmed OE Events"

POP_PRIORITY = ["Active", "COBRA", "Retiree"]
# ==================================================


def find_latest_excel(directory: str) -> str:
    xlsx = [os.path.join(directory, f) for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
    if not xlsx:
        raise FileNotFoundError("No Excel file (.xlsx) found in this folder!")
    return max(xlsx, key=os.path.getmtime)


def parse_args():
    p = argparse.ArgumentParser(description="Weekly OE 3-week report")
    p.add_argument("--excel", help="Path to the OE export (.xlsx). Default: most recent .xlsx in script folder.")
    p.add_argument("--sheet", default=SHEET_NAME, help=f"Worksheet name (default: {SHEET_NAME})")
    p.add_argument("--auto", action="store_true", help="Auto-detect current Monday–Sunday as 'this week'.")
    p.add_argument("--start", help="Week start (YYYY-MM-DD). Must be a Monday.")
    p.add_argument("--end",   help="Week end (YYYY-MM-DD). Must be a Sunday.")
    p.add_argument("--export", help="Optional path to export an Excel summary.")
    p.add_argument("--with-details", action="store_true", help="If exporting, include detail tabs.")
    return p.parse_args()


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def three_week_ranges(this_week_start: date):
    """Return dict of week ranges (start_date, end_date) as DATE objects (Mon..Sun)."""
    last = this_week_start - timedelta(days=7)
    nxt  = this_week_start + timedelta(days=7)
    return {
        "Last Week": (last, last + timedelta(days=6)),
        "This Week": (this_week_start, this_week_start + timedelta(days=6)),
        "Next Week": (nxt,  nxt + timedelta(days=6)),
    }


def normalize_pop_type(val: str) -> str:
    if not isinstance(val, str):
        return "Unknown"
    v = val.strip().lower()
    if "active" in v or v == "act":
        return "Active"
    if "cobra" in v or v == "cob":
        return "COBRA"
    if "ret" in v:  # retiree, ret, retirees
        return "Retiree"
    return val.strip().title()


def coalesce_dates(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    cols_present = [c for c in candidates if c in df.columns]
    if not cols_present:
        return pd.to_datetime(pd.Series([pd.NaT] * len(df)), errors="coerce")
    as_dt = df[cols_present].apply(pd.to_datetime, errors="coerce")
    return as_dt.bfill(axis=1).iloc[:, 0]


def load_and_clean_excel(path: str, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]
    df.replace("No date configured", pd.NA, inplace=True)

    if CONTROL_ID_COL not in df.columns:
        raise KeyError(f"Missing required column: {CONTROL_ID_COL}")

    df[CONTROL_ID_COL] = df[CONTROL_ID_COL].astype(str).str.strip()
    if POP_TYPE_COL in df.columns:
        df[POP_TYPE_COL] = df[POP_TYPE_COL].apply(normalize_pop_type)
    else:
        df[POP_TYPE_COL] = "Unknown"

    for c in (POP_SIZE_COL, OE_TOTAL_COL, CONFIRMED_COL):
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Build Start/End with CDR -> SYS/Config fallback
    df["__Start"] = coalesce_dates(df, START_FALLBACKS)
    df["__End"]   = coalesce_dates(df, END_FALLBACKS)

    # If one bound missing, mirror the other to make a 1-day window
    df.loc[df["__Start"].isna(), "__Start"] = df.loc[df["__Start"].isna(), "__End"]
    df.loc[df["__End"].isna(), "__End"]     = df.loc[df["__End"].isna(), "__Start"]

    # Drop invalid windows
    df = df[df["__End"] >= df["__Start"]].copy()

    # Date-only columns for comparisons (CRITICAL FIX)
    df["__Start_d"] = pd.to_datetime(df["__Start"]).dt.date
    df["__End_d"]   = pd.to_datetime(df["__End"]).dt.date

    return df.reset_index(drop=True)


# ---------- Filters (population-level; use DATE comparisons) ----------

def rows_going_live(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    return df[(df["__Start_d"] >= start_d) & (df["__Start_d"] <= end_d)].copy()

def rows_active(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    # Overlap on dates: start <= week_end AND end >= week_start
    return df[(df["__Start_d"] <= end_d) & (df["__End_d"] >= start_d)].copy()

def rows_completed(df: pd.DataFrame, week_start_d: date) -> pd.DataFrame:
    return df[df["__End_d"] < week_start_d].copy()


# ---------- Client counting (dedupe by ControlId with population priority) ----------

def dedupe_clients_for_count(df: pd.DataFrame) -> pd.DataFrame:
    order = {p: i for i, p in enumerate(POP_PRIORITY)}
    df = df.copy()
    df["__pop_order"] = df[POP_TYPE_COL].map(lambda p: order.get(p, len(order)))
    df.sort_values([CONTROL_ID_COL, "__pop_order"], inplace=True)
    out = df.drop_duplicates(subset=[CONTROL_ID_COL], keep="first")
    return out.drop(columns=["__pop_order"])

def count_unique_clients(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return dedupe_clients_for_count(df)[CONTROL_ID_COL].nunique()


# ---------- Lives calculations (population-level, clamp negatives) ----------

def calc_lives_active(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    oe   = pd.to_numeric(df[OE_TOTAL_COL], errors="coerce").fillna(0)
    conf = pd.to_numeric(df[CONFIRMED_COL], errors="coerce").fillna(0)
    pop  = pd.to_numeric(df[POP_SIZE_COL], errors="coerce").fillna(0)
    lives = np.where(oe > 0, oe - conf, pop)
    return int(np.maximum(lives, 0).sum())


def calc_lives_confirmed_for_active(df_active: pd.DataFrame, week_end_d: date) -> int:
    if df_active.empty:
        return 0
    start_cutoff = date(week_end_d.year, 9, 8)
    mask = (df_active["__Start_d"] >= start_cutoff) & (df_active["__Start_d"] <= week_end_d)
    return int(pd.to_numeric(df_active.loc[mask, CONFIRMED_COL], errors="coerce").fillna(0).sum())


# ---------- Next-week projection helpers ----------

def continuing_next_week(df: pd.DataFrame, next_start_d: date, next_end_d: date) -> pd.DataFrame:
    nxt_active = rows_active(df, next_start_d, next_end_d)
    return nxt_active[nxt_active["__Start_d"] < next_start_d].copy()

def new_go_lives_next_week(df: pd.DataFrame, next_start_d: date, next_end_d: date) -> pd.DataFrame:
    return rows_going_live(df, next_start_d, next_end_d)

def sum_new_client_popsize_with_guard(new_rows: pd.DataFrame) -> int:
    if new_rows.empty:
        return 0
    total = 0
    for cid, g in new_rows.groupby(CONTROL_ID_COL, dropna=False):
        g = g.copy()
        g["__pop_est"] = np.where(
            pd.to_numeric(g[POP_SIZE_COL], errors="coerce").fillna(0) > 0,
            pd.to_numeric(g[POP_SIZE_COL], errors="coerce").fillna(0),
            pd.to_numeric(g[OE_TOTAL_COL], errors="coerce").fillna(0),
        )
        by_type = g.groupby(POP_TYPE_COL)["__pop_est"].sum().to_dict()
        active  = float(by_type.get("Active", 0))
        retiree = float(by_type.get("Retiree", 0))
        cobra   = float(by_type.get("COBRA", 0))
        others  = float(sum(v for k, v in by_type.items() if k not in {"Active", "Retiree", "COBRA"}))
        if active > 0 and retiree > active:
            retiree = active
        total += int(round(active + retiree + cobra + others))
    return total


# ---------- Summary builder ----------

def build_summary(df: pd.DataFrame, ranges: dict[str, tuple[date, date]]) -> tuple[pd.DataFrame, dict]:
    rows = []
    details = {}

    for label, (start_d, end_d) in ranges.items():
        going = rows_going_live(df, start_d, end_d)
        active = rows_active(df, start_d, end_d)
        completed = rows_completed(df, start_d)

        clients_going_cnt = count_unique_clients(going)
        clients_active_cnt = count_unique_clients(active)
        clients_completed_cnt = count_unique_clients(completed)

        lives_active = calc_lives_active(active)
        lives_confirmed = calc_lives_confirmed_for_active(active, end_d)

        rows.append({
            "Week": f"{start_d.strftime('%m/%d')} - {end_d.strftime('%m/%d')}",
            "Clients Going Live": clients_going_cnt,
            "Clients Active": clients_active_cnt,
            "Clients Completed": clients_completed_cnt,
            "Lives Active (Not Confirmed)": lives_active,
            "Lives Confirmed & Complete": lives_confirmed,
        })

        details[label] = {
            "going_live_rows": going.sort_values([CONTROL_ID_COL, POP_TYPE_COL, "__Start"]),
            "active_rows": active.sort_values([CONTROL_ID_COL, POP_TYPE_COL, "__Start"]),
            "completed_rows": completed.sort_values([CONTROL_ID_COL, POP_TYPE_COL, "__End"]),
        }

    # Next-week projection (replace Lives Active for "Next Week")
    nw_start_d, nw_end_d = ranges["Next Week"]
    cont = continuing_next_week(df, nw_start_d, nw_end_d)
    newn = new_go_lives_next_week(df, nw_start_d, nw_end_d)
    projected = calc_lives_active(cont) + sum_new_client_popsize_with_guard(newn)
    rows[2]["Lives Active (Not Confirmed)"] = projected

    return pd.DataFrame(rows), details


def export_summary(path: str, summary_df: pd.DataFrame, details: dict, include_details: bool):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        summary_df.to_excel(xw, index=False, sheet_name="Summary")
        if include_details:
            for label, tabs in details.items():
                safe_label = label.replace(" ", "_")
                tabs["going_live_rows"].to_excel(xw, index=False, sheet_name=f"{safe_label}_Going")
                tabs["active_rows"].to_excel(xw, index=False, sheet_name=f"{safe_label}_Active")
                tabs["completed_rows"].to_excel(xw, index=False, sheet_name=f"{safe_label}_Completed")


def main():
    args = parse_args()

    # Excel path
    if args.excel:
        excel_path = args.excel
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = find_latest_excel(script_dir)

    print(f"\nUsing Excel file: {excel_path}")
    df = load_and_clean_excel(excel_path, args.sheet)
    print(f"Loaded {len(df):,} rows from sheet '{args.sheet}'.\n")

    # Determine week window *as dates*, not datetimes
    if args.auto:
        today_d = datetime.today().date()
        this_monday_d = monday_of(today_d)
        start_d, end_d = this_monday_d, this_monday_d + timedelta(days=6)
    elif args.start and args.end:
        start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_d   = datetime.strptime(args.end, "%Y-%m-%d").date()
        if start_d.weekday() != 0 or end_d.weekday() != 6:
            print("WARNING: Weeks are Monday–Sunday. You passed non-Mon/Sun dates.")
        if end_d < start_d:
            raise ValueError("End date is before start date.")
    else:
        today_d = datetime.today().date()
        this_monday_d = monday_of(today_d)
        start_d, end_d = this_monday_d, this_monday_d + timedelta(days=6)

    ranges = three_week_ranges(start_d)

    # Helpful: echo exact bounds (for sanity)
    for lbl, (s, e) in ranges.items():
        print(f"{lbl}: {s} -> {e}")

    summary_df, details = build_summary(df, ranges)

    print("\n=== Weekly OE Summary ===\n")
    print(summary_df.to_string(index=False))

    if args.export:
        export_summary(args.export, summary_df, details, include_details=args.with_details)
        print(f"\nExported: {args.export}\n")

    


if __name__ == "__main__":
    main()
