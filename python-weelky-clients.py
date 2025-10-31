import pandas as pd
from datetime import datetime
from weeklyreports import load_and_clean_excel, rows_going_live, rows_active

EXCEL_FILE = "Asof_2025-10-30.xlsx"
SHEET_NAME = "OE Counts"

# choose week manually
week_start = datetime(2025, 10, 27).date()
week_end   = datetime(2025, 11, 2).date()


def dedupe_clients(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # assign population priority
    pop_priority = {"Active": 1, "COBRA": 2, "Retiree": 3}
    df = df.copy()
    df["pop_rank"] = df["Population Type"].map(pop_priority).fillna(4)

    df = df.sort_values(["ControlId", "pop_rank"], kind="stable")
    deduped = df.drop_duplicates(subset=["ControlId"], keep="first")

    # restore Excel’s original row order
    deduped = deduped.sort_index(kind="stable")

    return deduped.drop(columns=["pop_rank"], errors="ignore")


df = load_and_clean_excel(EXCEL_FILE, SHEET_NAME)

going = rows_going_live(df, week_start, week_end)
active = rows_active(df, week_start, week_end)

# Remove duplicates while preserving order
going = dedupe_clients(going)
active = dedupe_clients(active)


# Print results

print(f"\n=== CLIENTS GOING LIVE ({len(going)} unique) ===")
print(going[["ControlId","Population Type","Population Size",
             "Total OE Count","Confirmed OE Events","__Start","__End"]]
      .to_string(index=False))

print(f"\n=== CLIENTS ACTIVE ({len(active)} unique) ===")
print(active[["ControlId","Population Type","Population Size",
              "Total OE Count","Confirmed OE Events","__Start","__End"]]
      .to_string(index=False))
