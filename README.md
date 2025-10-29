# weeklyaereports

Weekly OE Client Viewer

This lets you view which clients are going live or active for a specific week based on your weekly OE export.

Overview

Each week, you download an Excel export (for example, Asof_2025-10-26.xlsx).
These scripts read that file and display client data for the week you choose.

You can see:

Clients going live

Clients active

Key fields such as Population Size, Total OE Count, and Confirmed OE Events


Load data
The script reads your Excel file (sheet name OE Counts) and cleans it automatically.

Choose a week
Set your start and end dates in the script (Monday–Sunday):

week_start = datetime(2025, 10, 27).date()
week_end   = datetime(2025, 11, 2).date()


Determine clients

Going Live: Clients whose Window Start from CDR is within the week.

Active: Clients whose window overlaps the week (start <= week_end and end >= week_start).

Remove duplicates
Each client appears only once (by ControlId).
If multiple population types exist, Active is preferred, then COBRA, then Retiree.

Keep Excel order
The results appear in the same order as the Excel file.

Running the Script

Make sure your Excel file (for example Asof_2025-10-26.xlsx) is in the same folder as the scripts.

Open VS Code or a terminal in that folder.

Run:
 python-weekly-clients.py


The terminal will show something like:

=== CLIENTS GOING LIVE (39 unique) ===
 ControlId   Population Type  Population Size  Total OE Count  Confirmed OE Events  __Start      __End
  "clientname"  Active           1200             1180            950                 2025-10-27   2025-10-31
 "clientname"     Active            800              780            620                 2025-10-28   2025-11-02

=== CLIENTS ACTIVE (72 unique) ===
 ControlId   Population Type  Population Size  Total OE Count  Confirmed OE Events  __Start      __End
 "clientname"       Active           5000             4700            3900                2025-10-18   2025-11-01
