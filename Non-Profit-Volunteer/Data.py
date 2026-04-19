from pathlib import Path
import pandas as pd




def get_total_events(data):

    if "Event" not in data.columns:
        raise KeyError("DataFrame is missing an 'Event' column")

    per_event = {}
    order = []
    for event in data["Event"]:
        if pd.isna(event) or event == "":
            continue
        if event not in per_event:
            per_event[event] = 0
            order.append(event)
        per_event[event] += 1

    return {
        "total_events": len(order),
        "event_repository": order,
        "sum_people": [per_event[e] for e in order],
    }


def get_volunteer_count(data):
    """Return the total number of volunteer rows."""
    return len(data)

COLUMN_ALIASES = {
    "Name":   ["name", "full name", "volunteer", "volunteer name"],
    "Email":  ["email", "email address", "e-mail"],
    "Phone":  ["phone", "phone number", "cell", "mobile", "cell phone"],
    "Status": ["status", "attendance", "attended"],
}


def _normalize_columns(df):
    """Rename columns in df to canonical names where we recognize them."""
    rename_map = {}
    for col in df.columns:
        low = str(col).strip().lower()
        for canonical, aliases in COLUMN_ALIASES.items():
            if low in aliases:
                rename_map[col] = canonical
                break
    return df.rename(columns=rename_map)


def _normalize_status(val):
    """Map arbitrary status strings to one of signed_up / showed_up / no_show."""
    if pd.isna(val):
        return "signed_up"
    s = str(val).strip().lower()
    if s in {"showed up", "showed_up", "attended", "present", "x", "yes", "y"}:
        return "showed_up"
    if s in {"no show", "no_show", "absent", "no", "n"}:
        return "no_show"
    return "signed_up"


def _event_name_from_path(path):
    """Derive event name from filename: drop extension, replace underscores with spaces."""
    return Path(path).stem.replace("_", " ").strip()


def _read_one_file(path):
    """Read a single event file into a DataFrame. Supports .csv, .xlsx, .xls."""
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {suffix}")


def consolidate_event_folder(folder_path, recursive=False):
    """
    Read every .csv/.xlsx file in a folder and return one consolidated
    DataFrame of volunteers, with each row tagged by the event (= filename).

    Parameters
    ----------
    folder_path : str or Path
    recursive : bool
        If True, walk subdirectories too.

    Returns
    -------
    pandas.DataFrame with columns: Name, Email, Phone, Event, Status
        Missing columns are filled with empty strings.
        Status is normalized to signed_up / showed_up / no_show.
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise NotADirectoryError(f"{folder_path} is not a directory")

    pattern = "**/*" if recursive else "*"
    files = [p for p in folder.glob(pattern)
             if p.suffix.lower() in (".csv", ".xlsx", ".xls")]

    if not files:
        return pd.DataFrame(columns=["Name", "Email", "Phone", "Event", "Status"])

    frames = []
    errors = []

    for path in files:
        try:
            df = _read_one_file(path)
            df = _normalize_columns(df)
            df["Event"] = _event_name_from_path(path)

            for col in ("Name", "Email", "Phone", "Status"):
                if col not in df.columns:
                    df[col] = ""

            df["Status"] = df["Status"].apply(_normalize_status)
            df = df[["Name", "Email", "Phone", "Event", "Status"]]

            # Drop rows with neither name nor email (usually blank template rows)
            df = df[(df["Name"].astype(str).str.strip() != "") |
                    (df["Email"].astype(str).str.strip() != "")]

            frames.append(df)
        except Exception as e:
            errors.append((str(path), str(e)))

    if errors:
        print(f"Skipped {len(errors)} file(s) with errors:")
        for path, err in errors:
            print(f"  {path}: {err}")

    if not frames:
        return pd.DataFrame(columns=["Name", "Email", "Phone", "Event", "Status"])

    return pd.concat(frames, ignore_index=True)


# =============================================================
# 4. QUICK ANALYSIS HELPERS
# =============================================================

def retention_rate(data):
    """
    Fraction of volunteers (by email) who attended >= 2 events.
    Only counts rows where Status == 'showed_up'.
    """
    if "Email" not in data.columns or "Status" not in data.columns:
        return 0.0
    attended = data[data["Status"] == "showed_up"]
    counts = attended.groupby(attended["Email"].str.lower()).size()
    if len(counts) == 0:
        return 0.0
    return (counts >= 2).sum() / len(counts)


def summary_report(data):
    """Print a one-shot summary of a consolidated DataFrame."""
    summary = get_total_events(data)
    print(f"Total volunteer rows: {get_volunteer_count(data)}")
    print(f"Unique volunteers:    {data['Email'].str.lower().nunique()}")
    print(f"Events:               {summary['total_events']}")
    print(f"Retention (>=2 events): {retention_rate(data):.0%}")
    print()
    print("Per-event volunteer counts:")
    for event, count in zip(summary["event_repository"], summary["sum_people"]):
        print(f"  {event:<40} {count}")


# =============================================================
# USAGE EXAMPLE
# =============================================================
if __name__ == "__main__":
    # Point at your folder of event sign-up sheets
    df = consolidate_event_folder("./events", recursive=False)
    summary_report(df)

    # Save the consolidated roster for upload to Google Sheets
    df.to_csv("consolidated_volunteers.csv", index=False)






