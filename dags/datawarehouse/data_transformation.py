from datetime import timedelta, datetime

def parse_duration(duration_str: str) -> timedelta:
    """Convert ISO 8601 duration string (e.g., PT5M33S) into timedelta."""
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {c: 0 for c in components}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component, 1)
            values[component] = int(value)

    return timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"],
    )


def transform_data(row: dict) -> dict:
    """Transform raw row into the format expected by the core table."""
    # --- Handle duration ---
    duration_td = parse_duration(row.get("Duration", "PT0S"))
    row["Duration"] = (datetime.min + duration_td).time()

    # --- Add video type ---
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    # --- Normalize stats keys ---
    row["Video_Views"] = int(row.get("Video_Views") or row.get("viewCount") or 0)
    row["Likes_Counts"] = int(row.get("Likes_Counts") or row.get("likeCount") or 0)
    row["Comment_Counts"] = int(row.get("Comment_Counts") or row.get("commentCount") or 0)

    return row
