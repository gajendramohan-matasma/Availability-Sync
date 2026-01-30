import os
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List

from notion_client import Client

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
SOURCE_DB_ID = os.getenv("SOURCE_DB_ID")
TARGET_DB_ID = os.getenv("TARGET_DB_ID")
LOOKBACK_DAYS = int(os.getenv("SYNC_LOOKBACK_DAYS", "45"))

if not all([NOTION_TOKEN, SOURCE_DB_ID, TARGET_DB_ID]):
    raise RuntimeError("Missing required environment variables")

notion = Client(auth=NOTION_TOKEN)

# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------
def iso_week(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def get_formula_string(prop) -> str | None:
    if not prop or prop.get("type") != "formula":
        return None
    return prop.get("formula", {}).get("string")


def get_date(prop) -> date | None:
    if not prop:
        return None
    d = prop.get("date")
    if not d or not d.get("start"):
        return None
    return date.fromisoformat(d["start"][:10])


def query_all(db_id: str, filter_payload=None) -> List[Dict]:
    results = []
    cursor = None

    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        if filter_payload:
            payload["filter"] = filter_payload

        resp = notion.databases.query(database_id=db_id, **payload)
        results.extend(resp["results"])

        if not resp.get("has_more"):
            break
        cursor = resp["next_cursor"]

    return results

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def run():
    cutoff = date.today() - timedelta(days=LOOKBACK_DAYS)
    logger.info("Syncing approved availability from %s onward", cutoff)

    source_rows = query_all(SOURCE_DB_ID)
    logger.info("Fetched %d source rows", len(source_rows))

    # Build target index
    target_index: Dict[str, str] = {}
    for row in query_all(TARGET_DB_ID):
        key = row.get("properties", {}).get("Sync Key", {}).get("rich_text", [])
        if key:
            target_index[key[0]["plain_text"]] = row["id"]

    created = updated = skipped = 0

    for page in source_rows:
        props = page.get("properties", {})

        # ------------------ APPROVED ONLY ------------------
        status = get_formula_string(props.get("Status"))
        if status != "Approved":
            skipped += 1
            continue

        start = get_date(props.get("Leave Start Date"))
        end = get_date(props.get("Leave End Date"))
        if not start or not end or end < cutoff:
            skipped += 1
            continue

        leave_type = (
            props.get("Leave Type", {})
            .get("select", {})
            .get("name", "Unknown")
        )

        assignees = props.get("Requestor", {}).get("people", [])
        if not assignees:
            skipped += 1
            continue

        for person in assignees:
            week = iso_week(start)
            sync_key = f"{page['id']}|{person['id']}|{week}"

            payload = {
                "Name": {
                    "title": [{
                        "text": {
                            "content": f"Leave | {person['id']} | {week}"
                        }
                    }]
                },
                "Sync Key": {
                    "rich_text": [{"text": {"content": sync_key}}]
                },
                "Assigned To": {
                    "people": [{"id": person["id"]}]
                },
                "ISO Week": {
                    "rich_text": [{"text": {"content": week}}]
                },
                "Leave Start Date": {
                    "date": {"start": start.isoformat()}
                },
                "Leave End Date": {
                    "date": {"start": end.isoformat()}
                },
                "Leave Type": {
                    "select": {"name": leave_type}
                },
                "Client Unavailability": {
                    "checkbox": True
                },
                "Last Synced At": {
                    "date": {"start": datetime.utcnow().isoformat()}
                },
            }

            existing_id = target_index.get(sync_key)

            if existing_id:
                notion.pages.update(page_id=existing_id, properties=payload)
                updated += 1
            else:
                resp = notion.pages.create(
                    parent={"database_id": TARGET_DB_ID},
                    properties=payload,
                )
                target_index[sync_key] = resp["id"]
                created += 1

    logger.info(
        "Availability sync completed | created=%d updated=%d skipped=%d",
        created, updated, skipped
    )


if __name__ == "__main__":
    run()
