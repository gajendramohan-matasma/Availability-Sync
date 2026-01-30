import os
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List

from notion_client import Client

# ------------------------------------------------------------------
# LOGGING (NO src DEPENDENCY)
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
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"


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
    logger.info("Syncing availability from %s onward", cutoff)

    source_rows = query_all(SOURCE_DB_ID)
    logger.info("Fetched %d source rows", len(source_rows))

    target_index: Dict[str, str] = {}
    for row in query_all(TARGET_DB_ID):
        uid = (
            row.get("properties", {})
            .get("Source UID", {})
            .get("rich_text", [])
        )
        if uid:
            target_index[uid[0]["plain_text"]] = row["id"]

    created = updated = skipped = 0

    for page in source_rows:
        props = page.get("properties", {})

        # ----------------------------------------------------------
        # FILTER: ONLY APPROVED (FORMULA FIELD)
        # ----------------------------------------------------------
        status = get_formula_string(props.get("Status"))
        if status != "Approved":
            skipped += 1
            continue

        start_date = get_date(props.get("Leave Start Date"))
        end_date = get_date(props.get("Leave End Date"))

        if not start_date or not end_date or end_date < cutoff:
            skipped += 1
            continue

        assignees = props.get("Requestor", {}).get("people", [])
        if not assignees:
            skipped += 1
            continue

        for person in assignees:
            for offset in range((end_date - start_date).days + 1):
                d = start_date + timedelta(days=offset)
                if d < cutoff:
                    continue

                week = iso_week(d)
                source_uid = f"{person['id']}|{week}|Availability"

                payload = {
                    "Name": {
                        "title": [{
                            "text": {
                                "content": f"Availability | {person['id']} | {week}"
                            }
                        }]
                    },
                    "Source UID": {
                        "rich_text": [{"text": {"content": source_uid}}]
                    },
                    "Assigned To": {"people": [{"id": person["id"]}]},
                    "Calendar Week": {
                        "rich_text": [{"text": {"content": week}}]
                    },
                    "Week Start": {"date": {"start": d.isoformat()}},
                    "Metric Type": {"select": {"name": "Availability"}},
                    "Value": {"number": 0},
                    "Last Synced At": {
                        "date": {"start": datetime.utcnow().isoformat()}
                    },
                }

                existing_id = target_index.get(source_uid)

                if existing_id:
                    notion.pages.update(page_id=existing_id, properties=payload)
                    updated += 1
                else:
                    resp = notion.pages.create(
                        parent={"database_id": TARGET_DB_ID},
                        properties=payload,
                    )
                    target_index[source_uid] = resp["id"]
                    created += 1

    logger.info(
        "Availability sync completed | created=%d updated=%d skipped=%d",
        created,
        updated,
        skipped,
    )


if __name__ == "__main__":
    run()
