import os
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List

from notion_client import Client

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
SOURCE_DB_ID = os.getenv("SOURCE_DB_ID")
TARGET_DB_ID = os.getenv("TARGET_DB_ID")
LOOKBACK_DAYS = int(os.getenv("SYNC_LOOKBACK_DAYS", "45"))

if not NOTION_TOKEN or not SOURCE_DB_ID or not TARGET_DB_ID:
    raise RuntimeError("Missing required environment variables")

notion = Client(auth=NOTION_TOKEN)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
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


def query_all(database_id: str, filter_payload=None) -> List[Dict]:
    results = []
    cursor = None

    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        if filter_payload:
            payload["filter"] = filter_payload

        resp = notion.databases.query(database_id=database_id, **payload)
        results.extend(resp["results"])

        if not resp.get("has_more"):
            break
        cursor = resp["next_cursor"]

    return results


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def run():
    cutoff = date.today() - timedelta(days=LOOKBACK_DAYS)
    logger.info("Syncing approved availability from %s onward", cutoff)

    # --------------------------------------------------------
    # SOURCE QUERY — APPROVED ONLY (FORMULA FILTER)
    # --------------------------------------------------------
    source_filter = {
        "property": "Status",
        "formula": {
            "string": {
                "equals": "Approved"
            }
        }
    }

    source_rows = query_all(SOURCE_DB_ID, source_filter)
    logger.info("Fetched %d APPROVED source rows", len(source_rows))

    # Debug: If no rows, try without filter to check connection
    if len(source_rows) == 0:
        all_rows = query_all(SOURCE_DB_ID)
        logger.warning("DEBUG: No approved rows found. Total rows without filter: %d", len(all_rows))
        if all_rows:
            sample_props = all_rows[0].get("properties", {})
            status_prop = sample_props.get("Status", {})
            logger.warning("DEBUG: Sample Status property: %s", status_prop)

    # --------------------------------------------------------
    # TARGET INDEX (Sync Key → Page ID)
    # --------------------------------------------------------
    target_index: Dict[str, str] = {}

    for row in query_all(TARGET_DB_ID):
        sync_key_prop = (
            row.get("properties", {})
            .get("Sync Key", {})
            .get("rich_text", [])
        )
        if sync_key_prop:
            target_index[sync_key_prop[0]["plain_text"]] = row["id"]

    created = updated = skipped = 0

    # --------------------------------------------------------
    # PROCESS SOURCE ROWS
    # --------------------------------------------------------
    # Debug: Log property names from first row
    if source_rows:
        first_props = source_rows[0].get("properties", {})
        logger.info("DEBUG: All property names: %s", list(first_props.keys()))
        # Log raw date and client unavailability properties to see their structure
        for key in first_props:
            if "date" in key.lower() or "till" in key.lower() or "start" in key.lower() or "end" in key.lower() or "client" in key.lower() or "unavail" in key.lower():
                logger.info("DEBUG: Property '%s' = %s", key, first_props[key])

    for page in source_rows:
        props = page.get("properties", {})

        start_date = get_date(props.get("Leave Start Date"))
        end_date = get_date(props.get("Till Date"))  # Source uses "Till Date", not "Leave End Date"

        if not start_date or not end_date or end_date < cutoff:
            # Only log first few skips to avoid spam
            if skipped < 3:
                logger.info("DEBUG: Skipping page %s - start=%s, end=%s, cutoff=%s",
                            page['id'][:8], start_date, end_date, cutoff)
            skipped += 1
            continue

        leave_type = props.get("Leave Type", {}).get("select", {})
        leave_type_name = leave_type.get("name") if leave_type else None

        # Extract Requestor (person) to map to Assigned To in target
        requestor_prop = props.get("Requestor", {})
        requestor_people = requestor_prop.get("people", []) if requestor_prop else []

        # Extract Client Unavailability from source (it's a formula returning boolean)
        client_unavail_prop = props.get("Client Unavailability", {})
        if client_unavail_prop.get("type") == "formula":
            client_unavailability = client_unavail_prop.get("formula", {}).get("boolean", False)
        else:
            client_unavailability = client_unavail_prop.get("checkbox", False)

        # Calculate ISO Week from start date (format: "2026-W05")
        iso_year, iso_week, _ = start_date.isocalendar()
        iso_week_str = f"{iso_year}-W{iso_week:02d}"

        sync_key = f"{page['id']}|LEAVE"

        payload = {
            "Name": {
                "title": [{
                    "text": {
                        "content": f"Leave | {start_date} → {end_date}"
                    }
                }]
            },
            "Sync Key": {
                "rich_text": [{"text": {"content": sync_key}}]
            },
            "Leave Start Date": {
                "date": {"start": start_date.isoformat()}
            },
            "Leave End Date": {
                "date": {"start": end_date.isoformat()}
            },
            "Leave Type": (
                {"select": {"name": leave_type_name}}
                if leave_type_name
                else None
            ),
            "Client Unavailability": {"checkbox": client_unavailability},
            "ISO Week": {"rich_text": [{"text": {"content": iso_week_str}}]},
            "Assigned To": (
                {"people": [{"id": p["id"]} for p in requestor_people]}
                if requestor_people
                else None
            ),
            "Last Synced At": {
                "date": {"start": datetime.utcnow().isoformat()}
            },
        }

        # Remove None values (Notion API requirement)
        payload = {k: v for k, v in payload.items() if v is not None}

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
        created,
        updated,
        skipped,
    )


if __name__ == "__main__":
    run()
