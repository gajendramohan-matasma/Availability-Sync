from datetime import datetime
from typing import Dict, Any, List
import os

from notion_client import Client

from src.logger import logger
from src.load import upsert, build_master_uid_index
from src.calendar_utils import iso_week_from_date_str
from src.config import HOURS_PER_DAY, UNASSIGNED


# ================================================================
# NOTION CLIENT
# ================================================================
def get_client() -> Client:
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise RuntimeError("NOTION_TOKEN is required")
    return Client(auth=token)


# ================================================================
# SAFE PROPERTY EXTRACTORS
# ================================================================
def get_date(prop) -> str | None:
    if not prop or prop.get("type") != "date":
        return None
    return prop.get("date", {}).get("start")


def get_people_ids(prop) -> List[str]:
    if not prop or prop.get("type") != "people":
        return []
    return [p["id"] for p in prop.get("people", [])]


def is_formula_approved(prop) -> bool:
    """
    Authoritative approval gate.

    Only returns True if:
    - Property is a formula
    - Formula evaluates to 'Approved'
    """
    if not prop or prop.get("type") != "formula":
        return False

    formula = prop.get("formula", {})
    ftype = formula.get("type")

    if ftype == "string":
        return formula.get("string") == "Approved"

    if ftype == "select":
        sel = formula.get("select")
        return sel and sel.get("name") == "Approved"

    return False


# ================================================================
# NORMALIZE LEAVE PAGE
# ================================================================
def normalize_leave(page: Dict[str, Any]) -> List[Dict[str, Any]]:
    props = page.get("properties", {})

    # ------------------------------------------------------------
    # DATE (MANDATORY)
    # ------------------------------------------------------------
    start_date = get_date(props.get("Leave Start Date"))
    end_date = get_date(props.get("Leave End Date")) or start_date

    if not start_date:
        logger.error(
            "LEAVE_DROPPED_NO_START_DATE: page=%s",
            page.get("id"),
        )
        return []

    # ------------------------------------------------------------
    # ASSIGNEES
    # ------------------------------------------------------------
    assignees = get_people_ids(props.get("Requestor"))
    if not assignees:
        assignees = [UNASSIGNED]

    # ------------------------------------------------------------
    # EXPAND DAYS → WEEKS
    # ------------------------------------------------------------
    from datetime import date, timedelta

    start = date.fromisoformat(start_date[:10])
    end = date.fromisoformat(end_date[:10])

    rows = []

    curr = start
    while curr <= end:
        week, week_start = iso_week_from_date_str(curr.isoformat())

        for assignee in assignees:
            rows.append({
                "assignee": assignee,
                "week": week,
                "week_start": week_start,
                "customer": "ALL",
                "project": "ALL",
                "workstream": "ALL",
                "metric": "Availability",
                "value": -HOURS_PER_DAY,
                "source_page_id": page["id"],
            })

        curr += timedelta(days=1)

    return rows


# ================================================================
# MAIN SYNC
# ================================================================
def run():
    db_id = os.getenv("AVAILABILITY_DB_ID")
    if not db_id:
        raise RuntimeError("AVAILABILITY_DB_ID must be set")

    notion = get_client()
    uid_index = build_master_uid_index()

    logger.info("Starting availability sync")

    pages = []
    cursor = None

    while True:
        resp = notion.databases.query(
            database_id=db_id,
            start_cursor=cursor,
        )
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break

    logger.info("Fetched %d availability requests", len(pages))

    approved = 0
    skipped = 0

    for page in pages:
        props = page.get("properties", {})

        # --------------------------------------------------------
        # 🔒 APPROVAL GATE (FORMULA-AWARE, AUTHORITATIVE)
        # --------------------------------------------------------
        if not is_formula_approved(props.get("Status")):
            skipped += 1
            logger.debug(
                "LEAVE_SKIPPED_NOT_APPROVED: page=%s",
                page.get("id"),
            )
            continue

        approved += 1

        rows = normalize_leave(page)
        for r in rows:
            upsert(
                assignee=r["assignee"],
                week=r["week"],
                week_start=r["week_start"],
                customer=r["customer"],
                project=r["project"],
                workstream=r["workstream"],
                metric=r["metric"],
                value=r["value"],
                pages=[r["source_page_id"]],
                uid_index=uid_index,
            )

    logger.info(
        "Availability sync complete | approved=%d skipped=%d",
        approved,
        skipped,
    )


# ================================================================
# ENTRYPOINT
# ================================================================
if __name__ == "__main__":
    run()
