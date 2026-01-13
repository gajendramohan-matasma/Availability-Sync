import os
import re
import hashlib
from datetime import date, datetime, timedelta
from notion_client import Client
from tenacity import retry, wait_exponential, stop_after_attempt

# ================= CONFIG =================
HOURS_PER_DAY = 9

PROP_REQUESTOR = "Requestor"
PROP_ASSIGNED_TO = "Assigned To"

PROP_LEAVE_START = "Leave Start Date"
PROP_LEAVE_END = "Leave End Date"
PROP_LEAVE_TYPE = "Leave Type"
PROP_CLIENT_UNAVAIL = "Client Unavailability"

PROP_PROJECTS = "Projects"
PROP_WORKSTREAMS = "Impacted Workstreams"

PROP_SYNC_KEY = "Sync Key"
PROP_ISO_WEEK = "ISO Week"
PROP_LEAVE_DAYS = "Leave Days"
PROP_LEAVE_HOURS = "Leave Hours"
PROP_LAST_SYNCED = "Last Synced At"

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
SOURCE_DB_ID = os.environ["SOURCE_DB_ID"]
TARGET_DB_ID = os.environ["TARGET_DB_ID"]

notion = Client(auth=NOTION_TOKEN)

# ================= UTILITIES =================
def parse_db_id(val):
    m = re.search(r'([0-9a-f]{32})', val.replace("-", ""), re.I)
    raw = m.group(1)
    return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"

def iso_week(d: date):
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def is_working_day(d: date):
    return d.weekday() < 5

def expand_date_range(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def leave_fraction(leave_type: str):
    return 0.5 if leave_type and "half" in leave_type.lower() else 1.0

def build_sync_key(requestor_ids, start, end, leave_type):
    raw = f"{requestor_ids}|{start}|{end}|{leave_type}"
    return hashlib.sha256(raw.encode()).hexdigest()

# ================= NOTION WRAPPERS =================
@retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
def query_db(db_id, **kwargs):
    return notion.databases.query(database_id=db_id, **kwargs)

@retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
def create_page(db_id, props):
    return notion.pages.create(parent={"database_id": db_id}, properties=props)

@retry(wait=wait_exponential(1, 2, 30), stop=stop_after_attempt(5))
def update_page(page_id, props):
    return notion.pages.update(page_id=page_id, properties=props)

def get_all_pages(db_id, filter=None):
    pages, cursor = [], None
    while True:
        resp = query_db(
            db_id,
            start_cursor=cursor,
            page_size=100,
            **({"filter": filter} if filter else {})
        )
        pages.extend(resp["results"])
        if not resp["has_more"]:
            break
        cursor = resp["next_cursor"]
    return pages

# ================= INDEX TARGET =================
def build_target_index():
    pages = get_all_pages(TARGET_DB_ID)
    idx = {}
    for p in pages:
        key = p["properties"].get(PROP_SYNC_KEY, {}).get("rich_text", [])
        if key:
            idx[key[0]["plain_text"]] = p["id"]
    return idx

def last_sync_time():
    pages = get_all_pages(
        TARGET_DB_ID,
        filter={"property": PROP_LAST_SYNCED, "date": {"is_not_empty": True}}
    )
    if not pages:
        return None
    return max(
        datetime.fromisoformat(
            p["properties"][PROP_LAST_SYNCED]["date"]["start"]
        )
        for p in pages
    )

# ================= MAIN LOGIC =================
def main():
    source_db = parse_db_id(SOURCE_DB_ID)
    target_db = parse_db_id(TARGET_DB_ID)

    since = last_sync_time()
    source_filter = None
    if since:
        source_filter = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": since.isoformat()}
        }

    source_pages = get_all_pages(source_db, filter=source_filter)
    target_index = build_target_index()

    created = updated = 0
    now_iso = datetime.utcnow().isoformat()

    for sp in source_pages:
        p = sp["properties"]

        people = p[PROP_REQUESTOR]["people"]
        if not people:
            continue

        requestor_ids = ",".join(u["id"] for u in people)
        start = date.fromisoformat(p[PROP_LEAVE_START]["date"]["start"])
        end = date.fromisoformat(p[PROP_LEAVE_END]["date"]["start"])
        leave_type = p[PROP_LEAVE_TYPE]["select"]["name"]

        sync_key = build_sync_key(requestor_ids, start, end, leave_type)

        weekly = {}
        frac = leave_fraction(leave_type)

        for d in expand_date_range(start, end):
            if not is_working_day(d):
                continue
            wk = iso_week(d)
            weekly[wk] = weekly.get(wk, 0) + frac

        for wk, days in weekly.items():
            props = {
                PROP_ASSIGNED_TO: {"people": people},
                PROP_LEAVE_START: {"date": {"start": start.isoformat()}},
                PROP_LEAVE_END: {"date": {"start": end.isoformat()}},
                PROP_LEAVE_TYPE: {"select": {"name": leave_type}},
                PROP_CLIENT_UNAVAIL: {"select": p[PROP_CLIENT_UNAVAIL]["select"]},
                PROP_WORKSTREAMS: {
                    "multi_select": p[PROP_PROJECTS].get("multi_select", [])
                },
                PROP_SYNC_KEY: {
                    "rich_text": [{"text": {"content": sync_key + "|" + wk}}]
                },
                PROP_ISO_WEEK: {
                    "rich_text": [{"text": {"content": wk}}]
                },
                PROP_LEAVE_DAYS: {"number": days},
                PROP_LEAVE_HOURS: {"number": days * HOURS_PER_DAY},
                PROP_LAST_SYNCED: {"date": {"start": now_iso}},
            }

            key = sync_key + "|" + wk
            if key in target_index:
                update_page(target_index[key], props)
                updated += 1
            else:
                create_page(target_db, props)
                created += 1

    print(f"Done. Created={created}, Updated={updated}")

if __name__ == "__main__":
    main()
