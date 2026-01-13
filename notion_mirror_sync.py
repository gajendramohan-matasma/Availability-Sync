import os
import re
from notion_client import Client
from notion_client.errors import APIResponseError
from tenacity import retry, wait_exponential, stop_after_attempt

# ========= Config: property names (must match your Notion DBs) =========
PROP_ACTIVITY     = "Activity"     # Title in both
PROP_STATUS       = "Status"       # Status or Select in Master; any in Mirror
PROP_START_DATE   = "Start Date"   # Date
PROP_DUE_DATE     = "Due Date"     # Date
PROP_PRIORITY     = "Priority"     # Select in Master; optional in Mirror
PROP_RAISED_BY    = "Raised By"    # Select in Master
PROP_ASSIGNED_TO  = "Assigned To"  # People in Master

# Map Master Status -> Mirror Status (use this to normalize names)
STATUS_MAP = {
    # "Planned": "To Do",     # <- example mapping; change to your Mirror option
    # "In progress": "In Progress",
    # "WIP": "In Progress",
}

# If a Status value still isn't valid after mapping, optionally fall back:
STATUS_FALLBACK = None  # e.g., "To Do" or None to clear status when unknown

# ========= Env / Client =========
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
RAW_MASTER_DB_ID = os.environ.get("MASTER_DB_ID")
RAW_MIRROR_DB_ID = os.environ.get("MIRROR_DB_ID")

if not NOTION_TOKEN:
    raise RuntimeError("Missing NOTION_TOKEN")
if not RAW_MASTER_DB_ID or not RAW_MIRROR_DB_ID:
    raise RuntimeError("MASTER_DB_ID and/or MIRROR_DB_ID are missing")

notion = Client(auth=NOTION_TOKEN)

# ========= Helpers: IDs, preflight, schema =========
def parse_db_id(val: str) -> str:
    """Accept plain ID or full Notion URL; return hyphenated UUID."""
    if not val:
        return val
    m = re.search(r'([0-9a-f]{32})', val.replace("-", ""), re.I)
    if not m:
        return val  # let Notion error out; at least we tried to parse
    raw = m.group(1).lower()
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

def obf(s: str) -> str:
    return (s[:4] + "..." + s[-4:]) if s and len(s) > 8 else ("set" if s else "EMPTY")

def assert_db_access(db_id: str, label: str):
    """Fail fast if the DB is not accessible to this integration."""
    try:
        info = notion.databases.retrieve(db_id)
        name = "".join(t.get("plain_text", "") for t in info.get("title", []))
        print(f"[OK] {label}: {name!r} ({obf(db_id)})")
    except APIResponseError as e:
        raise RuntimeError(
            f"[ERROR] Cannot access {label} ({obf(db_id)}). "
            f"HTTP {getattr(e, 'status', 'n/a')}: {getattr(e, 'message', str(e))}\n"
            f"Fix:\n"
            f"• Use the DATABASE ID (not a view/page ID). For URLs: use the part before ?v=...\n"
            f"• Share the DB with this integration (Share → Invite → your integration → Can edit)\n"
            f"• Ensure the token’s workspace matches the DB’s workspace"
        ) from e

def get_db_schema_types(db_id: str) -> dict:
    """Return {prop_name: type} for a database."""
    info = notion.databases.retrieve(db_id)
    props = info.get("properties", {})
    return {name: meta.get("type") for name, meta in props.items()}

def get_db_schema_options(db_id: str) -> dict:
    """
    Return {prop_name: set(option_names)} for select/multi_select/status properties.
    For other types, returns empty set.
    """
    info = notion.databases.retrieve(db_id)
    props = info.get("properties", {})
    out = {}
    for name, meta in props.items():
        t = meta.get("type")
        opts = set()
        if t in ("select", "multi_select", "status"):
            for o in meta[t].get("options", []):
                nm = o.get("name")
                if nm:
                    opts.add(nm)
        out[name] = opts
    return out

def coerce_choice_payload(name: str | None, mirror_type: str):
    """
    Build a payload for choice-like properties depending on Mirror type.
    - select       -> {"select": {"name": name}} or {"select": None}
    - multi_select -> {"multi_select": [{"name": name}]} or {"multi_select": []}
    - status       -> {"status": {"name": name}} or {"status": None}
    """
    if mirror_type == "select":
        return {"select": ({"name": name} if name else None)}
    if mirror_type == "multi_select":
        return {"multi_select": ([{"name": name}] if name else [])}
    if mirror_type == "status":
        return {"status": ({"name": name} if name else None)}
    return None  # unsupported type

def read_choice_name(prop):
    """
    Returns the 'name' for select/status values, or None if empty.
    Works for a page property object like:
      {"type": "select", "select": {... or None}}
      {"type": "status", "status": {... or None}}
    """
    if not prop:
        return None
    t = prop.get("type")
    if t in ("select", "status"):
        val = prop.get(t)  # dict or None
        return val.get("name") if isinstance(val, dict) else None
    return None

def read_people_names(prop):
    """Returns list of display names (or IDs) from a People property; [] if empty."""
    if not prop or prop.get("type") != "people":
        return []
    out = []
    for u in prop.get("people", []):
        nm = u.get("name") or u.get("id")
        if nm:
            out.append(nm)
    return out

# ========= Notion API wrappers =========
@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def query_database(db_id, **kwargs):
    return notion.databases.query(database_id=db_id, **kwargs)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def create_page(parent_db_id, properties):
    return notion.pages.create(
        parent={"type": "database_id", "database_id": parent_db_id},
        properties=properties
    )

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def update_page(page_id, properties):
    return notion.pages.update(page_id=page_id, properties=properties)

# ========= Page utilities =========
def get_all_pages(db_id):
    pages, start_cursor = [], None
    while True:
        resp = query_database(db_id, page_size=100, **({"start_cursor": start_cursor} if start_cursor else {}))
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def get_title_text(page, title_prop_name):
    props = page.get("properties", {})
    title_prop = props.get(title_prop_name, {})
    if title_prop.get("type") != "title":
        return ""
    parts = title_prop.get("title", [])
    return "".join([t.get("plain_text", "") for t in parts]).strip()

def build_mirror_index_by_activity(mirror_pages):
    idx = {}
    for p in mirror_pages:
        activity = get_title_text(p, PROP_ACTIVITY)
        if activity:
            idx[activity.lower()] = p["id"]
    return idx

def prop_or_none(page, name):
    return page.get("properties", {}).get(name)

# ========= Property extraction / mapping =========
missing_status_names = set()

def extract_sync_properties_from_master(master_page, mirror_schema_types: dict, mirror_allowed: dict):
    """
    Build properties for MIRROR from MASTER fields, adapting to Mirror schema:
    - Skips properties missing in Mirror
    - Chooses status/select/multi_select based on Mirror types
    - Assigned To: People (Master) -> Select or Multi-select (Mirror)
    - Raised By:  Select (Master)  -> Select or Multi-select (Mirror)
    - For Status: validates against allowed options; maps or falls back/clears
    """
    props = {}

    # STATUS (Master may be select or status) -> Mirror's actual type
    if PROP_STATUS in mirror_schema_types:
        name = read_choice_name(prop_or_none(master_page, PROP_STATUS))
        # Map first (normalize)
        if name in STATUS_MAP:
            name = STATUS_MAP[name]

        mt = mirror_schema_types[PROP_STATUS]
        allowed = mirror_allowed.get(PROP_STATUS, set())

        # If Mirror uses 'status', Notion can't auto-create options → validate
        if mt == "status" and name is not None and name not in allowed:
            # Try configured fallback if valid
            if STATUS_FALLBACK and STATUS_FALLBACK in allowed:
                name = STATUS_FALLBACK
            else:
                # Clear the value (avoid 400) and record the missing name
                missing_status_names.add(name)
                name = None

        payload = coerce_choice_payload(name, mt)
        if payload:
            props[PROP_STATUS] = payload

    # START DATE
    if PROP_START_DATE in mirror_schema_types:
        sd = prop_or_none(master_page, PROP_START_DATE)
        if sd and sd.get("type") == "date" and mirror_schema_types[PROP_START_DATE] == "date":
            props[PROP_START_DATE] = {"date": sd.get("date")}

    # DUE DATE
    if PROP_DUE_DATE in mirror_schema_types:
        dd = prop_or_none(master_page, PROP_DUE_DATE)
        if dd and dd.get("type") == "date" and mirror_schema_types[PROP_DUE_DATE] == "date":
            props[PROP_DUE_DATE] = {"date": dd.get("date")}

    # PRIORITY (Master: select; Mirror: select or multi-select; Mirror may not have it)
    if PROP_PRIORITY in mirror_schema_types:
        name = read_choice_name(prop_or_none(master_page, PROP_PRIORITY))
        mt = mirror_schema_types[PROP_PRIORITY]
        if mt == "select":
            props[PROP_PRIORITY] = {"select": ({"name": name} if name else None)}
        elif mt == "multi_select":
            props[PROP_PRIORITY] = {"multi_select": ([{"name": name}] if name else [])}

    # RAISED BY (Master: select) -> Mirror: select or multi_select
    if PROP_RAISED_BY in mirror_schema_types:
        name = read_choice_name(prop_or_none(master_page, PROP_RAISED_BY))
        mt = mirror_schema_types[PROP_RAISED_BY]
        if mt == "multi_select":
            props[PROP_RAISED_BY] = {"multi_select": ([{"name": name}] if name else [])}
        elif mt == "select":
            props[PROP_RAISED_BY] = {"select": ({"name": name} if name else None)}

    # ASSIGNED TO (Master: people) -> Mirror: select or multi_select
    if PROP_ASSIGNED_TO in mirror_schema_types:
        names = read_people_names(prop_or_none(master_page, PROP_ASSIGNED_TO))
        mt = mirror_schema_types[PROP_ASSIGNED_TO]
        if mt == "multi_select":
            props[PROP_ASSIGNED_TO] = {"multi_select": [{"name": n} for n in names]}
        elif mt == "select":
            first = names[0] if names else None
            props[PROP_ASSIGNED_TO] = {"select": ({"name": first} if first else None)}
        # else: unsupported type -> skip

    return props

def to_title_property(text):
    return {"title": [{"type": "text", "text": {"content": text or ""}}]}

# ========= Main =========
def main():
    master_db = parse_db_id(RAW_MASTER_DB_ID)
    mirror_db = parse_db_id(RAW_MIRROR_DB_ID)

    print(f"Preflight… MASTER_DB_ID={obf(master_db)}  MIRROR_DB_ID={obf(mirror_db)}")
    assert_db_access(master_db, "MASTER_DB")
    assert_db_access(mirror_db, "MIRROR_DB")

    print("Fetching Master and Mirror pages...")
    master_pages = get_all_pages(master_db)
    mirror_pages = get_all_pages(mirror_db)
    mirror_schema_types = get_db_schema_types(mirror_db)
    mirror_allowed = get_db_schema_options(mirror_db)

    mirror_index = build_mirror_index_by_activity(mirror_pages)

    created, updated, skipped = 0, 0, 0

    for mp in master_pages:
        activity = get_title_text(mp, PROP_ACTIVITY)
        if not activity:
            skipped += 1
            continue

        mirror_props = extract_sync_properties_from_master(mp, mirror_schema_types, mirror_allowed)
        mirror_id = mirror_index.get(activity.lower())

        if mirror_id:
            if mirror_props:
                update_page(mirror_id, mirror_props)
            updated += 1
            print(f"Updated: {activity}")
        else:
            new_props = {PROP_ACTIVITY: to_title_property(activity)}
            new_props.update(mirror_props)
            create_page(mirror_db, new_props)
            created += 1
            print(f"Created: {activity}")

    if missing_status_names:
        print("WARNING: These Status values were not present in the Mirror and were cleared or mapped:")
        for s in sorted(missing_status_names):
            print(f"  - {s!r}  (add in Mirror Status options or map via STATUS_MAP/STATUS_FALLBACK)")

    print(f"Done. Created: {created}, Updated: {updated}, Skipped (no title): {skipped}")

if __name__ == "__main__":
    main()
