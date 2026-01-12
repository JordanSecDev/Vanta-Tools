#!/usr/bin/env python3
"""
Vanta multi-workspace device-monitoring cross-reference

- Auth per workspace (client_credentials)
- Pull /v1/people with pagination
- Extract tasksSummary.details.installDeviceMonitoring
- Output:
  1) raw_people_device_monitoring.csv
  2) consolidated_by_email.csv
  3) report.xlsx (Raw + Consolidated sheets)

Refs:
- Token: https://api.vanta.com/oauth/token
- People: https://api.vanta.com/v1/people
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from openpyxl import Workbook


TOKEN_URL = "https://api.vanta.com/oauth/token"
PEOPLE_URL = "https://api.vanta.com/v1/people"


@dataclass
class Workspace:
    name: str
    client_id: str
    client_secret: str


def load_workspaces(path: str) -> List[Workspace]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "workspaces" not in data:
        raise ValueError("Config must be JSON object with key 'workspaces' (array).")

    out: List[Workspace] = []
    for w in data["workspaces"]:
        out.append(
            Workspace(
                name=w["name"],
                client_id=w["client_id"],
                client_secret=w["client_secret"],
            )
        )
    return out


def get_token(ws: Workspace, scope: str = "vanta-api.all:read") -> str:
    payload = {
        "client_id": ws.client_id,
        "client_secret": ws.client_secret,
        "scope": scope,
        "grant_type": "client_credentials",
    }
    r = requests.post(TOKEN_URL, json=payload, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(
            f"[{ws.name}] Token request failed: {r.status_code} {r.text}"
        )
    return r.json()["access_token"]


def http_get(url: str, token: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GET failed: {r.status_code} {r.text}")
    return r.json()


def iter_people(token: str, page_size: int, extra_params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Iterates through /v1/people using pageSize/pageCursor.
    Tries to handle both likely response shapes:
      A) {"results": [...], "pageInfo": {...}}
      B) {"results": {"nodes": [...], "pageInfo": {...}}}
      C) {"people": [...], "pageInfo": {...}}  (fallback)
    """
    cursor: Optional[str] = None

    while True:
        params = dict(extra_params)
        params["pageSize"] = page_size
        if cursor:
            params["pageCursor"] = cursor

        data = http_get(PEOPLE_URL, token, params=params)

        # Best-effort parsing for slightly different shapes:
        nodes = None
        page_info = None

        if isinstance(data.get("results"), list):
            nodes = data["results"]
            page_info = data.get("pageInfo") or data.get("resultsPageInfo") or {}
        elif isinstance(data.get("results"), dict):
            nodes = (
                data["results"].get("nodes")
                or data["results"].get("results")
                or data["results"].get("data")
            )
            page_info = data["results"].get("pageInfo") or {}
        elif isinstance(data.get("people"), list):
            nodes = data["people"]
            page_info = data.get("pageInfo") or {}

        if not nodes:
            break

        for p in nodes:
            yield p

        # Pagination
        has_next = False
        end_cursor = None

        if isinstance(page_info, dict):
            has_next = bool(page_info.get("hasNextPage"))
            end_cursor = page_info.get("endCursor")

        # Some APIs use nextPageCursor instead
        if not has_next and data.get("nextPageCursor"):
            has_next = True
            end_cursor = data["nextPageCursor"]

        if not has_next or not end_cursor:
            break

        cursor = end_cursor


def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


from datetime import datetime, timezone

def parse_iso_z(s: str):
    # Handles "2025-07-02T02:46:59.919Z"
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def extract_install_device_monitoring(person: Dict[str, Any]) -> Dict[str, Any]:
    task = safe_get(person, ["tasksSummary", "details", "installDeviceMonitoring"], {}) or {}

    status = task.get("status")
    completion = task.get("completionDate")
    due = task.get("dueDate")

    installed = (status == "COMPLETE")

    # days overdue (only when overdue + dueDate exists)
    days_overdue = None
    if status == "OVERDUE" and due:
        due_dt = parse_iso_z(due)
        if due_dt:
            now = datetime.now(timezone.utc)
            days_overdue = (now - due_dt).days

    return {
        "installDeviceMonitoring_status": status,
        "installDeviceMonitoring_completionDate": completion,
        "installDeviceMonitoring_dueDate": due,
        "installDeviceMonitoring_disabled": task.get("disabled"),
        "installDeviceMonitoring_installed": installed,
        "installDeviceMonitoring_daysOverdue": days_overdue,
    }


def normalise_person_row(ws_name: str, person: Dict[str, Any]) -> Dict[str, Any]:
    name_display = safe_get(person, ["name", "display"])
    first = safe_get(person, ["name", "first"])
    last = safe_get(person, ["name", "last"])
    email = person.get("emailAddress")

    employment_status = safe_get(person, ["employment", "status"])
    employment_start = safe_get(person, ["employment", "startDate"])
    employment_end = safe_get(person, ["employment", "endDate"])

    row = {
        "workspace": ws_name,
        "personId": person.get("id"),
        "emailAddress": email,
        "name_display": name_display,
        "name_first": first,
        "name_last": last,
        "employment_status": employment_status,
        "employment_startDate": employment_start,
        "employment_endDate": employment_end,
    }
    row.update(extract_install_device_monitoring(person))
    return row


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def write_xlsx(path: str, raw_rows: List[Dict[str, Any]], raw_fields: List[str],
               consolidated_rows: List[Dict[str, Any]], consolidated_fields: List[str]) -> None:
    wb = Workbook()

    ws_raw = wb.active
    ws_raw.title = "Raw"
    ws_raw.append(raw_fields)
    for r in raw_rows:
        ws_raw.append([r.get(k) for k in raw_fields])

    ws_con = wb.create_sheet("Consolidated")
    ws_con.append(consolidated_fields)
    for r in consolidated_rows:
        ws_con.append([r.get(k) for k in consolidated_fields])

    wb.save(path)


def consolidate_by_email(raw_rows: List[Dict[str, Any]], workspace_names: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Returns consolidated rows keyed by email, with per-workspace status columns.
    """
    by_email: Dict[str, Dict[str, Any]] = {}

    for r in raw_rows:
        email = (r.get("emailAddress") or "").strip().lower()
        if not email:
            continue

        if email not in by_email:
            by_email[email] = {
                "emailAddress": email,
                "name_display": r.get("name_display"),
                "employment_status_any": r.get("employment_status"),
            }

        ws = r["workspace"]
        by_email[email][f"{ws}__installDeviceMonitoring_status"] = r.get("installDeviceMonitoring_status")
        by_email[email][f"{ws}__installDeviceMonitoring_completionDate"] = r.get("installDeviceMonitoring_completionDate")

    # Build field list
    fields = ["emailAddress", "name_display", "employment_status_any"]
    for ws in workspace_names:
        fields.extend([
            f"{ws}__installDeviceMonitoring_status",
            f"{ws}__installDeviceMonitoring_completionDate",
        ])

    rows = list(by_email.values())
    rows.sort(key=lambda x: x["emailAddress"])
    return rows, fields


def parse_kv_params(kvs: List[str]) -> Dict[str, Any]:
    """
    Allows passing query params like:
      --param taskTypeMatchesAny=INSTALL_DEVICE_MONITORING --param taskStatusMatchesAny=OVERDUE
    Repeatable.
    """
    out: Dict[str, Any] = {}
    multi_keys = {"taskTypeMatchesAny", "taskStatusMatchesAny"}  # known repeatables

    for item in kvs:
        if "=" not in item:
            raise ValueError(f"Invalid param format (expected k=v): {item}")
        k, v = item.split("=", 1)

        if k in multi_keys:
            out.setdefault(k, [])
            out[k].append(v)
        else:
            out[k] = v

    # Requests will encode list params as repeated keys automatically
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to JSON config containing workspaces")
    ap.add_argument("--page-size", type=int, default=100, help="People page size")
    ap.add_argument("--param", action="append", default=[], help="Extra query param(s) for /v1/people, as k=v (repeatable)")
    ap.add_argument("--email", action="append", default=[], help="Only include these email(s) (repeatable). If omitted, includes all.")
    ap.add_argument("--out-prefix", default="vanta_device_monitoring_report", help="Output file prefix")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug logging to stderr")

    args = ap.parse_args()
    if args.page_size > 100:
        print(
            f"page-size {args.page_size} too high; clamping to 100",
            file=sys.stderr
        )
        args.page_size = 100

    workspaces = load_workspaces(args.config)
    ws_names = [w.name for w in workspaces]

    extra_params = parse_kv_params(args.param)

    # Optional email filter set
    email_filter = {e.strip().lower() for e in args.email if e.strip()}

    raw_rows: List[Dict[str, Any]] = []

    def debug_log(msg: str) -> None:
        if args.debug:
            print(msg, file=sys.stderr)

    for ws in workspaces:
        print(f"[{ws.name}] Authenticating…", file=sys.stderr)
        try:
            token = get_token(ws, scope="vanta-api.all:read")
            debug_log(f"[{ws.name}] Authentication succeeded.")
        except Exception as e:  # keep going on per-workspace auth failures
            print(f"[{ws.name}] Authentication FAILED: {e}", file=sys.stderr)
            continue

        print(f"[{ws.name}] Fetching people…", file=sys.stderr)
        count = 0
        for p in iter_people(token, page_size=args.page_size, extra_params=extra_params):
            row = normalise_person_row(ws.name, p)
            debug_log(
                f"[{ws.name}] email={row.get('emailAddress')} "
                f"installDeviceMonitoring_status={row.get('installDeviceMonitoring_status')} "
                f"dueDate={row.get('installDeviceMonitoring_dueDate')}"
            )
            if email_filter:
                email = (row.get("emailAddress") or "").strip().lower()
                if email not in email_filter:
                    continue

            raw_rows.append(row)
            count += 1

        print(f"[{ws.name}] Collected {count} people rows.", file=sys.stderr)

        # Small delay to be polite with rate limits (tune as needed)
        time.sleep(0.2)

    # Output raw
    raw_fields = [
        "workspace", "personId", "emailAddress",
        "name_display", "name_first", "name_last",
        "employment_status", "employment_startDate", "employment_endDate",
        "installDeviceMonitoring_status", "installDeviceMonitoring_completionDate",
        "installDeviceMonitoring_dueDate", "installDeviceMonitoring_disabled",
        "installDeviceMonitoring_installed", "installDeviceMonitoring_daysOverdue",
    ]

    out_raw_csv = f"{args.out_prefix}__raw.csv"
    write_csv(out_raw_csv, raw_rows, raw_fields)

    # Consolidated
    consolidated_rows, consolidated_fields = consolidate_by_email(raw_rows, ws_names)
    out_con_csv = f"{args.out_prefix}__consolidated.csv"
    write_csv(out_con_csv, consolidated_rows, consolidated_fields)

    # XLSX
    out_xlsx = f"{args.out_prefix}.xlsx"
    write_xlsx(out_xlsx, raw_rows, raw_fields, consolidated_rows, consolidated_fields)

    print(f"Done.\n- {out_raw_csv}\n- {out_con_csv}\n- {out_xlsx}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
