# Vanta Device Monitoring – People Check

Python tool to pull people from multiple Vanta workspaces and report device-monitoring task status.

## Prerequisites
- Python 3.9+
- Install dependencies:
  ```bash
  pip install requests openpyxl
  ```

## Configuration (`Workspaces.json`)
```json
{
  "workspaces": [
    { "name": "Core", "client_id": "YOUR_ID", "client_secret": "YOUR_SECRET" }
    // repeat per workspace
  ]
}
```
- `name` is used as a column prefix.
- Keep this file out of version control (it contains secrets).

## Running
From the People Check directory:
```bash
python "Device Monitoring.py" --config "Workspaces.json" --page-size 100 --out-prefix "vanta_device_monitoring_report"
```

### Useful flags
- `--page-size N` (max 100; auto-clamped)
- `--param k=v` (repeatable) to pass extra `/v1/people` query params  
  Example: `--param taskTypeMatchesAny=INSTALL_DEVICE_MONITORING`
- `--email someone@example.com` (repeatable) to filter to specific emails
- `--out-prefix PREFIX` to change output filenames
- `--debug` to log auth success/fail and per-person task info to stderr

## Outputs
- `${out-prefix}__raw.csv` — one row per person per workspace
- `${out-prefix}__consolidated.csv` — consolidated by email with per-workspace status columns
- `${out-prefix}.xlsx` — Excel workbook with “Raw” and “Consolidated” sheets

## Data captured
For each person:
- Identity: `workspace`, `personId`, `emailAddress`, name fields
- Employment: `status`, `startDate`, `endDate`
- Device monitoring task: `status`, `completionDate`, `dueDate`, `disabled`, `installed` (bool), `daysOverdue` (when overdue + due date present)

## Debugging tips
- Use `--debug` to see auth outcome per workspace and per-person task lines.
- If you see zero rows, ensure the API response includes `results.data`/`results.nodes`/`results.results` or `people` (these shapes are handled).
- Use `--email` with a known address to test end-to-end quickly.

## Security
`Workspaces.json` contains credentials—do not commit it and limit file permissions.
