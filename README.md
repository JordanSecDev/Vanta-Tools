Device Monitoring – Usage
Prereqs
Python 3.9+
Install deps: pip install requests openpyxl
Config (Workspaces.json)
{
  "workspaces": [
    { "name": "Core", "client_id": "...", "client_secret": "..." }
    // repeat for each workspace
  ]
}
name is used as the column prefix in outputs; client_id/client_secret are the Vanta OAuth client credentials.

Running
From the People Check folder:

python "Device Monitoring.py" --config "Workspaces.json" --page-size 100 --out-prefix "vanta_device_monitoring_report"
Common flags:

--page-size N (max 100; auto-clamped).
--param k=v (repeatable) to pass extra query params to /v1/people (e.g., --param taskTypeMatchesAny=INSTALL_DEVICE_MONITORING).
--email someone@example.com (repeatable) to only include specified emails.
--out-prefix PREFIX to change output file names.
--debug to emit verbose auth + per-person info to stderr.
Outputs
${out-prefix}__raw.csv – one row per person per workspace with task fields.
${out-prefix}__consolidated.csv – consolidated by email with per-workspace status columns.
${out-prefix}.xlsx – Excel with “Raw” and “Consolidated” sheets.
What it collects
From /v1/people (pagination handled), the script extracts:

Basic identity: workspace, personId, emailAddress, name fields.
Employment: status, startDate, endDate.
Device monitoring task details: status, completionDate, dueDate, disabled flag, installed boolean, and daysOverdue (when overdue + dueDate present).
Debugging tips
Run with --debug to see auth success/fail per workspace and per-person task status lines.
If you get zero rows but the API returns data, ensure the response shape includes results.data/results.nodes/results.results or people; the script already handles these variants.
Use --email to narrow the dataset when testing.
Security
Workspaces.json contains secrets; keep it out of version control and restrict access.
