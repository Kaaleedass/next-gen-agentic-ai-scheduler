SCHEDULER STUDIO — CREWAI LOG + OUTPUT MONITOR
================================================

IMPORTANT: THE EXISTING LANGGRAPH SCHEDULER IS NOT MODIFIED
------------------------------------------------------------
This package does not replace, merge with, or edit your running rule-based LangGraph scheduler. The scheduler remains responsible for task execution and writing TASK_STATUS/log records. Run only `scheduler_dashboard_api.py` for the dashboard; it imports `crewai_monitor.py` as a normal Python module. Do not run `crewai_monitor.py` separately and do not start a second scheduler process. The dashboard API must point to the same SQLite database.

WHAT CONCEPT IS USED?
---------------------
1. Flask: serves the local dashboard HTML and API on 127.0.0.1:8765.
2. SQLite: the existing scheduler database remains the source of task definitions and TASK_STATUS.
3. GPT-4o mini: interprets natural language and selects an existing task ID; the server validates task IDs and supported actions.
4. CrewAI sequential log/output review: specialist agents analyze inspected log evidence, output metadata/data profiles and prepare concise monitoring reports.
5. Deterministic verdict rules: Python chooses SUCCESS / FAILURE / UNKNOWN. CrewAI may explain the verdict but cannot override it.
6. Background monitor: every MONITOR_INTERVAL_SECONDS (default 25) reloads task records from the same SQLite DB and fingerprints configured logs/output paths. Changed terminal runs and failure markers are analyzed.
7. Voice: browser voices remain available; optional OpenAI Nova uses the /v1/audio/speech endpoint with model tts-1 and voice nova. The cute robot mouth animates while audio plays. Set your Interview Kickstart OPENAI_API_KEY in .env. API usage may incur charges.

OUTPUT PATH CONFIGURATION
-------------------------
When creating/editing a task in the HTML form, set the fields that apply:
- Log file/folder path (optional): e.g. T:\Reports\Logs or T:\Reports\Logs\task.log
- Output file/folder path (optional): e.g. C:\Reports\CC_Level.xlsx or C:\Reports\CC_Level

The output path is stored in TASK_OUTPUT_CONFIG, separately from TASK_MASTER. This avoids changing the existing scheduler's task table layout. The dashboard loads task definitions/status from SQLite on every UI refresh (default every 15 seconds). A refresh does not restart or modify LangGraph.

LOG SUCCESS VALIDATION
----------------------
A TASK_STATUS value of SUCCESS or merely finding a new log file is not enough if a log path is configured. The monitor inspects log contents:
- FAILURE: a terminal failed status or explicit error/exception/traceback/timeout/failure evidence is found. Failure evidence has priority over success markers.
- SUCCESS: there is a readable configured log with an explicit completion-success marker (such as "Report saved successfully" or "Exit code: 0"), no failure evidence, and no configured output-file missing/empty failure.
- UNKNOWN: status says SUCCESS but the log is missing or lacks a clear success marker, or evidence is inconclusive. UNKNOWN is not silently treated as success.
- If a configured output path is missing or empty after a terminal success status, it is reported as FAILURE.

OUTPUT FILE READING + QUESTIONS
-------------------------------
The user can add an optional output file/folder path to every task. The monitor checks newest files in the configured path, including a few nested folders. A natural-language business/data question is matched to the task catalogue. If there is more than one plausible task, the agent asks which task you mean. After a task is selected, the API inspects only that task's configured output files and CrewAI answers from extracted evidence only. If the path is missing, content cannot be read, or the requested information is not available in the inspected file, the answer must explicitly say that the data is not available instead of guessing.

Supported outputs include:
- TXT, LOG, OUT, ERR, MD, CSV, TSV, JSON, JSONL, HTML, XML, YAML/INI, SQL and Python source
- XLSX/XLSM through openpyxl
- Parquet through pandas + pyarrow
- PDF through pypdf (text-extractable PDFs)
- DOCX through python-docx

CSV/TSV, Excel and Parquet files include numeric summaries calculated across the whole readable file when possible, along with a bounded row sample. This supports queries such as record counts and numeric summaries without sending a whole large file to the model. However, a requested detail outside the extracted evidence must be reported as unavailable. MONITOR_MAX_FILE_BYTES defaults to 12 MiB. Larger files are metadata-only unless you raise that limit deliberately.

OFFLINE RULE TESTS
------------------
Run `python test_monitor_rules.py` to test success verification, failure precedence, missing outputs, nested file discovery and CSV whole-file numeric profiles. These tests do not require Flask, CrewAI, an API key or a live scheduler DB.

HOW TO RUN ON WINDOWS
---------------------
1. Use Python 3.12 in the environment where you run this dashboard.
2. Copy .env.example to .env and set OPENAI_API_KEY to your own private API key. Optional: set OPENAI_API_KEY to enable OpenAI Nova voice (OpenAI text-to-speech billing may apply).
3. Ensure DB_PATH in scheduler_dashboard_api.py points to the same SQLite DB as the existing scheduler. Default: C:\Users\Kaale\Desktop\MY_Personal_project\Task_scheduler_agent\TASK_TABLE\scheduler.db
4. Install dependencies:
   python -m pip install -r requirements.txt
5. Run run_dashboard.bat, or run:
   python scheduler_dashboard_api.py
6. Open http://127.0.0.1:8765
7. Keep the existing rule-based LangGraph scheduler running separately. The dashboard queues actions through the shared DB but does not take over task execution.
8. Add the correct log and optional output paths to the relevant tasks. Use the magnifier beside a task for its log/output run review, or Scan Logs & Outputs for a manual scan of all tasks with configured paths.

AUTOMATIC MONITORING + ALERTING
-------------------------------
- The monitor checks all task definitions from SQLite on every poll. It detects changes to task status, recent log files and configured output files, including bounded nested folders.
- The dashboard stores monitor events in CREWAI_MONITOR_EVENTS and shows reports in the Log & Output Analysis panel.
- Newly detected automatic FAILURE events trigger a dashboard notification and can be spoken by the robot when voice is enabled.
- If ALERT_MAIL is configured for a task, the monitor attempts an Outlook desktop email for automatic/startup failures. Outlook must be configured in the logged-in Windows profile.
- Manual Scan Logs & Outputs reviews every task with a configured log and/or output path (may take longer and use API tokens when CrewAI is configured).

SAFETY / INTERPRETATION
-----------------------
- CrewAI has no shell/task-execution tool. It only receives bounded text excerpts/metadata already read by the local monitor.
- Output-data answers are grounded in the selected task's configured output path. Do not configure sensitive files you are not permitted to send to the API.
- Only use trusted paths. The local Flask API binds to 127.0.0.1 and should not be exposed to your LAN/Internet.
- API usage may incur OpenAI charges. Automatic analyses occur when the task/file signature changes, not on every browser refresh.
- Outlook alert sending is optional. Dashboard and voice alerts do not require Outlook.

TROUBLESHOOTING
---------------
- If GPT-4o mini/CrewAI fails, check .env, OPENAI_API_KEY, internet access and the server console.
- If verdict is UNKNOWN while TASK_STATUS says SUCCESS, inspect the configured log path and look for a real completion marker, not just a file-created message.
- If a question says data is unavailable, check that the selected task has the correct output path, that files exist and that the file type is supported.
- If tasks/status do not refresh, confirm the dashboard DB_PATH and existing scheduler's SQLite path are identical; the page reloads task/status data every 15 seconds.
- Open http://127.0.0.1:8765/api/health to check API/database state, whether OpenAI is configured and whether CrewAI is installed.


RUN-AND-ANSWER WORKFLOW (UPDATED)
---------------------------------
- You can ask one combined request such as: “Run IPO_DATA_COLLECTOR and tell me which IPOs have status OPEN.”
- The dashboard asks for one confirmation, queues the task exactly once, waits for a changed terminal scheduler status, reviews the configured log, and then answers from the configured output file.
- It does not ask for a second confirmation during the same workflow. A repeated command for a task that already has a pending workflow resumes monitoring that workflow instead of queueing another run. Pending workflow metadata is kept in this browser's local storage.
- If the run fails, or the log cannot verify success, the dashboard reports the verdict/reason and does not answer from potentially stale output. If the run exceeds the wait window, it reports that the run was queued once and avoids automatically retrying it.
- For a question after a clarification, reply with the exact task name; the API attempts to resolve the previous intent from recent chat history rather than asking the same task-selection question again.

CHAT TABLE FORMATTING
---------------------
The dashboard chat renderer supports standard Markdown tables and plain pipe-delimited output such as `column | column` followed by data rows, even if the model omits the Markdown separator row. Large tables are horizontally scrollable.
