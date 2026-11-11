# Cloud garment order evidence source

Daily 09:30 Asia/Shanghai. New runtime and timer only; existing forecast/collector schedules remain untouched. Default cloud_job is CHECK_ONLY. Explicit --execute requires a pinned private configuration. No secrets, business inventory, manual decisions or test results belong to this source folder.

## Flow
Existing server Feishu app (process-local token) -> bounded complete directory inventory -> delta source downloads -> pinned parser/date evidence checks -> immutable raw evidence package -> fresh nine-table snapshot -> append-only transaction -> independent read-back -> atomic source cursor.

Manual/material interpretation and evidence-reference tables are protected, never auto-appended. Metadata unchanged is not proof of unchanged content; retain review queues. A pending historical REPARSE queue stays visible and does not rewrite old extraction records. Source retrieval failures stop remaining file retrieval; publication ambiguity remains sticky across later runs.

## Run
Use the approved isolated Python with httpx, python-dotenv, PyMySQL and included unchanged xlrd 2.0.2 code (license included). Private job config provides absolute paths and SHA256 for template, fresh source baselines and code. See scripts/runtime_pipeline/config.example.json for the pipeline contract.

`python -B scripts/cloud_job.py --config /private/job.json`

Only after access and actual first-run read-back validation:

`python -B scripts/cloud_job.py --config /private/job.json --execute`

The two systemd files are deployment candidates, not evidence of enabled scheduling. Use a new independent service; inspect the actual next trigger after enabling.

## Tests and limits
Run tests/test_cloud_read_cli.py, tests/test_cloud_job.py and tests/test_scheduled_publisher.py separately with unittest. Runtime-stage tests live under scripts/runtime_pipeline/tests. All fixtures are synthetic and do not prove live access or a live database commit.

Pinned limits: 80 pages/80 folders/5000 inventory entries; at most15 changed source files and30 metadata/download operations per run;100MiB per downloaded file;25000 total support-table rows. Exceeding a bound stops the run rather than silently truncating. No automatic retries of publication. COMMIT_UNKNOWN requires read-only reconciliation through publisher.adapter.

Original parser and database core bytes are retained; the new parser recognizes explicit compact body dates and preserves filename/body conflicts. Existing historical conflict records require a separately identified append-only reparse migration; the timer does not silently overwrite them.
