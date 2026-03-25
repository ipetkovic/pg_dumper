#!/usr/bin/env python3
"""Trigger the backup workflow on GitHub Actions and wait for the result."""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from github import Github

REPO = "ipetkovic/pg_dumper"
WORKFLOW = "backup.yml"


def main():
    parser = argparse.ArgumentParser(description="Trigger backup workflow and wait for result.")
    parser.add_argument("--name", default="default", help="Backup name (default: 'default')")
    args = parser.parse_args()

    token = os.environ.get("MELIORI_ACTION_GH_TOKEN")
    if not token:
        sys.exit("Error: MELIORI_ACTION_GH_TOKEN environment variable not set.")

    g = Github(token)
    repo = g.get_repo(REPO)
    workflow = repo.get_workflow(WORKFLOW)

    before = datetime.now(timezone.utc)
    start = time.monotonic()
    print(f"Triggering {WORKFLOW} with name={args.name}...")
    workflow.create_dispatch("main", inputs={"name": args.name})

    # Wait for the run to appear
    print("Waiting for run to start...")
    run = None
    for _ in range(30):
        time.sleep(1)
        runs = workflow.get_runs()
        for r in runs:
            if r.created_at.replace(tzinfo=timezone.utc) > before:
                run = r
                break
        if run:
            break

    if not run:
        sys.exit("Error: could not find the triggered workflow run.")

    print(f"Run #{run.run_number} started (id={run.id})")

    # Poll until complete
    while run.status != "completed":
        time.sleep(1)
        run = repo.get_workflow_run(run.id)

    elapsed = time.monotonic() - start
    print(f"Result: {run.conclusion} ({elapsed:.1f}s)")
    sys.exit(0 if run.conclusion == "success" else 1)


if __name__ == "__main__":
    main()
