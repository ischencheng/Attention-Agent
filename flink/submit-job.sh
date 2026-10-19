#!/bin/sh
# Re-submit after a JobManager restart; never mistake a sleeping submitter for a job.
set -eu
while true; do
    if curl -fsS http://flink-jobmanager:8081/jobs/overview > /tmp/jobs.json; then
        if ! python3 -c 'import json; import sys; jobs=json.load(open("/tmp/jobs.json"))["jobs"]; sys.exit(0 if any(j["name"] == "source-raw-to-clean-and-embedding" and j["state"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING") for j in jobs) else 1)'; then
            flink run -m flink-jobmanager:8081 -d \
                -py /opt/flink/usrlib/bluesky_flink_job.py \
                --bootstrap kafka1:19092,kafka2:19092,kafka3:19092 \
                --raw-topic source_raw --clean-topic source_clean \
                --startup-mode committed --parallelism 1 || true
        fi
    fi
    sleep 30
done
