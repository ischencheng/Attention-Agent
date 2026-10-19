"""Check that the grader accepts the reference and rejects known bad implementations."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from grade import grade

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

MUTATIONS = {
    "no_snapshot": (
        "skills/db_write.py",
        '\n_original_prepare = prepare_payload\n'
        'def prepare_payload(topic, payload):\n'
        '    prepared = _original_prepare(topic, payload)\n'
        '    prepared.pop("evidence_snapshot", None)\n'
        '    return prepared\n',
        ["original_preservation"],
    ),
    "fail_open": (
        "skills/db_write.py",
        '\n_original_prepare = prepare_payload\n'
        'def prepare_payload(topic, payload):\n'
        '    try:\n'
        '        return _original_prepare(topic, payload)\n'
        '    except Exception:\n'
        '        return dict(payload)\n',
        ["publish_fail_closed"],
    ),
    "everything_verified": (
        "skills/evidence.py",
        '\n_original_verify = verify_evidence\n'
        'def verify_evidence(conn, decision_id, payload):\n'
        '    result = _original_verify(conn, decision_id, payload)\n'
        '    result["status"] = "verified"\n'
        '    result["errors"] = []\n'
        '    return result\n',
        ["api_validation"],
    ),
    "read_committed": (
        "skills/db_write.py", None, ["consistent_snapshot"],
    ),
    "dropped_decision_fields": (
        "skills/db_write.py",
        '\n_original_prepare = prepare_payload\n'
        'def prepare_payload(topic, payload):\n'
        '    result = _original_prepare(topic, payload)\n'
        '    return {key: result[key] for key in ("related_annotations", "evidence_snapshot")}\n',
        ["original_preservation"],
    ),
    "non_idempotent_migration": (
        "postgres/init/002_evidence.sql",
        '\nCREATE TABLE one_time_migration_probe (id integer);\n',
        ["immutable_storage"],
    ),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=1729)
    args = parser.parse_args()
    args.report_dir.mkdir(parents=True, exist_ok=True)
    summary = []

    def save(name, report, expected_success):
        (args.report_dir / f"{name}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        matches = report["success"] == expected_success
        summary.append({"name": name, "score": report["score"], "success": report["success"],
                        "expected_success": expected_success, "expectation_met": matches,
                        "full_suite": report["full_suite"],
                        "failed_groups": [g["name"] for g in report["groups"] if not g["passed"]]})
        print(f"{name}: score={report['score']} success={report['success']} expected={expected_success}", flush=True)

    save("reference", grade(ROOT, args.seed), True)
    with tempfile.TemporaryDirectory(prefix="attention-selfcheck-") as directory:
        work = Path(directory)
        subprocess.run([sys.executable, str(HERE / "prepare.py"), str(work / "baseline")], check=True)
        save("baseline", grade(work / "baseline", args.seed), False)
        for name, (filename, addition, selected) in MUTATIONS.items():
            mutant = work / name
            for folder in ("skills", "backend-fastapi", "postgres/init"):
                shutil.copytree(ROOT / folder, mutant / folder, ignore=shutil.ignore_patterns("__pycache__"))
            path = mutant / filename
            text = path.read_text()
            if name == "read_committed":
                assert 'isolation_level="REPEATABLE READ"' in text, "Mutation no longer matches reference"
                text = text.replace('isolation_level="REPEATABLE READ"', 'isolation_level="READ COMMITTED"')
            else:
                text += addition
            path.write_text(text)
            save(name, grade(mutant, args.seed, selected), False)
    output = {"checks": summary, "passed": all(row["expectation_met"] for row in summary),
              "note": "Reference/seeded defect checks, not blinded model attempts or an adversarial sandbox audit."}
    (args.report_dir / "summary.json").write_text(json.dumps(output, indent=2) + "\n")
    return 0 if output["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
