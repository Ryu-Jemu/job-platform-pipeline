"""Build the offline CareerRadar UI from verified report exports.

Run ``python scripts/build_web.py`` after exporting the analysis catalog. This
module never reads the database or changes the source CSVs.
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
LIST_FIELDS = {"sources", "job_groups", "source_links", "skills", "competencies", "also_posted_on", "regions", "career_types"}
NUMBER_FIELDS = {"canonical_id", "n_postings", "n_sources", "career_min_yr", "career_max_yr", "skill_count"}


def _value(key: str, value: str):
    if key in LIST_FIELDS:
        if not value:
            return []
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = ast.literal_eval(value)
        if not isinstance(parsed, list):
            raise ValueError(f"Expected a list in {key}")
        return parsed
    if key == "is_open":
        return {"true": True, "false": False}.get(value.lower())
    if key in NUMBER_FIELDS:
        if not value:
            return None
        number = float(value)
        return int(number) if number.is_integer() else number
    return value or None


def _csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [{key: _value(key, value) for key, value in row.items()}
                for row in csv.DictReader(handle)]


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def build_web(root: str | Path = ROOT) -> dict:
    root = Path(root).resolve()
    reports = root / "reports"
    jobs = _csv(reports / "all_jobs.csv")
    postings = _csv(reports / "all_platform_postings.csv")
    operations = _json(reports / "operations.json")
    validation = _json(reports / "analysis_validation.json")
    ids = {str(job["canonical_id"]) for job in jobs}
    links = [link for job in jobs for link in job.get("source_links", [])]
    assert len(ids) == len(jobs), "Canonical IDs must be unique"
    assert len(links) == len(postings), "Source link count differs from posting export"
    assert all(str(row["canonical_id"]) in ids for row in postings), "Unlinked source posting"
    assert all(urlparse(link["url"]).scheme in {"http", "https"} for link in links), "Unsafe source URL"
    source_ids = {(row["platform"], row["posting_id"]) for row in postings}
    link_ids = {(link["platform"], str(link["posting_id"])) for link in links}
    assert len(source_ids) == len(postings), "Duplicate platform/posting IDs in export"
    assert source_ids == link_ids, "Source links do not cover all source postings"
    source_urls = {(row["platform"], row["posting_id"]): row["url"] for row in postings}
    assert all(source_urls[(link["platform"], str(link["posting_id"]))] == link["url"] for link in links), "Source URL mismatch"
    asof = validation.get("asof") or max((job.get("last_seen_at") or "" for job in jobs), default="")
    metadata = {
        "asof": asof,
        "final_slot": validation.get("final_slot") is True,
        "source_counts": dict(Counter(row["platform"] for row in postings)),
        "operations": operations,
        "report_checks": validation.get("checks", {}),
        "csv_hash": hashlib.sha256((reports / "all_jobs.csv").read_bytes()).hexdigest(),
        "canonical_rows": len(jobs),
        "platform_rows": len(postings),
        "source_links": len(links),
    }
    payload = json.dumps({"jobs": jobs, "postings": postings, "metadata": metadata},
                         ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    template = (root / "web/template.html").read_text(encoding="utf-8")
    styles = (root / "web/styles.css").read_text(encoding="utf-8")
    app = (root / "web/app.js").read_text(encoding="utf-8")
    document = template.replace("<!-- STYLES -->", f"<style>{styles}</style>")
    document = document.replace("<!-- DATA -->", f'<script type="application/json" id="report-data">{payload}</script>')
    document = document.replace("<!-- APP -->", f"<script>{app}</script>")
    (reports / "all_jobs.html").write_text(document, encoding="utf-8")
    (root / "web/index.html").write_text(document, encoding="utf-8")
    result = {key: metadata[key] for key in ["canonical_rows", "platform_rows", "source_links", "asof", "final_slot", "csv_hash"]}
    result["standalone"] = True
    result["external_dependencies"] = 0
    (root / "web/build_manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    print(json.dumps(build_web(parser.parse_args().root), ensure_ascii=False, indent=2))
