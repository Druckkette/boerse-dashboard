
import argparse
import json
import os
import traceback
from datetime import datetime, timezone

import app


def _run_url_from_env():
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return ""




def _parse_utc_timestamp(value: str):
    text = str(value or "").strip()
    if not text:
        return None
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _should_skip_scheduled_refresh(store, min_interval_hours: float):
    if min_interval_hours <= 0:
        return False, ""
    active = app._get_active_refresh_job(store)
    if active:
        return True, "Skip: Es läuft bereits ein aktiver Refresh-Job."

    last_refresh_raw = app._get_cache_metadata(store, "last_refresh_at", "")
    last_refresh_at = _parse_utc_timestamp(last_refresh_raw)
    if last_refresh_at is None:
        return False, ""

    age_hours = (datetime.now(timezone.utc) - last_refresh_at).total_seconds() / 3600.0
    if age_hours < float(min_interval_hours):
        return True, (
            f"Skip: letzter Refresh vor {age_hours:.2f}h (min. Intervall {min_interval_hours:.2f}h)."
        )
    return False, ""

def _mark_running(store, job_id, job_type):
    return app._update_refresh_job(
        store,
        job_id,
        status="running",
        progress=10,
        current_step=f"Worker gestartet: {app._job_type_label(job_type)}",
        message="Der Job läuft jetzt außerhalb von Streamlit auf GitHub Actions.",
        started_at=app._utc_now_str(),
        runner_source=os.environ.get("RUNNER_NAME", "github-actions"),
        run_url=_run_url_from_env(),
    )


def _mark_done(store, job_id, stats):
    return app._update_refresh_job(
        store,
        job_id,
        status="done",
        progress=100,
        current_step="Abgeschlossen",
        message=(stats or {}).get("message") or "Job erfolgreich abgeschlossen.",
        finished_at=app._utc_now_str(),
        result_json=stats or {},
        runner_source=os.environ.get("RUNNER_NAME", "github-actions"),
        run_url=_run_url_from_env(),
    )


def _mark_failed(store, job_id, message, result=None):
    return app._update_refresh_job(
        store,
        job_id,
        status="failed",
        progress=100,
        current_step="Fehlgeschlagen",
        message=message,
        finished_at=app._utc_now_str(),
        result_json=result or {},
        runner_source=os.environ.get("RUNNER_NAME", "github-actions"),
        run_url=_run_url_from_env(),
    )


def main():
    parser = argparse.ArgumentParser(description="Run market refresh jobs outside Streamlit.")
    parser.add_argument("--job-id", default="", help="refresh_jobs.job_id")
    parser.add_argument("--job-type", default="", help="Optional explicit job type")
    parser.add_argument("--requested-by", default="", help="Requester label for scheduled/manual runs")
    parser.add_argument(
        "--create-job-if-missing",
        action="store_true",
        help="Create a queued refresh job when --job-id is not provided.",
    )
    parser.add_argument(
        "--min-interval-hours",
        type=float,
        default=float(os.environ.get("SCHEDULE_MIN_INTERVAL_HOURS", "8")),
        help="Skip scheduled refreshes when the last successful refresh is newer than this interval.",
    )
    args = parser.parse_args()

    store = app._get_price_store()
    app._init_price_cache_db(store)
    job = None
    job_id = (args.job_id or "").strip()
    requested_by = (args.requested_by or "").strip() or "github-actions"
    if job_id:
        job = app._get_refresh_job(store, job_id)
        if not job:
            raise SystemExit(f"Job nicht gefunden: {job_id}")
    elif args.create_job_if_missing:
        inferred_type = (args.job_type or "refresh_universe").strip()
        if inferred_type == "refresh_universe":
            should_skip, reason = _should_skip_scheduled_refresh(store, float(args.min_interval_hours))
            if should_skip:
                print(reason)
                return
        job = app._create_refresh_job(
            store,
            inferred_type,
            requested_by=requested_by,
            payload={"trigger": "github_schedule"},
            trigger_mode="github_schedule",
        )
        job_id = job["job_id"]
        print(f"Created scheduled refresh job: {job_id}")
    else:
        raise SystemExit("--job-id fehlt. Nutze --create-job-if-missing für geplante Läufe.")

    job_type = (args.job_type or job.get("job_type") or "").strip()
    if not job_type:
        _mark_failed(store, job_id, "Job-Typ fehlt.")
        raise SystemExit(2)

    _mark_running(store, job_id, job_type)

    try:
        if job_type == "refresh_universe":
            stats = app.refresh_nyse_price_store()
        elif job_type == "rescue_missing":
            stats = app.rescue_missing_nyse_price_store()
        elif job_type == "auto_remap":
            stats = app.auto_remap_missing_nyse_yahoo()
        else:
            raise ValueError(f"Unbekannter Job-Typ: {job_type}")

        stats = stats or {}
        if stats.get("ok"):
            _mark_done(store, job_id, stats)
            print(json.dumps(stats, ensure_ascii=False))
            return

        message = stats.get("error") or stats.get("message") or "Job lieferte kein OK-Ergebnis."
        _mark_failed(store, job_id, message, stats)
        print(json.dumps(stats, ensure_ascii=False))
        raise SystemExit(1)
    except Exception as exc:
        tb = traceback.format_exc()
        result = {"error": f"{type(exc).__name__}: {exc}", "traceback": tb[-4000:]}
        _mark_failed(store, job_id, result["error"], result)
        raise


if __name__ == "__main__":
    main()
