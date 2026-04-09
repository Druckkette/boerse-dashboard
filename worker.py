
import argparse
import json
import os
import traceback

import app


def _run_url_from_env():
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return ""


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
    parser.add_argument("--job-id", required=True, help="refresh_jobs.job_id")
    parser.add_argument("--job-type", default="", help="Optional explicit job type")
    args = parser.parse_args()

    store = app._get_price_store()
    app._init_price_cache_db(store)
    job = app._get_refresh_job(store, args.job_id)
    if not job:
        raise SystemExit(f"Job nicht gefunden: {args.job_id}")

    job_type = (args.job_type or job.get("job_type") or "").strip()
    if not job_type:
        _mark_failed(store, args.job_id, "Job-Typ fehlt.")
        raise SystemExit(2)

    _mark_running(store, args.job_id, job_type)

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
            _mark_done(store, args.job_id, stats)
            print(json.dumps(stats, ensure_ascii=False))
            return

        message = stats.get("error") or stats.get("message") or "Job lieferte kein OK-Ergebnis."
        _mark_failed(store, args.job_id, message, stats)
        print(json.dumps(stats, ensure_ascii=False))
        raise SystemExit(1)
    except Exception as exc:
        tb = traceback.format_exc()
        result = {"error": f"{type(exc).__name__}: {exc}", "traceback": tb[-4000:]}
        _mark_failed(store, args.job_id, result["error"], result)
        raise


if __name__ == "__main__":
    main()
