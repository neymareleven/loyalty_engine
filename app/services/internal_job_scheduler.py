from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.internal_job import InternalJob
from app.services.internal_job_runner import compute_next_run_at_from_schedule, run_internal_job_once


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    # Keep naive UTC timestamps to match existing DB column types/semantics.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _claim_due_jobs(
    db: Session,
    *,
    now: datetime,
    worker_id: str,
    batch_size: int,
    lock_ttl_seconds: int,
):
    lock_expired_before = now - timedelta(seconds=int(lock_ttl_seconds))

    q = (
        db.query(InternalJob)
        .filter(InternalJob.active.is_(True))
        .filter(InternalJob.schedule.isnot(None))
        .filter(InternalJob.next_run_at.isnot(None))
        .filter(InternalJob.next_run_at <= now)
        .filter(or_(InternalJob.locked_at.is_(None), InternalJob.locked_at < lock_expired_before))
        .order_by(InternalJob.next_run_at.asc())
        .with_for_update(skip_locked=True)
        .limit(batch_size)
    )

    jobs = q.all()
    for job in jobs:
        job.locked_at = now
        job.locked_by = worker_id

    return jobs


def run_scheduler_loop(
    *,
    worker_id: str | None = None,
    batch_size: int = 5,
    lock_ttl_seconds: int = 600,
    idle_sleep_seconds: int = 5,
    max_sleep_seconds: int = 30,
):
    if worker_id is None:
        worker_id = os.getenv("INTERNAL_JOB_WORKER_ID") or os.getenv("HOSTNAME") or "worker"

    logger.info(
        "internal job scheduler started",
        extra={
            "worker_id": worker_id,
            "batch_size": batch_size,
            "lock_ttl_seconds": lock_ttl_seconds,
            "idle_sleep_seconds": idle_sleep_seconds,
            "max_sleep_seconds": max_sleep_seconds,
        },
    )

    while True:
        now = _utcnow()

        db = SessionLocal()
        try:
            jobs = _claim_due_jobs(
                db,
                now=now,
                worker_id=worker_id,
                batch_size=batch_size,
                lock_ttl_seconds=lock_ttl_seconds,
            )
            db.commit()

            if jobs:
                logger.info("claimed due internal jobs", extra={"count": len(jobs), "now": now.isoformat()})

            if not jobs:
                next_due = (
                    db.query(InternalJob.next_run_at)
                    .filter(InternalJob.active.is_(True))
                    .filter(InternalJob.schedule.isnot(None))
                    .filter(InternalJob.next_run_at.isnot(None))
                    .filter(or_(InternalJob.locked_at.is_(None), InternalJob.locked_at < (now - timedelta(seconds=int(lock_ttl_seconds)))))
                    .order_by(InternalJob.next_run_at.asc())
                    .first()
                )

                sleep_for = idle_sleep_seconds
                if next_due and next_due[0]:
                    delta = (next_due[0] - now).total_seconds()
                    if delta > 0:
                        sleep_for = min(max_sleep_seconds, max(1, int(delta)))

                logger.debug(
                    "no due jobs; sleeping",
                    extra={
                        "sleep_for_seconds": sleep_for,
                        "now": now.isoformat(),
                        "next_due": (next_due[0].isoformat() if next_due and next_due[0] else None),
                    },
                )
                time.sleep(sleep_for)
                continue

            for job in jobs:
                run_now = _utcnow()
                try:
                    logger.info(
                        "running internal job",
                        extra={
                            "job_id": str(job.id),
                            "job_key": job.job_key,
                            "brand": job.brand,
                            "run_now": run_now.isoformat(),
                        },
                    )
                    stats = run_internal_job_once(db, job=job, now=run_now)
                    job.last_status = "SUCCESS"
                    job.last_error = None

                    job.last_run_at = run_now
                    job.next_run_at = compute_next_run_at_from_schedule(base_utc=run_now, schedule=job.schedule)

                    logger.info(
                        "internal job success",
                        extra={
                            "job_id": str(job.id),
                            "job_key": job.job_key,
                            "processed": stats.processed,
                            "created": stats.created,
                            "idempotent_existing": stats.idempotent_existing,
                            "failed": stats.failed,
                            "next_run_at": (job.next_run_at.isoformat() if job.next_run_at else None),
                        },
                    )

                except Exception as e:
                    job.last_status = "FAILED"
                    job.last_error = str(e)

                    # On failure, keep moving next_run_at forward to avoid a tight retry loop.
                    job.next_run_at = compute_next_run_at_from_schedule(base_utc=run_now, schedule=job.schedule)

                    logger.exception(
                        "internal job failed",
                        extra={
                            "job_id": str(job.id),
                            "job_key": job.job_key,
                            "next_run_at": (job.next_run_at.isoformat() if job.next_run_at else None),
                        },
                    )

                finally:
                    job.locked_at = None
                    job.locked_by = None
                    db.commit()

        finally:
            db.close()


def main():
    batch_size = int(os.getenv("INTERNAL_JOB_BATCH_SIZE") or "5")
    lock_ttl_seconds = int(os.getenv("INTERNAL_JOB_LOCK_TTL_SECONDS") or "600")
    idle_sleep_seconds = int(os.getenv("INTERNAL_JOB_IDLE_SLEEP_SECONDS") or "5")
    max_sleep_seconds = int(os.getenv("INTERNAL_JOB_MAX_SLEEP_SECONDS") or "30")

    run_scheduler_loop(
        batch_size=batch_size,
        lock_ttl_seconds=lock_ttl_seconds,
        idle_sleep_seconds=idle_sleep_seconds,
        max_sleep_seconds=max_sleep_seconds,
    )


if __name__ == "__main__":
    main()
