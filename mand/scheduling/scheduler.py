import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from mand.config.settings import settings
from mand.config.logging import configure_logging
from mand.monitoring.metrics import ensure_metrics_server
from mand.scheduling.jobs import job_scrape_ah_nl

def start_scheduler():
    configure_logging()
    ensure_metrics_server()
    logging.getLogger(__name__).info("Starting scheduler")

    sched = BlockingScheduler(timezone=settings.SCHEDULER_TIMEZONE)
    sched.add_job(job_scrape_ah_nl, CronTrigger.from_crontab("7 * * * *"), id="ah_nl_hourly")
    sched.start()
