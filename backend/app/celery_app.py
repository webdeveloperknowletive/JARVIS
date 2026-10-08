import os
from celery import Celery

redis_url = os.environ.get(
    "REDIS_URL",
    "redis://localhost:6379/0"
)

celery_app = Celery(
    "jarvis_worker",
    broker=redis_url,
    backend=redis_url,
    include=[
        "app.tasks.import_tasks",
        "app.tasks.daily_tasks",
        "app.tasks.billing_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    worker_max_tasks_per_child=100,
)
