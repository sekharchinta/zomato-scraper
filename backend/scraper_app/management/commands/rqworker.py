"""
Custom rqworker management command that defaults queue to 'zomato'.
Prevents KeyError: 'default' when run without queue arguments.
"""

from typing import Any
from django.conf import settings
from config import SINGLE_QUEUE_NAME
from django_rq.management.commands.rqworker import Command as RQWorkerCommand


class Command(RQWorkerCommand):
    help = "Run RQ worker on 'zomato' queue (defaults to single queue 'zomato')"

    def handle(self, *args: Any, **options: Any) -> None:
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        # If no queue arguments passed, default to single queue 'zomato'
        if not args:
            args = (queue_name,)
        super().handle(*args, **options)
