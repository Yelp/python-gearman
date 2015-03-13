"""
Gearman API - Client, worker, and admin client interfaces
"""

__version__ = '2.0.3'

from gearman.admin_client import GearmanAdminClient
from gearman.client import GearmanClient
from gearman.worker import GearmanWorker

from gearman.connection_manager import DataEncoder
from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_PENDING, JOB_CREATED, JOB_FAILED, JOB_COMPLETE, JOB_UNKNOWN

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

gearman_root_logger = logging.getLogger('gearman')
gearman_root_logger.addHandler(NullHandler())
