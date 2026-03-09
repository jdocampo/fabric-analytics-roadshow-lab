"""
Singleton Lock for Spark Job Definition

Provides a distributed lock mechanism to prevent concurrent runs of the same
Spark Job Definition in Microsoft Fabric.
"""

import json
import logging
from datetime import datetime, timezone

import notebookutils

logger = logging.getLogger(__name__)


class SingletonJobLock:
    """
    Distributed lock mechanism to prevent concurrent runs of the Spark Job Definition.
    Uses a lock file in OneLake to ensure only one instance runs at a time.
    """
    
    def __init__(self, lock_path: str, job_name: str = "stream_bronze_and_silver"):
        self.lock_path = lock_path
        self.job_name = job_name
        self.lock_acquired = False
        
    def _read_lock_file(self) -> dict | None:
        """Read existing lock file if it exists."""
        try:
            content = notebookutils.fs.head(self.lock_path, maxBytes=4096)
            return json.loads(content)
        except Exception:
            return None
    
    def _write_lock_file(self, lock_info: dict) -> bool:
        """Write lock file with job information."""
        try:
            content = json.dumps(lock_info, indent=2)
            notebookutils.fs.put(self.lock_path, content, overwrite=True)
            return True
        except Exception as e:
            logger.error(f"Failed to write lock file: {e}")
            return False
    
    def _delete_lock_file(self) -> bool:
        """Delete the lock file."""
        try:
            notebookutils.fs.rm(self.lock_path, recurse=False)
            return True
        except Exception as e:
            logger.warning(f"Failed to delete lock file: {e}")
            return False
    
    def acquire(self) -> bool:
        """
        Attempt to acquire the lock.
        Returns True if lock was acquired, False if another instance is running.
        """
        existing_lock = self._read_lock_file()
        
        if existing_lock:
            logger.error("=" * 80)
            logger.error("LOCK ACQUISITION FAILED - Another instance is already running!")
            logger.error("=" * 80)
            logger.error(f"  Job Name: {existing_lock.get('job_name', 'unknown')}")
            logger.error(f"  Started At: {existing_lock.get('started_at', 'unknown')}")
            logger.error(f"  Activity ID: {existing_lock.get('activity_id', 'unknown')}")
            logger.error(f"  Livy ID: {existing_lock.get('livy_id', 'unknown')}")
            logger.error("=" * 80)
            logger.error("This job will exit to prevent concurrent streaming operations.")
            logger.error("To force a new run, manually delete the lock file at:")
            logger.error(f"  {self.lock_path}")
            logger.error("=" * 80)
            return False
        
        # Create lock with current job info
        context = notebookutils.runtime.context
        lock_info = {
            "job_name": self.job_name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "activity_id": context.get('activityId', 'unknown'),
            "livy_id": context.get('livyId', 'unknown'),
            "workspace_id": context.get('currentWorkspaceId', 'unknown'),
        }
        
        if self._write_lock_file(lock_info):
            self.lock_acquired = True
            logger.info("=" * 80)
            logger.info("LOCK ACQUIRED - This is the only running instance")
            logger.info(f"  Lock file: {self.lock_path}")
            logger.info("=" * 80)
            return True
        
        return False
    
    def release(self):
        """Release the lock by deleting the lock file."""
        if self.lock_acquired:
            if self._delete_lock_file():
                logger.info("Lock released successfully")
                self.lock_acquired = False
            else:
                logger.warning("Failed to release lock - manual cleanup may be required")
