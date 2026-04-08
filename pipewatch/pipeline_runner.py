"""High-level pipeline runner that integrates RunLogger and alert dispatching."""

import time
import traceback
from typing import Callable, Optional

from pipewatch.run_logger import RunLogger, RunStatus
from pipewatch.alert_hooks import AlertDispatcher


class PipelineRunner:
    """Wraps a callable pipeline function with logging and alerting."""

    def __init__(
        self,
        name: str,
        logger: RunLogger,
        dispatcher: Optional[AlertDispatcher] = None,
    ):
        self.name = name
        self.logger = logger
        self.dispatcher = dispatcher

    def run(self, fn: Callable, *args, **kwargs):
        """Execute *fn* with full lifecycle logging and optional alerting.

        Returns:
            The return value of *fn* on success, or None on failure.
        """
        run_id = self.logger.log_start(self.name)
        start = time.monotonic()
        result = None
        try:
            result = fn(*args, **kwargs)
            duration = time.monotonic() - start
            self.logger.log_success(run_id, duration_seconds=duration)
            if self.dispatcher:
                self.dispatcher.dispatch(
                    level="info",
                    message=f"Pipeline '{self.name}' completed successfully "
                            f"(run_id={run_id}, duration={duration:.2f}s)",
                )
        except Exception as exc:  # noqa: BLE001
            duration = time.monotonic() - start
            tb = traceback.format_exc()
            self.logger.log_failure(run_id, error=str(exc), traceback=tb, duration_seconds=duration)
            if self.dispatcher:
                self.dispatcher.dispatch(
                    level="error",
                    message=f"Pipeline '{self.name}' FAILED "
                            f"(run_id={run_id}): {exc}",
                )
        return result
