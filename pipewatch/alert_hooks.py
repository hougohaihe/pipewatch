"""Alert hooks for pipewatch — triggered on pipeline run failures or thresholds."""

import json
import logging
import urllib.request
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class BaseAlertHook(ABC):
    """Abstract base class for alert hooks."""

    @abstractmethod
    def send(self, run_record: Dict[str, Any]) -> bool:
        """Send an alert based on the run record. Returns True on success."""
        ...


class LogAlertHook(BaseAlertHook):
    """Simple alert hook that logs to stderr/stdout via Python logging."""

    def __init__(self, level: str = "WARNING"):
        self.level = getattr(logging, level.upper(), logging.WARNING)

    def send(self, run_record: Dict[str, Any]) -> bool:
        msg = (
            f"[pipewatch alert] pipeline={run_record.get('pipeline_name')} "
            f"run_id={run_record.get('run_id')} "
            f"status={run_record.get('status')} "
            f"error={run_record.get('error_message')}"
        )
        logger.log(self.level, msg)
        return True


class WebhookAlertHook(BaseAlertHook):
    """Alert hook that POSTs run record JSON to a webhook URL."""

    def __init__(self, url: str, timeout: int = 5, extra_headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.timeout = timeout
        self.extra_headers = extra_headers or {}

    def send(self, run_record: Dict[str, Any]) -> bool:
        payload = json.dumps(run_record).encode("utf-8")
        headers = {"Content-Type": "application/json", **self.extra_headers}
        req = urllib.request.Request(self.url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                status = resp.status
                logger.debug("Webhook alert sent, HTTP %s", status)
                return 200 <= status < 300
        except Exception as exc:  # noqa: BLE001
            logger.error("Webhook alert failed: %s", exc)
            return False


class AlertDispatcher:
    """Dispatches alerts to one or more registered hooks."""

    def __init__(self):
        self._hooks: list[BaseAlertHook] = []

    def register(self, hook: BaseAlertHook) -> None:
        """Register an alert hook."""
        self._hooks.append(hook)

    def dispatch(self, run_record: Dict[str, Any]) -> Dict[str, bool]:
        """Dispatch the run record to all registered hooks.

        Returns a dict mapping hook class name to success boolean.
        """
        results: Dict[str, bool] = {}
        for hook in self._hooks:
            key = type(hook).__name__
            try:
                results[key] = hook.send(run_record)
            except Exception as exc:  # noqa: BLE001
                logger.error("Alert hook %s raised: %s", key, exc)
                results[key] = False
        return results
