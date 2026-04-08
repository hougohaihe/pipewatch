"""Load and validate alert hook configuration from a dict or TOML/JSON config file."""

import json
from pathlib import Path
from typing import Any, Dict, List

from pipewatch.alert_hooks import AlertDispatcher, LogAlertHook, WebhookAlertHook

_HOOK_REGISTRY = {
    "log": LogAlertHook,
    "webhook": WebhookAlertHook,
}


def _build_hook(hook_cfg: Dict[str, Any]):
    """Instantiate a single hook from a config dict."""
    hook_type = hook_cfg.get("type", "").lower()
    if hook_type not in _HOOK_REGISTRY:
        raise ValueError(
            f"Unknown alert hook type '{hook_type}'. "
            f"Available: {list(_HOOK_REGISTRY)}"
        )
    cls = _HOOK_REGISTRY[hook_type]
    params = {k: v for k, v in hook_cfg.items() if k != "type"}
    return cls(**params)


def build_dispatcher_from_config(config: Dict[str, Any]) -> AlertDispatcher:
    """Build an AlertDispatcher from a configuration dict.

    Expected shape::

        {
          "alerts": [
            {"type": "log", "level": "ERROR"},
            {"type": "webhook", "url": "https://example.com/hook"}
          ]
        }
    """
    dispatcher = AlertDispatcher()
    hooks_cfg: List[Dict[str, Any]] = config.get("alerts", [])
    for hook_cfg in hooks_cfg:
        hook = _build_hook(hook_cfg)
        dispatcher.register(hook)
    return dispatcher


def load_dispatcher_from_file(path: str | Path) -> AlertDispatcher:
    """Load alert dispatcher configuration from a JSON file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Alert config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    return build_dispatcher_from_config(config)
