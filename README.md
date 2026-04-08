# pipewatch

A lightweight CLI tool for monitoring and logging data pipeline runs with structured output and alerting hooks.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Run pipewatch alongside any pipeline command to capture structured logs and trigger alerts:

```bash
pipewatch run --name "daily_etl" --alert-on failure python pipeline.py
```

Log output to a file with JSON formatting:

```bash
pipewatch run --name "data_sync" --log-file ./logs/run.json python sync.py
```

List recent pipeline runs:

```bash
pipewatch history --last 10
```

Configure an alerting hook (e.g., Slack webhook) in `~/.pipewatch/config.toml`:

```toml
[alerts]
webhook_url = "https://hooks.slack.com/services/your/webhook/url"
notify_on = ["failure", "timeout"]
```

---

## Features

- Structured JSON logging for every pipeline run
- Configurable alerting hooks (Slack, webhooks, email)
- Run history tracking with exit codes and durations
- Minimal setup — works with any command-line pipeline

---

## License

This project is licensed under the [MIT License](LICENSE).