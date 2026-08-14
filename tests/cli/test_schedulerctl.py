#!/usr/bin/env python3
"""Black-box schedulerctl regression tests against a running API server."""

from __future__ import annotations

import json
import os
import subprocess
import unittest
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any


class SchedulerCTL:
    def __init__(self) -> None:
        self.binary = os.environ.get("SCHEDULERCTL", "schedulerctl")
        self.server = os.environ.get("SCHEDULER_URL", "http://127.0.0.1:18080")
        self.email = os.environ.get("SCHEDULER_EMAIL", "admin@example.com")
        self.password = os.environ.get("SCHEDULER_PASSWORD", "SchedulerDemo123!")
        self.tenant = os.environ.get("SCHEDULER_TENANT", "")
        self.token = ""

    def invoke(
        self,
        *args: str,
        stdin: str | None = None,
        authenticated: bool = True,
        parse_json: bool = True,
    ) -> Any:
        command = [self.binary, "--server", self.server]
        if authenticated and self.token:
            command.extend(["--token", self.token])
        if authenticated and self.tenant:
            command.extend(["--tenant", self.tenant])
        command.extend(args)
        result = subprocess.run(
            command,
            input=stdin,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"command failed ({result.returncode}): {' '.join(command)}\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        output = result.stdout.strip()
        if not parse_json:
            return output
        return json.loads(output) if output else None

    def login(self) -> str:
        token = self.invoke(
            "--email",
            self.email,
            "--password-stdin",
            "login",
            stdin=self.password + "\n",
            authenticated=False,
            parse_json=False,
        )
        if not token:
            raise AssertionError("schedulerctl login returned an empty token")
        self.token = token
        return token


def as_int(value: Any) -> int:
    return int(value)


def items(value: dict[str, Any], key: str) -> list[dict[str, Any]]:
    result = value.get(key, [])
    if not isinstance(result, list):
        raise AssertionError(f"{key} is not a list: {result!r}")
    return result


class SchedulerCTLRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ctl = SchedulerCTL()
        health = cls.ctl.invoke("health", authenticated=False)
        if health.get("status") != "ready":
            raise AssertionError(f"API server is not ready: {health!r}")
        cls.ctl.login()
        cls.prefix = f"python-cli-{uuid.uuid4().hex[:10]}"

    def test_authentication_version_completion_and_read_models(self) -> None:
        version = self.ctl.invoke("version", authenticated=False, parse_json=False)
        expected = os.environ.get("SCHEDULERCTL_EXPECTED_VERSION")
        if expected:
            self.assertEqual(expected, version)
        else:
            self.assertTrue(version)

        dashboard = self.ctl.invoke("dashboard")
        self.assertIsInstance(dashboard, dict)
        report = self.ctl.invoke(
            "reports",
            "runs",
            "--from",
            (datetime.now(UTC) - timedelta(days=1)).date().isoformat(),
            "--to",
            datetime.now(UTC).date().isoformat(),
            "--timezone",
            "UTC",
        )
        self.assertIn("points", report)
        for shell in ("bash", "zsh", "fish", "powershell"):
            completion = self.ctl.invoke(
                "completion", shell, authenticated=False, parse_json=False
            )
            self.assertIn("schedulerctl", completion)

    def test_job_dependency_trigger_and_run_lifecycle(self) -> None:
        created: list[dict[str, Any]] = []

        def create_job(suffix: str) -> dict[str, Any]:
            definition = {
                "name": f"{self.prefix}-{suffix}",
                "schedule_type": "fixed_rate",
                "schedule_expression": "3600",
                "timezone": "UTC",
                "target_url": f"{self.ctl.server}/health/live",
                "http_method": "GET",
                "timeout_seconds": 10,
                "max_retries": 0,
                "overlap_policy": "parallel",
                "misfire_policy": "fire_once",
                "max_concurrent_runs": 1,
                "max_catch_up": 10,
                "callback_timeout_seconds": 30,
                "max_queue_size": 10,
                "enabled": False,
            }
            job = self.ctl.invoke("jobs", "create", stdin=json.dumps(definition))
            created.append(job)
            return job

        parent = create_job("parent")
        child = create_job("child")
        try:
            listed = self.ctl.invoke("jobs", "list")
            self.assertTrue(
                {parent["id"], child["id"]}.issubset(
                    {job["id"] for job in items(listed, "jobs")}
                )
            )
            loaded = self.ctl.invoke("jobs", "get", parent["id"])
            loaded["description"] = "updated by Python CLI regression"
            updated = self.ctl.invoke(
                "jobs", "update", parent["id"], stdin=json.dumps(loaded)
            )
            self.assertEqual(loaded["description"], updated["description"])

            started = self.ctl.invoke(
                "jobs", "start", parent["id"], "--version", str(updated["version"])
            )
            stopped = self.ctl.invoke(
                "jobs", "stop", parent["id"], "--version", str(started["version"])
            )
            parent.update(stopped)

            preview = self.ctl.invoke(
                "jobs",
                "preview",
                "--type",
                "cron",
                "--expression",
                "0 * * * * *",
                "--timezone",
                "UTC",
                "--count",
                "3",
            )
            self.assertEqual(3, len(preview.get("times", preview.get("trigger_times", []))))

            self.ctl.invoke(
                "jobs", "dependencies", "set", parent["id"], "--child", child["id"]
            )
            dependencies = self.ctl.invoke(
                "jobs", "dependencies", "get", parent["id"]
            )
            self.assertIn(child["id"], dependencies.get("child_job_ids", []))

            key = f"{self.prefix}-idempotent"
            first = self.ctl.invoke(
                "jobs", "trigger", parent["id"], "--idempotency-key", key
            )
            second = self.ctl.invoke(
                "jobs", "trigger", parent["id"], "--idempotency-key", key
            )
            self.assertEqual(first["id"], second["id"])
            run = self.ctl.invoke("runs", "get", first["id"])
            self.assertEqual(parent["id"], run["job_id"])
            logs = self.ctl.invoke("runs", "logs", first["id"])
            self.assertIn("entries", logs)
            filtered = self.ctl.invoke("runs", "--job", parent["id"])
            self.assertIn(first["id"], {entry["id"] for entry in items(filtered, "runs")})
            if run["status"] in {"pending", "running"}:
                cancelled = self.ctl.invoke(
                    "runs", "cancel", first["id"], "--reason", "Python regression cleanup"
                )
                self.assertEqual("cancelled", cancelled["status"])
        finally:
            try:
                self.ctl.invoke("jobs", "dependencies", "set", parent["id"])
            except AssertionError:
                pass
            for job in reversed(created):
                try:
                    current = self.ctl.invoke("jobs", "get", job["id"])
                    self.ctl.invoke(
                        "jobs", "delete", job["id"], "--version", str(current["version"])
                    )
                except AssertionError:
                    pass

    def test_executor_group_and_grpc_node_lifecycle(self) -> None:
        group = self.ctl.invoke(
            "executors",
            "groups",
            "create",
            "--name",
            f"{self.prefix}-executors",
            "--strategy",
            "first",
            "--mode",
            "manual",
            "--address",
            "grpc://worker-a.invalid:9999",
        )
        try:
            updated = self.ctl.invoke(
                "executors",
                "groups",
                "update",
                group["id"],
                "--name",
                f"{self.prefix}-executors-updated",
                "--strategy",
                "round",
                "--mode",
                "manual",
                "--address",
                "grpc://worker-b.invalid:9999",
                "--version",
                str(group["version"]),
            )
            group.update(updated)
            automatic = self.ctl.invoke(
                "executors",
                "groups",
                "update",
                group["id"],
                "--name",
                f"{self.prefix}-executors-automatic",
                "--strategy",
                "round",
                "--mode",
                "automatic",
                "--version",
                str(group["version"]),
            )
            group.update(automatic)
            node_id = f"python-{uuid.uuid4().hex[:8]}"
            self.ctl.invoke(
                "executors",
                "register",
                group["id"],
                node_id,
                "--address",
                "grpc://worker-node.invalid:9999",
                "--ttl",
                "30",
                "--label",
                "python,regression",
            )
            nodes = self.ctl.invoke("executors", "list", group["id"])
            self.assertIn(node_id, {node["node_id"] for node in items(nodes, "nodes")})
            self.ctl.invoke("executors", "unregister", group["id"], node_id)
        finally:
            self.ctl.invoke(
                "executors",
                "groups",
                "delete",
                group["id"],
                "--version",
                str(group["version"]),
            )

    def test_kubernetes_credentials_are_preserved_on_metadata_update(self) -> None:
        cluster = self.ctl.invoke(
            "kubernetes-clusters",
            "create",
            "--name",
            f"{self.prefix}-cluster",
            "--auth-mode",
            "service_account",
            "--api-server",
            "https://kubernetes.invalid",
            "--namespace",
            "jobs",
            "--service-account-token",
            "regression-secret",
            "--max-concurrent-jobs",
            "10",
        )
        try:
            updated = self.ctl.invoke(
                "kubernetes-clusters",
                "update",
                cluster["id"],
                "--name",
                f"{self.prefix}-cluster-updated",
                "--auth-mode",
                "service_account",
                "--api-server",
                "https://kubernetes.invalid",
                "--namespace",
                "jobs-updated",
                "--max-concurrent-jobs",
                "25",
                "--version",
                str(cluster["version"]),
            )
            cluster.update(updated)
            loaded = self.ctl.invoke("kubernetes-clusters", "get", cluster["id"])
            self.assertEqual("jobs-updated", loaded["namespace"])
            self.assertEqual(25, as_int(loaded["max_concurrent_jobs"]))
        finally:
            self.ctl.invoke(
                "kubernetes-clusters",
                "delete",
                cluster["id"],
                "--version",
                str(cluster["version"]),
            )

    def test_notification_channel_lifecycle_and_history(self) -> None:
        channel = self.ctl.invoke(
            "notifications",
            "create",
            "--kind",
            "webhook",
            "--name",
            f"{self.prefix}-webhook",
            "--config",
            json.dumps({"url": "https://webhook.invalid/regression"}),
            "--events",
            "pending,running,succeeded,failed,cancelled,exhausted",
            "--all-jobs=true",
        )
        try:
            updated = self.ctl.invoke(
                "notifications",
                "update",
                channel["id"],
                "--kind",
                "webhook",
                "--name",
                f"{self.prefix}-webhook-updated",
                "--events",
                "failed,exhausted",
                "--all-jobs=true",
                "--version",
                str(channel["version"]),
            )
            channel.update(updated)
            disabled = self.ctl.invoke(
                "notifications",
                "disable",
                channel["id"],
                "--version",
                str(channel["version"]),
            )
            channel.update(disabled)
            enabled = self.ctl.invoke(
                "notifications",
                "enable",
                channel["id"],
                "--version",
                str(channel["version"]),
            )
            channel.update(enabled)
            listed = self.ctl.invoke("notifications", "list")
            self.assertIn(channel["id"], {item["id"] for item in items(listed, "channels")})
            history = self.ctl.invoke(
                "notifications", "history", "--channel-id", channel["id"], "--limit", "10"
            )
            self.assertIn("deliveries", history)
        finally:
            self.ctl.invoke(
                "notifications",
                "delete",
                channel["id"],
                "--version",
                str(channel["version"]),
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
