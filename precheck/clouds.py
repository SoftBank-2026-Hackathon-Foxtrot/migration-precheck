"""Thin wrappers over the aws/gcloud CLIs and the Cloudflare/Tailscale APIs.

The CLIs carry the operator's own login (`aws login --profile foxtrot`, gcloud configuration
foxtrot), so the tool needs no credentials of its own. Every call retries transient network errors.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
import urllib.request
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TRANSIENT = re.compile(r"Could not connect|NameResolution|Name or service not known|timed out|Connection reset"
                       r"|nodename nor servname|Temporary failure|ThrottlingException|TooManyRequests")


class CliError(RuntimeError):
    pass


@dataclass
class Clouds:
    aws_profile: str
    region: str
    gcloud_configuration: str
    project: str | None = None
    _secrets: dict[str, str] = field(default_factory=dict)

    # ---- CLIs ------------------------------------------------------------------------------
    def _run(self, args: list[str], env: dict[str, str], *, check: bool = True, timeout: int = 300) -> str:
        last = ""
        for attempt in range(6):
            proc = subprocess.run(args, capture_output=True, text=True, env={**os.environ, **env}, timeout=timeout)
            if proc.returncode == 0:
                return proc.stdout
            last = (proc.stderr or proc.stdout).strip()
            if not TRANSIENT.search(last):
                break
            time.sleep(2 + 3 * attempt)
        if check:
            raise CliError(f"{args[0]} {' '.join(args[1:4])}: {last[-400:]}")
        return ""

    def aws(self, *args: str, check: bool = True) -> Any:
        out = self._run(["aws", *args, "--output", "json"],
                        {"AWS_PROFILE": self.aws_profile, "AWS_REGION": self.region}, check=check)
        return json.loads(out) if out.strip() else None

    def gcloud(self, *args: str, check: bool = True) -> Any:
        extra = ["--project", self.project] if self.project else []
        out = self._run(["gcloud", *args, *extra, "--format", "json"],
                        {"CLOUDSDK_ACTIVE_CONFIG_NAME": self.gcloud_configuration}, check=check)
        return json.loads(out) if out.strip() else None

    def secret(self, name: str) -> str:
        if name not in self._secrets:
            self._secrets[name] = self.aws("secretsmanager", "get-secret-value", "--secret-id", name,
                                           "--query", "SecretString")
        return self._secrets[name]

    # ---- HTTP APIs -------------------------------------------------------------------------
    def http_json(self, url: str, *, token: str | None = None, basic: str | None = None,
                  method: str = "GET", body: object | None = None) -> Any:
        headers = {"Accept": "application/json", "User-Agent": "migration-precheck"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if basic:
            headers["Authorization"] = "Basic " + base64.b64encode(f"{basic}:".encode()).decode()
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        last: Exception | None = None
        for attempt in range(4):
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=20) as resp:
                    return json.loads(resp.read() or b"null")
            except Exception as exc:  # noqa: BLE001 - network errors of every kind are retried
                last = exc
                time.sleep(2 + 3 * attempt)
        raise CliError(f"{method} {url.split('?')[0]}: {last}")

    # ---- probes inside the platform VPC ----------------------------------------------------
    def probe(self, cluster: str, service: str, container: str, probe_file: str, params: dict) -> dict:
        """Run one probe (precheck/probes/<file>) in a running task of `service` via ECS exec.

        The platform tasks reach both app VPCs over the tailnet, which this machine does not.
        The probe gets its parameters inline and prints its JSON result in numbered base64 chunks.
        """
        source = (Path(__file__).parent / "probes" / probe_file).read_text()
        source += f"\n_emit(run(json.loads({json.dumps(params)!r})))\n"
        payload = base64.b64encode(zlib.compress(source.encode(), 9)).decode()
        task = self.aws("ecs", "list-tasks", "--cluster", cluster, "--service-name", service,
                        "--desired-status", "RUNNING", "--query", "taskArns[0]")
        if not task:
            raise CliError(f"no running task for {service}")

        def execute(command: str) -> str:
            return self._run(["aws", "ecs", "execute-command", "--cluster", cluster, "--task", task,
                              "--container", container, "--interactive", "--command", command],
                             {"AWS_PROFILE": self.aws_profile, "AWS_REGION": self.region}, timeout=600)

        # ECS exec cuts long commands (~2.5 KB), so the payload goes over in pieces to a temp file
        # that the probe deletes before it runs.
        path = f"/tmp/precheck-{os.getpid()}-{probe_file}.b64"
        step = 1800
        for i in range(0, len(payload), step):
            execute(f"sh -c \"printf %s '{payload[i:i + step]}' {'>' if i == 0 else '>>'} {path}\"")
        out = execute(f"python -c \"import base64,os,zlib;p='{path}';s=open(p).read();os.remove(p);"
                      f"exec(zlib.decompress(base64.b64decode(s)))\"")
        # The session manager writes its own lines (even into the middle of ours), so chunks are
        # framed (PRECHECK:i/n:<base64>|) and everything that is not base64 is dropped from a frame.
        text = out.replace("Cannot perform start session: EOF", "")
        chunks: dict[int, str] = {}
        total = None
        for m in re.finditer(r"PRECHECK:(\d+)/(\d+):([A-Za-z0-9+/=\s]*)\|", text):
            chunks[int(m.group(1))] = re.sub(r"\s+", "", m.group(3))
            total = int(m.group(2))
        if total is None or len(chunks) != total:
            tail = "\n".join(line[:200] for line in out.replace("\r", "").splitlines()[-8:])
            raise CliError(f"probe {probe_file} in {service} returned no result:\n{tail}")
        return json.loads(base64.b64decode("".join(chunks[i] for i in range(total))))
