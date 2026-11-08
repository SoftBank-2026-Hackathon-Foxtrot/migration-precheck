"""Apply the fixes the rules proposed. Every fix is idempotent: run twice, the second run finds
nothing to do. Probe fixes (databases, Redis, workflows) run inside the probes; the rest here."""

from __future__ import annotations

from typing import Any, Callable

from .clouds import Clouds
from .config import Config, Side
from .rules import Fix


def probe_fixes(fixes: list[Fix]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {"db": {"fix": []}, "redis": {"flush": []}, "cp": {"abort": []}}
    for f in fixes:
        if f.kind == "db":
            out["db"]["fix"].append(f.args)
        elif f.kind == "redis_flush":
            out["redis"]["flush"].append(f.args["name"])
        elif f.kind == "cp_abort":
            out["cp"]["abort"].extend(f.args["ids"])
    # Order inside one database: drop replication before truncating, fence last.
    rank = {"drop_replication": 0, "truncate": 1, "login": 2}
    out["db"]["fix"].sort(key=lambda op: rank[op["op"]])
    return out


def _keys(c: Clouds, s: Side) -> set[str]:
    if s.cloud == "aws":
        out = c.aws("s3api", "list-objects-v2", "--bucket", s.bucket, "--query", "Contents[].Key") or []
        return set(out)
    objs = c.gcloud("storage", "objects", "list", f"gs://{s.bucket}/**", check=False) or []
    return {o["name"] for o in objs}


def empty_bucket(c: Clouds, cfg: Config, role: str, force: bool, log: Callable[[str], None]) -> None:
    s, other = cfg.side(role), cfg.side("source" if role == "target" else "target")
    keys = _keys(c, s)
    if not keys:
        return
    safe = keys & _keys(c, other) if not force else keys
    kept = sorted(keys - safe)
    if kept:
        log(f"  kept {len(kept)} object(s) in {s.bucket} that {other.bucket} does not have (e.g. {kept[0]}); "
            "rerun with --force to delete them too")
    batch = sorted(safe)
    for i in range(0, len(batch), 500):
        part = batch[i:i + 500]
        if s.cloud == "aws":
            import json
            c.aws("s3api", "delete-objects", "--bucket", s.bucket, "--delete",
                  json.dumps({"Objects": [{"Key": k} for k in part], "Quiet": True}))
        else:
            c.gcloud("storage", "rm", *(f"gs://{s.bucket}/{k}" for k in part))
    log(f"  deleted {len(batch)} object(s) from {s.bucket}")


def remove_bucket_feeds(c: Clouds, cfg: Config, role: str, log: Callable[[str], None]) -> None:
    s = cfg.side(role)
    if s.cloud == "aws":
        c.aws("s3api", "put-bucket-notification-configuration", "--bucket", s.bucket,
              "--notification-configuration", "{}")
        log(f"  cleared the notification configuration of {s.bucket}")
        return
    for item in c.gcloud("storage", "buckets", "notifications", "list", f"gs://{s.bucket}") or []:
        n = item.get("Notification Configuration", item)
        c.gcloud("storage", "buckets", "notifications", "delete",
                 f"projects/_/buckets/{s.bucket}/notificationConfigs/{n['id']}")
        log(f"  removed notification {n['id']} ({n.get('topic', '').rsplit('/', 1)[-1]}) from {s.bucket}")


def delete_feeds(c: Clouds, feeds: dict[str, Any], log: Callable[[str], None]) -> None:
    for name in feeds.get("sqs", []):
        url = c.aws("sqs", "get-queue-url", "--queue-name", name, "--query", "QueueUrl", check=False)
        if url:
            c.aws("sqs", "delete-queue", "--queue-url", url)
            log(f"  deleted queue {name}")
    for name in feeds.get("pubsub_subscriptions", []):
        c.gcloud("pubsub", "subscriptions", "delete", name, check=False)
        log(f"  deleted subscription {name}")
    for name in feeds.get("pubsub_topics", []):
        c.gcloud("pubsub", "topics", "delete", name, check=False)
        log(f"  deleted topic {name}")


def apply_local(c: Clouds, cfg: Config, fixes: list[Fix], force: bool, log: Callable[[str], None]) -> None:
    # Feeds first: a bucket emptied while its change feed still exists would publish deletes.
    order = {"bucket_feeds": 0, "feeds_delete": 1, "bucket_empty": 2}
    for f in sorted((f for f in fixes if f.kind in order), key=lambda f: order[f.kind]):
        log(f"- {f.summary}")
        if f.kind == "bucket_feeds":
            remove_bucket_feeds(c, cfg, f.args["side"], log)
        elif f.kind == "feeds_delete":
            delete_feeds(c, f.args, log)
        elif f.kind == "bucket_empty":
            empty_bucket(c, cfg, f.args["side"], force, log)
