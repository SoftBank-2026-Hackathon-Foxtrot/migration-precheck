"""Gather the facts the rules judge: one read-only pass over both clouds and the platform.

Control-plane APIs (EC2, RDS, Cloud SQL, buckets, Cloudflare, Tailscale) are read from here.
Data inside the app VPCs (databases, Redis, app readiness, the control-plane's own state) is read by
probes that run inside platform tasks, which reach both app VPCs over the tailnet.
"""

from __future__ import annotations

from typing import Any, Callable

from .clouds import Clouds
from .config import Config, Side

FEED_PREFIX = "om-"          # object-migration's change feeds: SQS queues, Pub/Sub topics/subs
REPLICATION_PREFIX = "fxt_"  # db-migration-adaptor's publications, subscriptions and slots


def clouds_for(cfg: Config) -> Clouds:
    return Clouds(aws_profile=cfg.cli.get("aws_profile", "foxtrot"), region=cfg.platform["region"],
                  gcloud_configuration=cfg.cli.get("gcloud_configuration", "foxtrot"), project=cfg.gcp_project)


class Collector:
    def __init__(self, cfg: Config, clouds: Clouds, log: Callable[[str], None] = lambda _m: None):
        self.cfg, self.c, self.log = cfg, clouds, log
        self.facts: dict[str, Any] = {"errors": []}

    def _try(self, name: str, fn: Callable[[], Any]) -> Any:
        self.log(f"reading {name}")
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - a failed read is a finding, not a crash
            self.facts["errors"].append(f"{name}: {exc}")
            return None

    # ---- per-cloud resources ---------------------------------------------------------------
    def _aws_side(self, s: Side) -> dict[str, Any]:
        c = self.c
        out: dict[str, Any] = {}
        vms = self._try(f"{s.role} vm", lambda: c.aws(
            "ec2", "describe-instances", "--filters", f"Name=tag:Name,Values={s.vm_name}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query", "Reservations[].Instances[].{id:InstanceId,state:State.Name,ip:PrivateIpAddress}"))
        out["vm"] = {"instances": vms or []}
        db = self._try(f"{s.role} rds", lambda: c.aws(
            "rds", "describe-db-instances", "--db-instance-identifier", s.db_instance,
            "--query", "DBInstances[0].{status:DBInstanceStatus,public:PubliclyAccessible}"))
        out["db_instance"] = db

        def redis() -> dict[str, Any]:
            group = c.aws("elasticache", "describe-replication-groups", "--replication-group-id", s.redis_instance,
                          "--query", "ReplicationGroups[0].{status:Status,members:MemberClusters}")
            pg = c.aws("elasticache", "describe-cache-clusters", "--cache-cluster-id", group["members"][0],
                       "--query", "CacheClusters[0].CacheParameterGroup.CacheParameterGroupName")
            notify = c.aws("elasticache", "describe-cache-parameters", "--cache-parameter-group-name", pg,
                           "--query", "Parameters[?ParameterName=='notify-keyspace-events'].ParameterValue | [0]")
            return {"status": group["status"], "notify": notify}
        out["redis_instance"] = self._try(f"{s.role} elasticache", redis)

        def bucket() -> dict[str, Any]:
            listing = c.aws("s3api", "list-objects-v2", "--bucket", s.bucket, "--max-keys", "1000", "--no-paginate",
                            "--query", "{count:KeyCount,more:IsTruncated}")
            notif = c.aws("s3api", "get-bucket-notification-configuration", "--bucket", s.bucket) or {}
            feeds = [q.get("QueueArn", "") for q in notif.get("QueueConfigurations", [])]
            feeds += [t.get("TopicArn", "") for t in notif.get("TopicConfigurations", [])]
            if "EventBridgeConfiguration" in notif:
                feeds.append("eventbridge")
            return {"objects": listing["count"], "more": listing["more"], "notifications": feeds}
        out["bucket"] = self._try(f"{s.role} s3 bucket", bucket)
        return out

    def _gcp_side(self, s: Side) -> dict[str, Any]:
        c = self.c
        out: dict[str, Any] = {}
        vm = self._try(f"{s.role} vm", lambda: c.gcloud("compute", "instances", "describe", s.vm_name, "--zone", s.zone))
        out["vm"] = {"instances": [] if not vm else [
            {"id": vm["name"], "state": vm["status"].lower().replace("terminated", "stopped"),
             "ip": vm["networkInterfaces"][0]["networkIP"]}]}
        db = self._try(f"{s.role} cloud sql", lambda: c.gcloud("sql", "instances", "describe", s.db_instance))
        out["db_instance"] = None if not db else {
            "status": db["state"],
            "flags": {f["name"]: f["value"] for f in db.get("settings", {}).get("databaseFlags", [])},
            "public": db.get("settings", {}).get("ipConfiguration", {}).get("ipv4Enabled")}
        redis = self._try(f"{s.role} memorystore", lambda: c.gcloud("redis", "instances", "describe",
                                                                   s.redis_instance, "--region", s.region))
        out["redis_instance"] = None if not redis else {
            "status": redis["state"], "notify": (redis.get("redisConfigs") or {}).get("notify-keyspace-events")}

        def bucket() -> dict[str, Any]:
            objs = c.gcloud("storage", "objects", "list", f"gs://{s.bucket}/**", "--limit", "1000", check=False) or []
            notif = c.gcloud("storage", "buckets", "notifications", "list", f"gs://{s.bucket}") or []
            confs = [n.get("Notification Configuration", n) for n in notif]
            return {"objects": len(objs), "more": len(objs) >= 1000,
                    "notifications": [f"{n.get('id')}:{n.get('topic', '').rsplit('/', 1)[-1]}" for n in confs]}
        out["bucket"] = self._try(f"{s.role} gcs bucket", bucket)
        return out

    # ---- platform ----------------------------------------------------------------------------
    def _platform(self) -> None:
        c, p, f = self.c, self.cfg.platform, self.facts

        def ecs() -> dict[str, Any]:
            found: dict[str, Any] = {}
            names = p["services"]
            for i in range(0, len(names), 10):
                for svc in c.aws("ecs", "describe-services", "--cluster", p["cluster"], "--services", *names[i:i + 10],
                                 "--query", "services[].{name:serviceName,running:runningCount,desired:desiredCount,"
                                            "deployments:length(deployments)}"):
                    found[svc["name"]] = svc
            return found
        f["ecs"] = self._try("ecs services", ecs)

        def lb() -> dict[str, Any]:
            token = c.secret(p["cloudflare_token_secret"])
            base = "https://api.cloudflare.com/client/v4"
            lbr = c.http_json(f"{base}/zones/{p['cloudflare_zone']}/load_balancers/{p['load_balancer']}", token=token)["result"]
            pools = {}
            for pool_id in lbr.get("default_pools", []):
                pr = c.http_json(f"{base}/accounts/{p['cloudflare_account']}/load_balancers/pools/{pool_id}", token=token)["result"]
                pools[pool_id] = {"name": pr["name"], "healthy": pr.get("healthy"),
                                  "origins": [o["address"] for o in pr["origins"] if o.get("enabled", True)]}
            return {"steering": lbr.get("steering_policy"), "weights": (lbr.get("random_steering") or {}).get("pool_weights", {}),
                    "pools": pools}
        f["lb"] = self._try("cloudflare load balancer", lb)

        def tailscale() -> list[dict[str, Any]]:
            token = c.secret(p["tailscale_token_secret"])
            devices = c.http_json("https://api.tailscale.com/api/v2/tailnet/-/devices?fields=all", basic=token)["devices"]
            return [{"host": d["hostname"], "online": bool(d.get("connectedToControl")), "routes": d.get("enabledRoutes", []),
                     "last_seen": d.get("lastSeen")} for d in devices]
        f["tailscale"] = self._try("tailscale devices", tailscale)

        def feeds() -> dict[str, Any]:
            out: dict[str, Any] = {}
            if "aws" in (self.cfg.source.cloud, self.cfg.target.cloud):
                urls = c.aws("sqs", "list-queues", "--queue-name-prefix", FEED_PREFIX, "--query", "QueueUrls") or []
                out["sqs"] = [u.rsplit("/", 1)[-1] for u in urls]
            if self.cfg.gcp_project:
                out["pubsub_topics"] = [t["name"].rsplit("/", 1)[-1] for t in c.gcloud("pubsub", "topics", "list") or []
                                        if t["name"].rsplit("/", 1)[-1].startswith(FEED_PREFIX)]
                out["pubsub_subscriptions"] = [s["name"].rsplit("/", 1)[-1] for s in c.gcloud("pubsub", "subscriptions", "list") or []
                                               if s["name"].rsplit("/", 1)[-1].startswith(FEED_PREFIX)]
            return out
        f["feeds"] = self._try("change feeds", feeds)

    # ---- probes ------------------------------------------------------------------------------
    def probe_params(self) -> dict[str, dict[str, Any]]:
        apps = {}
        for role in ("source", "target"):
            s = self.cfg.side(role)
            ip = self.vm_ip(role)
            if ip:
                apps[role] = f"http://{ip}:{s.app_port}/readyz"
        return {
            "db": {"region": self.cfg.platform["region"], "pgbouncer_database": self.cfg.platform["pgbouncer_database"],
                   "secrets": {"source": self.cfg.source.db_secret, "target": self.cfg.target.db_secret},
                   "tables": self.cfg.database["tables"], "writer_role": self.cfg.database["writer_role"],
                   "prefix": REPLICATION_PREFIX, "apps": apps, "fix": []},
            "redis": {"names": [self.cfg.source.redis_name, self.cfg.target.redis_name], "flush": []},
            "cp": {"abort": []},
        }

    def vm_ip(self, role: str) -> str | None:
        running = [i for i in (self.facts.get(role, {}).get("vm") or {}).get("instances", []) if i.get("state") == "running"]
        return running[0]["ip"] if len(running) == 1 else None

    def run_probes(self, fixes: dict[str, dict[str, Any]] | None = None) -> None:
        params = self.probe_params()
        for name, extra in (fixes or {}).items():
            params[name].update(extra)
        cluster = self.cfg.platform["cluster"]
        where = {"db": ("db-proxy", "backend", "db.py"), "redis": ("redis-migrator", "statemig", "redis.py"),
                 "cp": ("control-plane", "server", "cp.py")}
        self.facts["probes"] = {}
        for name, (service, container, file) in where.items():
            self.facts["probes"][name] = self._try(f"{name} probe ({service})",
                                                   lambda s=service, ct=container, fl=file, n=name:
                                                   self.c.probe(cluster, s, ct, fl, params[n]))

    def collect(self) -> dict[str, Any]:
        for role in ("source", "target"):
            s = self.cfg.side(role)
            self.facts[role] = self._aws_side(s) if s.cloud == "aws" else self._gcp_side(s)
        self._platform()
        self.run_probes()
        return self.facts
