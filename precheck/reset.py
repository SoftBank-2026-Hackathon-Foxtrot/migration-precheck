"""Force the demo-ready state, accepting the loss of whatever the app wrote on the target.

`fix` only tidies a target that the app does not use. `reset` first moves every route back to the
source by rewriting the routers' own records (no migration runs, nothing is copied back), then
empties the target the way `fix` does. Each step is idempotent, so a failed reset can be rerun.

  1. workflows   ABORT the open ones; clear the object migration the control plane hands to apps
  2. database    drop fxt_* objects, source app LOGIN / target NOLOGIN, adaptor route -> source,
                 restart db-proxy (it replays the route into PgBouncer and resumes)
  3. redis       proxy control record -> source, restart redis-proxy
  4. objects     remove the apps' migration-ID file (AWS: in place; GCP: VM reset), change feeds
  5. traffic     load balancer weights source 1 / target 0
  6. target      TRUNCATE, FLUSHALL, empty the bucket (all objects)
"""

from __future__ import annotations

import time
from typing import Any, Callable

from .clouds import Clouds, CliError
from .collect import Collector
from .config import Config
from .fixes import delete_feeds, empty_bucket, remove_bucket_feeds

ID_FILE = "/tmp/object-migration-id"


def plan(cfg: Config) -> list[str]:
    s, t = cfg.source, cfg.target
    return [
        "ABORT open workflows; clear the control plane's object migration ID",
        f"drop fxt_* replication objects on both databases; app LOGIN on {s.db_instance}, NOLOGIN on {t.db_instance}",
        f"db adaptor route -> {s.db_ref}; restart db-proxy",
        f"Redis proxy control record -> {s.redis_name}; restart redis-proxy",
        f"remove {ID_FILE} in the app containers on {s.vm_name}; reset {t.vm_name} (fresh containers)",
        f"remove change feeds of both buckets and om-* queues/topics/subscriptions",
        f"load balancer weights {s.env}=1 {t.env}=0",
        f"TRUNCATE app tables on {t.db_instance}, FLUSHALL {t.redis_instance}, delete every object in {t.bucket}",
    ]


class Reset:
    def __init__(self, cfg: Config, clouds: Clouds, log: Callable[[str], None]):
        self.cfg, self.c, self.log = cfg, clouds, log
        self.col = Collector(cfg, clouds, log=lambda m: log(f"    {m}"))

    def probe(self, name: str, extra: dict[str, Any]) -> dict[str, Any]:
        params = self.col.probe_params()[name]
        params.update(extra)
        service, container, file = {"db": ("db-proxy", "backend", "db.py"),
                                     "redis": ("redis-migrator", "statemig", "redis.py"),
                                     "cp": ("control-plane", "server", "cp.py")}[name]
        out = self.c.probe(self.cfg.platform["cluster"], service, container, file, params)
        for line in out.get("fixed", []):
            self.log(f"    {line}")
        for err in out.get("errors", []):
            self.log(f"    ! {err}")
        return out

    def restart(self, *services: str) -> None:
        cluster = self.cfg.platform["cluster"]
        for svc in services:
            self.c.aws("ecs", "update-service", "--cluster", cluster, "--service", svc, "--force-new-deployment",
                       "--query", "service.serviceName")
        self.log(f"    restarting {', '.join(services)}; waiting until stable")
        self.c._run(["aws", "ecs", "wait", "services-stable", "--cluster", cluster, "--services", *services],
                    {"AWS_PROFILE": self.c.aws_profile, "AWS_REGION": self.c.region}, timeout=900)

    def run(self) -> None:
        cfg, s, t = self.cfg, self.cfg.source, self.cfg.target
        for role in ("source", "target"):
            side = cfg.side(role)
            self.col.facts[role] = self.col._aws_side(side) if side.cloud == "aws" else self.col._gcp_side(side)

        self.log("1. workflows")
        cp = self.probe("cp", {"clear_object_migration": True})
        open_ = [w["id"] for w in cp.get("workflows", [])]
        if open_:
            cp = self.probe("cp", {"abort": open_})
            still = [w for w in cp.get("workflows", [])]
            if still:
                raise CliError(f"workflows still open: {still}; halt them in the console and rerun")

        self.log("2. database")
        self.probe("db", {"route_to": s.db_ref, "fix": [
            {"side": "source", "op": "drop_replication"}, {"side": "target", "op": "drop_replication"},
            {"side": "target", "op": "login", "value": False}, {"side": "source", "op": "login", "value": True}]})
        self.restart("db-proxy")

        self.log("3. redis")
        self.probe("redis", {"active_to": s.redis_name})
        self.restart("redis-proxy")

        self.log("4. objects")
        self.clear_id_files()
        for role in ("source", "target"):
            remove_bucket_feeds(self.c, cfg, role, lambda m: self.log(f"  {m}"))
        feeds = {"sqs": [], "pubsub_topics": [], "pubsub_subscriptions": []}
        self.col._platform()
        feeds.update(self.col.facts.get("feeds") or {})
        delete_feeds(self.c, feeds, lambda m: self.log(f"  {m}"))

        self.log("5. traffic")
        self.weights()

        self.log("6. target")
        db = self.probe("db", {"fix": [{"side": "target", "op": "truncate"}]})
        route = (db.get("pgbouncer") or {}).get("ip")
        src_ip = ((db.get("databases") or {}).get("source") or {}).get("ip")
        if route != src_ip:
            raise CliError(f"PgBouncer still routes to {route}, not the source {src_ip}: target left as is")
        self.probe("redis", {"flush": [t.redis_name]})
        empty_bucket(self.c, cfg, "target", True, lambda m: self.log(f"  {m}"))

    def clear_id_files(self) -> None:
        cfg = self.cfg
        for role in ("source", "target"):
            side = cfg.side(role)
            if side.cloud == "aws":
                inst = [i for i in self.col.facts[role]["vm"]["instances"] if i["state"] == "running"]
                for i in inst:
                    script = (f"for c in $(docker ps --format '{{{{.Names}}}}'); do "
                              f"docker exec $c rm -f {ID_FILE} 2>/dev/null; done; echo done")
                    cmd = self.c.aws("ssm", "send-command", "--instance-ids", i["id"], "--document-name",
                                     "AWS-RunShellScript", "--parameters", f'commands=["{script}"]',
                                     "--comment", "migration-precheck reset: forget the object migration",
                                     "--query", "Command.CommandId")
                    st = None
                    for _ in range(30):
                        time.sleep(3)
                        st = self.c.aws("ssm", "get-command-invocation", "--command-id", cmd, "--instance-id", i["id"],
                                        "--query", "Status", check=False)
                        if st in ("Success", "Failed", "TimedOut", "Cancelled"):
                            break
                    self.log(f"    {side.vm_name}: {ID_FILE} removed in every container ({st})")
            else:
                self.c.gcloud("compute", "instances", "reset", side.vm_name, "--zone", side.zone)
                self.log(f"    {side.vm_name}: reset (containers start fresh, without {ID_FILE})")

    def weights(self) -> None:
        p, s, t = self.cfg.platform, self.cfg.source, self.cfg.target
        token = self.c.secret(p["cloudflare_token_secret"])
        url = f"https://api.cloudflare.com/client/v4/zones/{p['cloudflare_zone']}/load_balancers/{p['load_balancer']}"
        body = {"steering_policy": "random",
                "random_steering": {"default_weight": 0, "pool_weights": {s.lb_pool: 1, t.lb_pool: 0}}}
        res = self.c.http_json(url, token=token, method="PATCH", body=body)
        weights = (res.get("result") or {}).get("random_steering", {}).get("pool_weights")
        self.log(f"    load balancer weights now {weights}")
