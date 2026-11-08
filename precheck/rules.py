"""Judge the collected facts against the demo-ready state. Pure functions: facts in, results out.

Demo-ready means:
- source: the app runs, serves all traffic, and every data route (PgBouncer, Redis proxy) points
  at it; its database, Redis and bucket hold the data.
- target: only the resources created ahead of time exist (VM with the app, database with the
  schema, Redis, bucket); they hold no data and nothing is left from earlier runs.
- platform: services up, no unfinished workflow, a seed that matches the deployment, the tailnet up.

A result may carry a Fix. Fixes only empty or fence the target, fence the source back open, and
remove leftovers of earlier runs; anything that moves traffic or data routes stays manual.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from .config import Config

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
KINDS = {"database": "DATABASE", "redis": "REDIS", "object": "OBJECT_STORAGE"}
ABORTABLE = {"PLANNED", "WAITING_APPROVAL", "HALTED"}


@dataclass(frozen=True)
class Fix:
    kind: str                 # db | redis_flush | cp_abort | bucket_empty | bucket_feeds | feeds_delete
    args: dict[str, Any]
    summary: str


@dataclass
class Result:
    id: str
    area: str
    status: str
    title: str
    detail: str = ""
    fix: Fix | None = None
    manual: str = ""


@dataclass
class Judge:
    cfg: Config
    facts: dict[str, Any]
    results: list[Result] = field(default_factory=list)

    def add(self, id_: str, area: str, ok: bool | None, title: str, detail: str = "", *,
            fix: Fix | None = None, manual: str = "", warn_only: bool = False) -> None:
        status = PASS if ok else (WARN if warn_only or ok is None else FAIL)
        self.results.append(Result(id_, area, status, title, detail, None if ok else fix, "" if ok else manual))

    # ---- helpers -------------------------------------------------------------------------------
    def probe(self, name: str) -> dict[str, Any] | None:
        return (self.facts.get("probes") or {}).get(name)

    def side(self, role: str) -> dict[str, Any]:
        return self.facts.get(role) or {}

    def vm_ip(self, role: str) -> str | None:
        running = [i for i in (self.side(role).get("vm") or {}).get("instances", []) if i.get("state") == "running"]
        return running[0]["ip"] if len(running) == 1 else None

    def proxy(self) -> dict[str, Any]:
        status = (self.probe("redis") or {}).get("proxy") or {}
        return status.get("proxy", status)   # GET /status nests the routing state under "proxy"

    def no_unfinished_workflow(self) -> bool:
        cp = self.probe("cp")
        return cp is not None and not cp.get("workflows")

    def app_on_source(self) -> bool:
        """Every data route points at the source: emptying the target cannot lose live data."""
        db, redis = self.probe("db") or {}, self.probe("redis") or {}
        src = (db.get("databases") or {}).get("source") or {}
        route = (db.get("pgbouncer") or {}).get("ip")
        active = self.proxy().get("active")
        return bool(route) and route == src.get("ip") and active == self.cfg.source.redis_name

    # ---- rules ---------------------------------------------------------------------------------
    def run(self) -> list[Result]:
        for err in self.facts.get("errors", []):
            self.add("read", "collect", False, "could not read", err, manual="check access/network and rerun")
        for name in ("db", "redis", "cp"):
            for err in (self.probe(name) or {}).get("errors", []):
                self.add(f"probe.{name}", "collect", False, f"{name} probe", err)
        self.platform()
        self.vms_and_apps()
        self.databases()
        self.redis()
        self.buckets()
        self.feeds()
        return self.results

    def platform(self) -> None:
        cfg = self.cfg
        for name, svc in sorted((self.facts.get("ecs") or {}).items()):
            ok = svc["desired"] >= 1 and svc["running"] == svc["desired"] and svc["deployments"] == 1
            self.add(f"ecs.{name}", "platform", ok, f"ECS service {name} steady",
                     f"running {svc['running']}/{svc['desired']}, deployments {svc['deployments']}",
                     manual="wait for the rollout or check the service events")
        missing = [s for s in cfg.platform["services"] if s not in (self.facts.get("ecs") or {})]
        if self.facts.get("ecs") is not None:
            self.add("ecs.missing", "platform", not missing, "every platform service exists", ", ".join(missing))

        cp = self.probe("cp")
        if cp:
            open_ = cp.get("workflows", [])
            abortable = [w["id"] for w in open_ if w["status"] in ABORTABLE]
            self.add("cp.workflows", "platform", not open_, "no unfinished workflow",
                     "; ".join(f"{w['id']} {w['status']} {w['source']}->{w['target']} stage {w['stage']}" for w in open_),
                     fix=Fix("cp_abort", {"ids": abortable}, f"ABORT {', '.join(abortable)}") if abortable else None,
                     manual="" if abortable == [w["id"] for w in open_] else "a running workflow: HALT it from the console first")
            self.seed(cp.get("seed") or {})

        lb = self.facts.get("lb")
        if lb:
            w = lb.get("weights", {})
            src_w, tgt_w = w.get(cfg.source.lb_pool), w.get(cfg.target.lb_pool)
            self.add("lb.weights", "traffic", bool(src_w) and not tgt_w, "all traffic goes to the source pool",
                     f"{cfg.source.env}={src_w} {cfg.target.env}={tgt_w} ({lb.get('steering')})",
                     manual="move traffic back to the source with the traffic service (no automatic fix)")
            for role in ("source", "target"):
                s = cfg.side(role)
                pool = lb["pools"].get(s.lb_pool)
                ok = bool(pool) and pool["healthy"] and any(s.tunnel in o for o in pool["origins"])
                self.add(f"lb.pool.{role}", "traffic", ok, f"{s.env} pool healthy and on tunnel {s.tunnel[:8]}",
                         "" if not pool else f"{pool['name']} healthy={pool['healthy']} origins={pool['origins']}",
                         manual="point the pool origin at the app's tunnel (IaC tools/gcp_pool_origin.sh)")

        ts = self.facts.get("tailscale")
        if ts is not None:
            want = {cfg.platform["tailscale_gateway"]: [], cfg.source.tailscale_host: cfg.source.routes,
                    cfg.target.tailscale_host: cfg.target.routes}
            for host, routes in want.items():
                online = [d for d in ts if d["host"] == host and d["online"]]
                ok = bool(online) and all(r in online[0]["routes"] for r in routes)
                self.add(f"tailscale.{host}", "network", ok, f"tailnet node {host} online with its routes",
                         "" if ok else f"online={bool(online)} routes={online[0]['routes'] if online else []}",
                         manual="check tailscaled on the VM; approve the routes")
            stale = sorted(d["host"] for d in ts if d["host"] in want and not d["online"])
            self.add("tailscale.stale", "network", not stale, "no offline duplicates of the app nodes",
                     f"{len(stale)} offline: {', '.join(stale)}", warn_only=True,
                     manual="remove them in the Tailscale admin console (cosmetic)")

    def seed(self, seed: dict[str, Any]) -> None:
        cfg = self.cfg
        envs = seed.get("environments", {})
        resources = seed.get("resources", {})
        for key, kind in KINDS.items():
            self.add(f"seed.{key}", "platform", kind in resources, f"seed migrates {key}",
                     manual=f"add resources.{key} to the seed")
        for role in ("source", "target"):
            s = cfg.side(role)
            env = envs.get(s.env)
            if env is None:
                self.add(f"seed.env.{role}", "platform", False, f"seed has environment {s.env}", manual="add it to the seed")
                continue
            ip = self.vm_ip(role)
            host = urlparse(env["appUrl"]).hostname
            if ip is None and not (self.side(role).get("vm") or {}).get("instances"):
                continue  # the VM could not be read; its own check reports that
            self.add(f"seed.appurl.{role}", "platform", ip is not None and host == ip,
                     f"seed appUrl of {s.env} is the VM's address", f"seed {env['appUrl']}, VM {ip}",
                     manual=f"set environments.{s.env}.appUrl to http://{ip}:{s.app_port} and redeploy the control plane")
            refs = env.get("resources") or {}
            if refs:
                want = s.resource_refs
                wrong = {k: refs.get(k) for k in want if refs.get(k) != want[k]}
                self.add(f"seed.refs.{role}", "platform", not wrong, f"seed refs of {s.env} match the deployment",
                         f"wrong: {wrong}", manual="fix environments.<env>.resources in the seed")
        legacy = {k: v for k, v in resources.items() if v.get("sourceRef")}
        if legacy:
            want = {"DATABASE": "database", "REDIS": "redis", "OBJECT_STORAGE": "object"}
            wrong = [k for k, v in legacy.items()
                     if v["sourceRef"] != cfg.source.resource_refs[want[k]] or v["targetRef"] != cfg.target.resource_refs[want[k]]]
            self.add("seed.direction", "platform", not wrong, f"seed direction is {cfg.source.env} -> {cfg.target.env}",
                     f"reversed or wrong: {wrong}", manual="swap sourceRef/targetRef in the seed")

    def vms_and_apps(self) -> None:
        apps = (self.probe("db") or {}).get("apps", {})
        for role in ("source", "target"):
            s = self.cfg.side(role)
            inst = (self.side(role).get("vm") or {}).get("instances", [])
            running = [i for i in inst if i.get("state") == "running"]
            self.add(f"{role}.vm", role, len(running) == 1, f"one running app VM {s.vm_name}",
                     ", ".join(f"{i['id']} {i['state']} {i['ip']}" for i in inst), manual="start or reprovision the VM")
            app = apps.get(role)
            if app is not None or self.probe("db"):
                ok = bool(app) and app.get("status") == 200
                self.add(f"{role}.app", role, ok, f"{s.env} app answers /readyz",
                         "" if not app else f"{app.get('status')} {app.get('body', '')[:80]}",
                         manual="check the app containers on the VM")

    def databases(self) -> None:
        cfg = self.cfg
        for role in ("source", "target"):
            inst = self.side(role).get("db_instance")
            if inst:
                ok = inst["status"] in ("available", "RUNNABLE")
                self.add(f"{role}.db.instance", role, ok, f"{cfg.side(role).db_instance} is up", inst["status"])
        tgt_inst = self.side("target").get("db_instance") or {}
        if cfg.target.cloud == "gcp" and tgt_inst.get("flags") is not None:
            self.add("target.db.flags", "target", tgt_inst["flags"].get("cloudsql.logical_decoding") == "on",
                     "Cloud SQL has logical decoding on", str(tgt_inst["flags"]), warn_only=True,
                     manual="set the cloudsql.logical_decoding flag (needed to migrate back)")

        db = self.probe("db")
        if not db:
            return
        dbs = db.get("databases", {})
        src, tgt = dbs.get("source"), dbs.get("target")
        route = db.get("pgbouncer") or {}
        if src:
            self.add("pgbouncer.route", "source", bool(route) and route.get("ip") == src.get("ip"),
                     "PgBouncer sends the app to the source database",
                     f"pgbouncer {route.get('host')} ({route.get('ip')}), source {src.get('ip')}"
                     + (f", target {tgt.get('ip')}" if tgt else ""),
                     manual="the app writes elsewhere: finish or roll back the last migration first")
            self.add("pgbouncer.paused", "source", not route.get("paused") and not route.get("disabled"),
                     "PgBouncer entry is not paused", f"paused={route.get('paused')} disabled={route.get('disabled')}")
        on_source = self.app_on_source()
        for role, s in (("source", src), ("target", tgt)):
            if not s:
                continue
            tables = s["tables"]
            missing = [t for t, n in tables.items() if n is None]
            rows = sum(n for n in tables.values() if n)
            self.add(f"{role}.db.schema", role, not missing, "every app table exists", ", ".join(missing),
                     manual="create the schema (app bootstrap) on this database")
            leftovers = ([f"pub {p}" for p in s["publications"] if p.startswith("fxt_")]
                         + [f"sub {x['name']}" for x in s["subscriptions"]]
                         + [f"slot {x['name']}" for x in s["slots"] if x["name"].startswith("fxt_")])
            idle = self.no_unfinished_workflow() and on_source
            self.add(f"{role}.db.replication", role, not leftovers, "no replication leftovers", ", ".join(leftovers),
                     fix=Fix("db", {"side": role, "op": "drop_replication"}, f"drop fxt_* replication objects on {role}")
                     if idle else None,
                     manual="" if idle else "a migration may still use them: finish it, or confirm no workflow is open")
            if role == "source":
                self.add("source.db.login", role, s["role_login"] is True, f"app role can log in on the source",
                         f"rolcanlogin={s['role_login']}",
                         fix=Fix("db", {"side": "source", "op": "login", "value": True}, "ALTER ROLE app LOGIN on source")
                         if on_source else None, manual="" if on_source else "data routes do not point at the source")
                self.add("source.db.data", role, rows > 0, "source database holds the app data", f"{rows} rows",
                         manual="reseed the app data on the source")
                self.add("source.db.wal", role, s["wal_level"] == "logical", "source wal_level is logical",
                         s["wal_level"], manual="enable logical replication (rds.logical_replication=1)")
            else:
                self.add("target.db.login", role, s["role_login"] is False, "app role cannot log in on the target",
                         f"rolcanlogin={s['role_login']}",
                         fix=Fix("db", {"side": "target", "op": "login", "value": False}, "ALTER ROLE app NOLOGIN on target")
                         if on_source else None,
                         manual="" if on_source else "the app uses the target database: fencing it would take the app down")
                self.add("target.db.empty", role, rows == 0, "target database is empty",
                         f"{rows} rows in " + ", ".join(f"{t.split('.')[-1]}={n}" for t, n in tables.items() if n),
                         fix=Fix("db", {"side": "target", "op": "truncate"}, "TRUNCATE the app tables on target")
                         if on_source else None,
                         manual="" if on_source else "data routes do not point at the source: refusing to empty the target")

    def redis(self) -> None:
        cfg = self.cfg
        for role in ("source", "target"):
            inst = self.side(role).get("redis_instance")
            if inst:
                self.add(f"{role}.redis.instance", role, inst["status"] in ("available", "READY"),
                         f"{cfg.side(role).redis_instance} is up", inst["status"])
                notify = inst.get("notify") or ""
                ok = "K" in notify and ("A" in notify or "g" in notify)
                self.add(f"{role}.redis.notify", role, ok, "keyspace notifications on (KEA)", repr(notify),
                         warn_only=(role == "target"), manual="set notify-keyspace-events=KEA in the parameter group/config")
        r = self.probe("redis")
        if not r:
            return
        proxy = self.proxy()
        self.add("redis.proxy", "source", proxy.get("active") == cfg.source.redis_name and not proxy.get("paused"),
                 "Redis proxy sends the app to the source", f"active={proxy.get('active')} paused={proxy.get('paused')}",
                 manual="the app uses another Redis: finish or roll back the last migration first")
        src, tgt = r["redis"].get(cfg.source.redis_name), r["redis"].get(cfg.target.redis_name)
        if src:
            self.add("source.redis.data", "source", src["keys"] > 0, "source Redis holds keys", f"{src['keys']} keys",
                     warn_only=True, manual="exercise the app to fill sessions/caches")
        if tgt:
            on_source = proxy.get("active") == cfg.source.redis_name
            self.add("target.redis.empty", "target", tgt["keys"] == 0, "target Redis is empty", f"{tgt['keys']} keys",
                     fix=Fix("redis_flush", {"name": cfg.target.redis_name}, f"FLUSHALL on {cfg.target.redis_instance}")
                     if on_source else None, manual="" if on_source else "the proxy uses the target: refusing to flush")

    def buckets(self) -> None:
        cfg = self.cfg
        src = self.side("source").get("bucket")
        tgt = self.side("target").get("bucket")
        if src:
            self.add("source.bucket.data", "source", src["objects"] > 0, f"source bucket {cfg.source.bucket} holds objects",
                     f"{src['objects']}{'+' if src['more'] else ''} objects", warn_only=True)
        if tgt:
            n = f"{tgt['objects']}{'+' if tgt['more'] else ''}"
            on_source = self.app_on_source() and self.no_unfinished_workflow()
            self.add("target.bucket.empty", "target", tgt["objects"] == 0, f"target bucket {cfg.target.bucket} is empty",
                     f"{n} objects", fix=Fix("bucket_empty", {"side": "target"},
                                            f"delete the objects in {cfg.target.bucket} that the source also has")
                     if on_source else None,
                     manual="" if on_source else "the app may be using the target bucket: refusing to empty it")
        for role, b in (("source", src), ("target", tgt)):
            if b and b["notifications"]:
                self.add(f"{role}.bucket.feeds", role, False, "no change feed left on the bucket",
                         ", ".join(str(n) for n in b["notifications"]),
                         fix=Fix("bucket_feeds", {"side": role}, f"remove the change-feed notifications of {cfg.side(role).bucket}")
                         if self.no_unfinished_workflow() else None)
            elif b:
                self.add(f"{role}.bucket.feeds", role, True, "no change feed left on the bucket")

    def feeds(self) -> None:
        f = self.facts.get("feeds")
        if f is None:
            return
        names = [*(f"sqs {q}" for q in f.get("sqs", [])), *(f"topic {t}" for t in f.get("pubsub_topics", [])),
                 *(f"sub {s}" for s in f.get("pubsub_subscriptions", []))]
        self.add("feeds.leftover", "platform", not names, "no object-migration feed left over", ", ".join(names),
                 warn_only=True, fix=Fix("feeds_delete", dict(f), "delete the om-* queues, topics and subscriptions")
                 if self.no_unfinished_workflow() else None)


def judge(cfg: Config, facts: dict[str, Any]) -> list[Result]:
    return Judge(cfg, facts).run()
