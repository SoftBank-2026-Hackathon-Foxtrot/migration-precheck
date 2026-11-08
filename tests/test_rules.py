import copy
import unittest
from pathlib import Path

from precheck.config import Config
from precheck.fixes import probe_fixes
from precheck.rules import FAIL, PASS, judge

CFG = Config.load(Path(__file__).resolve().parent.parent / "config" / "chiikawa.toml")
TABLES = CFG.database["tables"]


def ready_facts() -> dict:
    """A demo-ready deployment: everything on AWS, GCP pre-created and empty."""
    vm = lambda ip: {"instances": [{"id": "vm", "state": "running", "ip": ip}]}  # noqa: E731
    db = lambda ip, login, rows: {  # noqa: E731
        "ip": ip, "server": ip, "version": "17", "wal_level": "logical", "role_login": login,
        "tables": {t: rows for t in TABLES}, "publications": [], "subscriptions": [], "slots": []}
    return {
        "errors": [],
        "source": {"vm": vm("10.10.0.132"), "db_instance": {"status": "available"},
                   "redis_instance": {"status": "available", "notify": "KEA"},
                   "bucket": {"objects": 10, "more": False, "notifications": []}},
        "target": {"vm": vm("10.20.0.2"), "db_instance": {"status": "RUNNABLE", "flags": {"cloudsql.logical_decoding": "on"}},
                   "redis_instance": {"status": "READY", "notify": "KEA"},
                   "bucket": {"objects": 0, "more": False, "notifications": []}},
        "ecs": {s: {"name": s, "running": 1, "desired": 1, "deployments": 1} for s in CFG.platform["services"]},
        "lb": {"steering": "random", "weights": {CFG.source.lb_pool: 1, CFG.target.lb_pool: 0},
               "pools": {CFG.source.lb_pool: {"name": "aws", "healthy": True, "origins": [CFG.source.tunnel + ".cfargotunnel.com"]},
                         CFG.target.lb_pool: {"name": "gcp", "healthy": True, "origins": [CFG.target.tunnel + ".cfargotunnel.com"]}}},
        "tailscale": [{"host": h, "online": True, "routes": r} for h, r in
                      [("foxtrot-platform-gw", []), ("chiikawa-aws-3e3d", CFG.source.routes), ("chiikawa-gcp-1975", CFG.target.routes)]],
        "feeds": {"sqs": [], "pubsub_topics": [], "pubsub_subscriptions": []},
        "probes": {
            "db": {"databases": {"source": db("10.10.11.118", True, 5), "target": db("10.21.0.3", False, 0)},
                   "pgbouncer": {"host": "rds", "ip": "10.10.11.118", "paused": 0, "disabled": 0},
                   "apps": {"source": {"status": 200}, "target": {"status": 200}}, "fixed": [], "errors": []},
            "redis": {"redis": {"aws": {"keys": 5, "ping": "PONG"}, "gcp": {"keys": 0, "ping": "PONG"}},
                      "proxy": {"active": "aws", "paused": False}, "fixed": [], "errors": []},
            "cp": {"seed": {"environments": {
                       "aws": {"appUrl": "http://10.10.0.132:8000", "resources": CFG.source.resource_refs},
                       "gcp": {"appUrl": "http://10.20.0.2:8000", "resources": CFG.target.resource_refs}},
                       "resources": {"DATABASE": {"sourceRef": None}, "REDIS": {"sourceRef": None},
                                     "OBJECT_STORAGE": {"sourceRef": None}}},
                   "workflows": [], "fixed": [], "errors": []},
        },
    }


def by_id(results):
    return {r.id: r for r in results}


class ReadyState(unittest.TestCase):
    def test_ready_deployment_passes_everything(self):
        results = judge(CFG, ready_facts())
        self.assertEqual([r for r in results if r.status != PASS], [])

    def test_dirty_target_is_fixable_while_the_app_lives_on_the_source(self):
        f = ready_facts()
        f["probes"]["db"]["databases"]["target"]["tables"] = {t: 3 for t in TABLES}
        f["probes"]["db"]["databases"]["target"]["role_login"] = True
        f["probes"]["redis"]["redis"]["gcp"]["keys"] = 7
        f["target"]["bucket"]["objects"] = 4
        r = by_id(judge(CFG, f))
        for key in ("target.db.empty", "target.db.login", "target.redis.empty", "target.bucket.empty"):
            self.assertEqual(r[key].status, FAIL, key)
            self.assertIsNotNone(r[key].fix, key)
        ops = probe_fixes([x.fix for x in r.values() if x.fix])
        self.assertEqual([o["op"] for o in ops["db"]["fix"]], ["truncate", "login"])
        self.assertEqual(ops["redis"]["flush"], ["gcp"])


class Guards(unittest.TestCase):
    """When the app runs on the target, nothing may empty or fence the target."""

    def live_on_target(self):
        f = ready_facts()
        f["probes"]["db"]["pgbouncer"]["ip"] = "10.21.0.3"
        f["probes"]["redis"]["proxy"]["active"] = "gcp"
        f["probes"]["db"]["databases"]["target"]["tables"] = {t: 100 for t in TABLES}
        f["probes"]["db"]["databases"]["target"]["role_login"] = True
        f["probes"]["db"]["databases"]["target"]["publications"] = ["fxt_abc_f"]
        f["probes"]["redis"]["redis"]["gcp"]["keys"] = 700
        f["target"]["bucket"]["objects"] = 50
        return f

    def test_no_destructive_fix_when_the_app_runs_on_the_target(self):
        r = by_id(judge(CFG, self.live_on_target()))
        for key in ("target.db.empty", "target.db.login", "target.redis.empty", "target.bucket.empty",
                    "target.db.replication", "pgbouncer.route", "redis.proxy"):
            self.assertEqual(r[key].status, FAIL, key)
            self.assertIsNone(r[key].fix, key)
            self.assertTrue(r[key].manual, key)

    def test_no_fix_without_knowing_the_workflows(self):
        f = copy.deepcopy(ready_facts())
        f["probes"]["cp"] = None
        f["probes"]["db"]["databases"]["source"]["subscriptions"] = [{"name": "fxt_x", "enabled": True, "slot": "fxt_x"}]
        f["target"]["bucket"]["objects"] = 2
        r = by_id(judge(CFG, f))
        self.assertIsNone(r["source.db.replication"].fix)
        self.assertIsNone(r["target.bucket.empty"].fix)

    def test_open_workflow_and_stale_seed_fail(self):
        f = ready_facts()
        f["probes"]["cp"]["workflows"] = [{"id": "wf-1", "status": "HALTED", "source": "gcp", "target": "aws", "stage": 0}]
        f["probes"]["cp"]["seed"]["environments"]["aws"]["appUrl"] = "http://10.10.0.79:8000"
        r = by_id(judge(CFG, f))
        self.assertEqual(r["cp.workflows"].fix.args["ids"], ["wf-1"])
        self.assertEqual(r["seed.appurl.source"].status, FAIL)


if __name__ == "__main__":
    unittest.main()
