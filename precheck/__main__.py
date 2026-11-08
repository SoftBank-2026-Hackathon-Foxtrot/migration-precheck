"""migration-precheck: is the demo ready to migrate?

  python -m precheck check            read-only; exit 1 when anything FAILs
  python -m precheck fix              show what `fix --apply` would change
  python -m precheck fix --apply      apply the fixes, then check again
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .collect import Collector, clouds_for
from .config import Config
from .fixes import apply_local, probe_fixes
from .rules import FAIL, PASS, WARN, Result, judge

ICON = {PASS: "✓", WARN: "!", FAIL: "✗"}
AREAS = ["collect", "platform", "network", "traffic", "source", "target"]


def report(results: list[Result], *, verbose: bool) -> None:
    for area in AREAS:
        rows = [r for r in results if r.area == area and (verbose or r.status != PASS)]
        if not rows:
            continue
        print(f"\n[{area}]")
        for r in rows:
            line = f"  {ICON[r.status]} {r.title}"
            if r.detail and r.status != PASS or verbose and r.detail:
                line += f"  — {r.detail}"
            print(line)
            if r.fix:
                print(f"      fix: {r.fix.summary}")
            elif r.manual:
                print(f"      manual: {r.manual}")
    counts = {s: sum(r.status == s for r in results) for s in (PASS, WARN, FAIL)}
    fixable = sum(1 for r in results if r.fix)
    verdict = "READY" if not counts[FAIL] else "NOT READY"
    print(f"\n{verdict}: {counts[PASS]} pass, {counts[WARN]} warn, {counts[FAIL]} fail"
          + (f" ({fixable} fixable with `fix --apply`)" if fixable else ""))


def main(argv: list[str] | None = None) -> int:
    default_cfg = Path(__file__).resolve().parent.parent / "config" / "chiikawa.toml"
    ap = argparse.ArgumentParser(prog="precheck", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["check", "fix"])
    ap.add_argument("-c", "--config", default=str(default_cfg))
    ap.add_argument("--apply", action="store_true", help="fix: change the resources (default: only show the plan)")
    ap.add_argument("--force", action="store_true", help="fix: also delete target objects the source does not have")
    ap.add_argument("--json", action="store_true", help="print results as JSON")
    ap.add_argument("-v", "--verbose", action="store_true", help="list passing checks too")
    args = ap.parse_args(argv)

    cfg = Config.load(args.config)
    clouds = clouds_for(cfg)
    log = (lambda m: print(m, file=sys.stderr)) if not args.json else (lambda _m: None)
    print(f"{cfg.application}: {cfg.source.env} (source) -> {cfg.target.env} (target)", file=sys.stderr)

    facts = Collector(cfg, clouds, log=lambda m: log(f"  {m}")).collect()
    results = judge(cfg, facts)

    if args.command == "fix":
        fixes = [r.fix for r in results if r.fix]
        if not fixes:
            print("nothing to fix automatically", file=sys.stderr)
        elif not args.apply:
            print("\nfix --apply would:")
            for f in fixes:
                print(f"  - {f.summary}")
        else:
            print("\napplying:", file=sys.stderr)
            apply_local(clouds, cfg, fixes, args.force, log)
            collector = Collector(cfg, clouds, log=lambda m: log(f"  {m}"))
            for role in ("source", "target"):
                s = cfg.side(role)
                collector.facts[role] = collector._aws_side(s) if s.cloud == "aws" else collector._gcp_side(s)
            collector._platform()
            collector.run_probes(probe_fixes(fixes))   # probes apply their fixes, then read the result
            facts = collector.facts
            for name in ("db", "redis", "cp"):
                for line in (facts.get("probes", {}).get(name) or {}).get("fixed", []):
                    log(f"- {line}")
            results = judge(cfg, facts)

    if args.json:
        json.dump({"ready": not any(r.status == FAIL for r in results), "results": [asdict(r) for r in results]},
                  sys.stdout, indent=2, default=str)
        print()
    else:
        report(results, verbose=args.verbose)
    return 1 if any(r.status == FAIL for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
