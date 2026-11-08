# Runs inside the control-plane `server` container: the seed it loaded and the workflows that are
# not finished. With `abort` it sends ABORT to the named workflows.
import asyncio
import base64
import json


def _emit(result):
    data = base64.b64encode(json.dumps(result, default=str).encode()).decode()
    parts = [data[i:i + 300] for i in range(0, len(data), 300)] or [""]
    for i, part in enumerate(parts):
        print(f"PRECHECK:{i}/{len(parts)}:{part}|", flush=True)


async def _main(params):
    from control_plane.config import Settings
    from control_plane.domain.models import ResourceKind
    from control_plane.main import build_platform
    from control_plane.seed import load_seed

    out = {"seed": {}, "workflows": [], "fixed": [], "errors": []}
    settings = Settings()
    seed = load_seed(settings.seed_path)
    out["seed"] = {
        "application": seed.application,
        "environments": {name: {"appUrl": str(env.app_url), "trafficPool": env.traffic_pool,
                                "actors": dict(env.actors), "resources": dict(getattr(env, "resources", {}) or {})}
                         for name, env in seed.environments.items()},
        "resources": {},
    }
    for kind in ResourceKind:
        res = seed.resources.of(kind)
        if res is not None:
            # Older seeds pin the direction in sourceRef/targetRef; newer ones take the refs from
            # the environments, so either direction runs from the same seed.
            out["seed"]["resources"][kind.value] = {"sourceRef": res.source_ref, "targetRef": res.target_ref}

    platform = build_platform(settings)
    repo, workflows = platform.context.repo, platform.context.workflows
    for wid in await repo.workflow_ids():
        w = await repo.workflow(wid)
        if w is None or w.status.value in ("COMPLETED", "ABORTED"):
            continue
        if wid in params.get("abort", []) and w.status.value in ("PLANNED", "WAITING_APPROVAL", "HALTED"):
            from control_plane.workflows import CommandInput

            try:
                w = await workflows.apply(wid, CommandInput(command="ABORT", expected_version=w.version),
                                          actor="migration-precheck")
                for _ in range(30):
                    w = await repo.workflow(wid)
                    if w.status.value != "ABORTING":
                        break
                    await asyncio.sleep(2)
                out["fixed"].append(f"{wid}: {w.status.value}")
            except Exception as exc:  # noqa: BLE001
                out["errors"].append(f"{wid}: ABORT failed: {exc}")
        if w.status.value not in ("COMPLETED", "ABORTED"):
            out["workflows"].append({"id": wid, "status": w.status.value, "source": w.source_env,
                                     "target": w.target_env, "stage": w.current_stage,
                                     "created": w.created_at.isoformat()[:19]})
    return out


def run(params):
    return asyncio.run(_main(params))
