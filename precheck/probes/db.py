# Runs inside the db-proxy `backend` container (python + psycopg + boto3, PGBOUNCER_ADMIN_DSN),
# which reaches both databases over the tailnet. Reads the source and target databases, the
# PgBouncer route and the app readiness; with `fix` it also changes them (see FIX_OPS).
import base64
import json
import os
import socket
import urllib.request

import boto3
import psycopg
from psycopg import sql


def _emit(result):
    data = base64.b64encode(json.dumps(result, default=str).encode()).decode()
    parts = [data[i:i + 300] for i in range(0, len(data), 300)] or [""]
    for i, part in enumerate(parts):
        print(f"PRECHECK:{i}/{len(parts)}:{part}|", flush=True)


def _ip(host):
    try:
        return socket.gethostbyname(host)
    except OSError:
        return None


def _qualified(name):
    schema, _, table = name.rpartition(".")
    return sql.Identifier(schema or "public", table)


def _inspect(conn, tables, role):
    one = lambda q, *a: conn.execute(q, a).fetchone()  # noqa: E731
    found = {}
    for name in tables:
        if one("SELECT to_regclass(%s)", name)[0] is None:
            found[name] = None
        else:
            found[name] = one(sql.SQL("SELECT count(*) FROM {}").format(_qualified(name)))[0]
    login = one("SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", role)
    return {
        "server": one("SELECT inet_server_addr()::text")[0],
        "version": one("SHOW server_version")[0],
        "wal_level": one("SHOW wal_level")[0],
        "role_login": None if login is None else login[0],
        "tables": found,
        "publications": [r[0] for r in conn.execute("SELECT pubname FROM pg_publication").fetchall()],
        "subscriptions": [{"name": r[0], "enabled": r[1], "slot": r[2]} for r in conn.execute(
            "SELECT subname, subenabled, subslotname FROM pg_subscription").fetchall()],
        "slots": [{"name": r[0], "active": r[1], "type": r[2]} for r in conn.execute(
            "SELECT slot_name, active, slot_type FROM pg_replication_slots").fetchall()],
    }


def _fix(conn, op, side, tables, role, prefix, log):
    if op["op"] == "truncate":
        present = [t for t in tables if conn.execute("SELECT to_regclass(%s)", (t,)).fetchone()[0]]
        if present:
            conn.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
                sql.SQL(", ").join(_qualified(t) for t in present)))
            log.append(f"{side}: truncated {len(present)} tables")
    elif op["op"] == "login":
        conn.execute(sql.SQL("ALTER ROLE {} " + ("LOGIN" if op["value"] else "NOLOGIN")).format(sql.Identifier(role)))
        log.append(f"{side}: role {role} {'LOGIN' if op['value'] else 'NOLOGIN'}")
    elif op["op"] == "drop_replication":
        for (name,) in conn.execute("SELECT subname FROM pg_subscription WHERE subname LIKE %s", (prefix + "%",)).fetchall():
            ident = sql.Identifier(name)
            conn.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(ident))
            conn.execute(sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name = NONE)").format(ident))
            conn.execute(sql.SQL("DROP SUBSCRIPTION {}").format(ident))
            log.append(f"{side}: dropped subscription {name}")
        for (name,) in conn.execute("SELECT pubname FROM pg_publication WHERE pubname LIKE %s", (prefix + "%",)).fetchall():
            conn.execute(sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(name)))
            log.append(f"{side}: dropped publication {name}")
        for (name,) in conn.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s AND NOT active",
                                    (prefix + "%",)).fetchall():
            conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            log.append(f"{side}: dropped replication slot {name}")


def run(params):
    sm = boto3.client("secretsmanager", region_name=params["region"])
    out = {"databases": {}, "pgbouncer": None, "apps": {}, "fixed": [], "errors": []}
    dsns = {}
    for side, secret in params["secrets"].items():
        try:
            dsns[side] = json.loads(sm.get_secret_value(SecretId=secret)["SecretString"])["adminDsn"]
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{side} secret: {exc}")

    # Reset only: point the adaptor's stored route at another database. The runtime replays this
    # record into PgBouncer when db-proxy restarts, and resumes it only if that database's writer
    # role is the only one that can log in.
    if params.get("route_to"):
        try:
            from db_operator.config import Settings
            from db_operator.runtime import RuntimeSettings
            from db_operator.service import Service
            from db_operator.store import now

            runtime, settings = RuntimeSettings(), Settings()
            service = Service(settings)
            want = params["route_to"]
            alias = params["pgbouncer_database"]

            def update(tx):
                if service.registry.bindings:
                    # Static mode: one binding from the registry file.
                    if want not in service.registry.databases:
                        raise RuntimeError(f"{want} is not in the registry")
                    key = runtime.binding_ref
                else:
                    # Dynamic mode (bindings registered over the API): find the app's binding.
                    rows = [r for r in tx.list("BINDING") if r.get("alias") == alias]
                    if len(rows) != 1:
                        raise RuntimeError(f"{len(rows)} bindings with alias {alias}")
                    key = rows[0]["bindingId"]
                    service.bindings.resource(tx, want)   # raises if the database is not registered
                row = tx.get("ROUTE", key)
                if row is None:
                    raise RuntimeError(f"no stored route for {key}")
                before = row["database_ref"]
                tx.put("ROUTE", key, {**row, "database_ref": want, "access": "OPEN", "updated_at": now()})
                return key, before

            key, before = service.store.transact(update)
            out["fixed"].append(f"route of {key}: {before} -> {want} (applies on db-proxy restart)")
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"route: {exc}")

    # PgBouncer: where does the app's database entry point now?
    try:
        with psycopg.connect(os.environ["PGBOUNCER_ADMIN_DSN"], autocommit=True, connect_timeout=5) as c:
            cur = c.execute("SHOW DATABASES")
            cols = [d.name for d in cur.description]
            for row in cur.fetchall():
                r = dict(zip(cols, row))
                if r.get("name") == params["pgbouncer_database"]:
                    out["pgbouncer"] = {k: r.get(k) for k in ("host", "port", "database", "paused", "disabled")}
                    out["pgbouncer"]["ip"] = _ip(r.get("host") or "")
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"pgbouncer: {exc}")

    host_ip = {side: _ip(psycopg.conninfo.conninfo_to_dict(d).get("host", "")) for side, d in dsns.items()}
    route_ip = (out["pgbouncer"] or {}).get("ip")

    for side, dsn in dsns.items():
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=8) as conn:
                for op in [o for o in params.get("fix", []) if o["side"] == side]:
                    if op["op"] == "truncate" and (route_ip is None or route_ip == host_ip.get(side)):
                        # Never empty the database the application is using right now.
                        out["errors"].append(f"{side}: refused to truncate, PgBouncer routes the app here ({route_ip})")
                        continue
                    _fix(conn, op, side, params["tables"], params["writer_role"], params["prefix"], out["fixed"])
                out["databases"][side] = _inspect(conn, params["tables"], params["writer_role"])
                out["databases"][side]["ip"] = host_ip.get(side)
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{side} database: {exc}")

    for side, url in params["apps"].items():
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                out["apps"][side] = {"status": resp.status, "body": resp.read(200).decode(errors="replace")}
        except Exception as exc:  # noqa: BLE001
            out["apps"][side] = {"status": None, "body": str(exc)[:200]}
    return out
