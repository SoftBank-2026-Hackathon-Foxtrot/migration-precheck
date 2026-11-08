# Runs inside the redis-migrator `statemig` container (python, STATEMIG_ENDPOINTS, STATEMIG_PROXY,
# STATEMIG_ADMIN_TOKEN), which reaches both Redis instances over the tailnet. Reads key counts,
# keyspace notifications and the proxy's active backend; with `fix` it flushes a named instance.
import base64
import json
import os
import socket
import ssl
import urllib.parse
import urllib.request


def _emit(result):
    data = base64.b64encode(json.dumps(result, default=str).encode()).decode()
    parts = [data[i:i + 300] for i in range(0, len(data), 300)] or [""]
    for i, part in enumerate(parts):
        print(f"PRECHECK:{i}/{len(parts)}:{part}|", flush=True)


class Redis:
    def __init__(self, url):
        u = urllib.parse.urlparse(url)
        sock = socket.create_connection((u.hostname, u.port or 6379), timeout=5)
        if u.scheme == "rediss":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=u.hostname)
        self.f = sock.makefile("rwb")
        if u.password:
            self.cmd(*(["AUTH", urllib.parse.unquote(u.username), urllib.parse.unquote(u.password)]
                       if u.username else ["AUTH", urllib.parse.unquote(u.password)]))

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a.encode()), a.encode()) for a in args)
        self.f.write(out)
        self.f.flush()
        return self._read()

    def _read(self):
        line = self.f.readline().rstrip(b"\r\n")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:-2]
            return data.decode(errors="replace")
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError(f"unexpected reply {line[:40]!r}")


def _keys(r):
    total = 0
    for line in (r.cmd("INFO", "keyspace") or "").splitlines():
        if line.startswith("db") and "keys=" in line:
            total += int(line.split("keys=")[1].split(",")[0])
    return total


def run(params):
    out = {"redis": {}, "proxy": None, "fixed": [], "errors": []}
    endpoints = dict(item.split("=", 1) for item in os.environ.get("STATEMIG_ENDPOINTS", "").split(",") if "=" in item)

    # Reset only: rewrite the proxy's control record (shared EFS file) to serve one backend. The
    # proxy starts from this record, so it takes effect when redis-proxy restarts.
    if params.get("active_to"):
        path = os.environ.get("STATEMIG_CONTROL_FILE", "/state/control.json")
        try:
            want = params["active_to"]
            if want not in endpoints:
                raise RuntimeError(f"{want} is not in STATEMIG_ENDPOINTS")
            try:
                with open(path) as f:
                    rec = json.load(f)
            except FileNotFoundError:
                rec = {"gen": 0}
            new = {"gen": int(rec.get("gen", 0)) + 1, "active": want, "consume": want, "paused": False}
            tmp = f"{path}.precheck"
            with open(tmp, "w") as f:
                json.dump(new, f)
            os.replace(tmp, path)
            out["fixed"].append(f"control record: {rec} -> {new} (applies on redis-proxy restart)")
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"control record: {exc}")
    try:
        req = urllib.request.Request(os.environ["STATEMIG_PROXY"].rstrip("/") + "/status",
                                     headers={"Authorization": "Bearer " + os.environ["STATEMIG_ADMIN_TOKEN"]})
        with urllib.request.urlopen(req, timeout=5) as resp:
            out["proxy"] = json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"proxy status: {exc}")
    active = (out["proxy"] or {}).get("active")

    for name in params["names"]:
        url = endpoints.get(name)
        if not url:
            out["errors"].append(f"{name}: not in STATEMIG_ENDPOINTS ({sorted(endpoints)})")
            continue
        try:
            r = Redis(url)
            if name in params.get("flush", []):
                if active is None or active == name:
                    out["errors"].append(f"{name}: refused to flush, the proxy's active backend is {active}")
                else:
                    r.cmd("FLUSHALL")
                    out["fixed"].append(f"{name}: flushed")
            info = {"ping": r.cmd("PING"), "keys": _keys(r)}
            try:
                got = r.cmd("CONFIG", "GET", "notify-keyspace-events")
                info["notify"] = got[1] if isinstance(got, list) and len(got) > 1 else None
            except RuntimeError as exc:
                info["notify"] = None
                info["notify_error"] = str(exc)[:120]
            out["redis"][name] = info
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{name}: {exc}")
    return out
