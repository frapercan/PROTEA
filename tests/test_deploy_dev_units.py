"""The desktop dev stack's unit files stay isolated from production.

``deploy/dev`` defines a development stack that runs BESIDE a production
worker on the same machine. Nothing here runs a unit; these tests read the
files and pin the properties that keep the two apart, so a later edit that
breaks one fails in CI instead of on the machine:

* no laptop path and no production worker unit name anywhere in deploy/dev;
* every unit is protea-dev-*, inside protea-dev.slice, with a memory cap;
* the infra images are the digests docker-compose.yml pins, and the web front's
  image is built from a Containerfile whose every base is pinned by digest;
* every published port is loopback and in the 26xxx range, and the
  readiness gate waits on exactly those ports;
* every unit needs the dev data root on the system disk, and every service
  runs the disk gate first, so dev stops before it can fill the disk
  production runs on (the author's decision of 2026-11-15: no new disk);
* the always-on processes fit their caps inside the slice;
* the dev processes carry the clock gate, RestartPreventExitStatus=78, and a
  start timeout above the gate's cap;
* no secret value is written into a unit.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEV = ROOT / "deploy" / "dev"
QUADLET = DEV / "quadlet"
SYSTEMD = DEV / "systemd"

CONTAINERS = sorted(QUADLET.glob("*.container"))
#: The web front's image is built here, not pulled: its own rule below.
WEB = QUADLET / "protea-dev-web.container"
INFRA = [p for p in CONTAINERS if p != WEB]
UNITS = sorted(p for p in SYSTEMD.iterdir() if p.suffix in {".service", ".slice", ".target"})
PROCESS_UNITS = [SYSTEMD / "protea-dev-worker@.service", SYSTEMD / "protea-dev-api.service"]

# The clock gate's own ceiling on its wait (protea-dev-clock-gate.sh).
GATE_CAP_SECONDS = 600

DATA_ROOT = "%h/protea-dev"
DISK_GATE = f"/bin/bash {DATA_ROOT}/deploy/PROTEA/deploy/dev/protea-dev-disk-gate.sh {DATA_ROOT} %n"


def _keys(path: Path) -> dict[str, list[str]]:
    """Every Key=value of a unit file, in order, duplicates kept."""
    found: dict[str, list[str]] = defaultdict(list)
    for line in path.read_text().splitlines():
        text = line.strip()
        if not text or text.startswith(("#", ";", "[")):
            continue
        key, sep, value = text.partition("=")
        if sep:
            found[key.strip()].append(value.strip())
    return found


def _environment(path: Path) -> dict[str, str]:
    env = {}
    for raw in _keys(path).get("Environment", []):
        name, _, value = raw.strip('"').partition("=")
        env[name] = value
    return env


def _seconds(value: str) -> int:
    match = re.fullmatch(r"(\d+)(s|min)?", value)
    assert match, value
    return int(match.group(1)) * (60 if match.group(2) == "min" else 1)


def test_there_is_something_to_check():
    assert {p.name for p in CONTAINERS} == {
        "protea-dev-minio.container",
        "protea-dev-postgres.container",
        "protea-dev-rabbitmq.container",
        "protea-dev-web.container",
    }
    assert set(PROCESS_UNITS) <= set(UNITS)


@pytest.mark.parametrize("needle", ["bioxaxi2", "Thesis2", "protea-lab-worker"])
def test_no_laptop_path_and_no_production_worker_name(needle):
    offenders = [
        str(p.relative_to(ROOT))
        for p in DEV.rglob("*")
        if p.is_file() and needle in p.read_text(errors="replace")
    ]
    assert offenders == []


def test_every_unit_is_a_protea_dev_unit():
    for path in [*CONTAINERS, *UNITS]:
        assert path.name.startswith("protea-dev"), path.name


@pytest.mark.parametrize("path", [*CONTAINERS, *PROCESS_UNITS], ids=lambda p: p.name)
def test_every_dev_process_is_capped_inside_the_dev_slice(path):
    keys = _keys(path)
    assert keys["Slice"] == ["protea-dev.slice"]
    assert len(keys["MemoryMax"]) == 1


@pytest.mark.parametrize(
    "path",
    [*CONTAINERS, *(p for p in UNITS if p.suffix == ".service")],
    ids=lambda p: p.name,
)
def test_every_dev_service_needs_the_dev_data_root(path):
    # The data root lives on the system disk, beside production (README, Disk).
    assert _keys(path)["AssertPathIsDirectory"] == [DATA_ROOT]
    assert "AssertPathIsMountPoint" not in _keys(path)


@pytest.mark.parametrize("path", [*CONTAINERS, *PROCESS_UNITS], ids=lambda p: p.name)
def test_every_dev_process_runs_the_disk_gate_first_and_does_not_loop(path):
    keys = _keys(path)
    assert keys["ExecStartPre"][0] == DISK_GATE
    assert keys["StartLimitBurst"] == ["3"]
    assert keys["StartLimitIntervalSec"] == ["900"]


def test_no_unit_names_a_mounted_dev_volume():
    for path in [*CONTAINERS, *UNITS]:
        assert "/srv/protea-dev" not in path.read_text(), path.name


def _bytes(value: str) -> int:
    match = re.fullmatch(r"(\d+)([KMG])", value)
    assert match, value
    return int(match.group(1)) * {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}[match.group(2)]


def test_the_always_on_processes_one_worker_and_the_web_fit_in_the_slice():
    caps = [
        _keys(QUADLET / "protea-dev-postgres.container")["MemoryMax"][0],
        _keys(QUADLET / "protea-dev-rabbitmq.container")["MemoryMax"][0],
        _keys(QUADLET / "protea-dev-minio.container")["MemoryMax"][0],
        _keys(SYSTEMD / "protea-dev-api.service")["MemoryMax"][0],
        _keys(SYSTEMD / "protea-dev-worker@.service")["MemoryMax"][0],
        _keys(WEB)["MemoryMax"][0],
    ]
    ceiling = _keys(SYSTEMD / "protea-dev.slice")["MemoryMax"][0]
    assert sum(_bytes(cap) for cap in caps) <= _bytes(ceiling)


def test_the_slice_is_the_dev_ceiling():
    keys = _keys(SYSTEMD / "protea-dev.slice")
    assert keys["MemoryMax"] == ["9G"]
    assert keys["CPUWeight"] == ["50"]


def test_postgres_is_sized_as_planned():
    keys = _keys(QUADLET / "protea-dev-postgres.container")
    assert keys["MemoryMax"] == ["3G"]
    (command,) = keys["Exec"]
    for setting in ("shared_buffers=768MB", "max_wal_size=8GB", "temp_file_limit=100GB"):
        assert f"-c {setting}" in command


def test_the_images_are_the_digests_docker_compose_pins():
    compose = (ROOT / "docker-compose.yml").read_text()
    pinned = dict(
        (digest, repo)
        for repo, digest in re.findall(r"image:\s*([^@\s]+)@sha256:([0-9a-f]{64})", compose)
    )

    def bare(repo: str) -> str:
        return re.sub(r"^(docker\.io/)?(library/)?", "", repo)

    for path in INFRA:
        (image,) = _keys(path)["Image"]
        repo, _, digest = image.partition("@sha256:")
        assert digest in pinned, path.name
        assert bare(repo) == bare(pinned[digest]), path.name


def test_the_web_image_is_built_from_bases_pinned_by_digest():
    # Built from the dev clone's apps/web, never pulled by a tag: the container names
    # its build unit, the build names the Containerfile, and every FROM is a digest.
    assert _keys(WEB)["Image"] == ["protea-dev-web.build"]
    build = _keys(QUADLET / "protea-dev-web.build")
    assert build["File"] == [f"{DATA_ROOT}/deploy/PROTEA/deploy/dev/web/Containerfile"]
    assert build["SetWorkingDirectory"] == [f"{DATA_ROOT}/deploy/PROTEA/apps/web"]
    assert build["Slice"] == ["protea-dev.slice"]
    bases = [
        line.split()[1]
        for line in (DEV / "web" / "Containerfile").read_text().splitlines()
        if line.startswith("FROM ")
    ]
    assert bases and all(re.search(r"@sha256:[0-9a-f]{64}$", base) for base in bases)


def test_the_web_listens_on_loopback_in_the_dev_range():
    # On the host's network, so it publishes nothing: its own variables bind it.
    env = _environment(WEB)
    assert env["HOSTNAME"] == "127.0.0.1"
    assert 26000 <= int(env["PORT"]) <= 26999
    assert env["PROTEA_INTERNAL_API_URL"] == "http://127.0.0.1:26800"


def _published() -> dict[int, str]:
    """Container port -> the host side it is published on, for all three."""
    out = {}
    for path in CONTAINERS:
        for spec in _keys(path)["PublishPort"]:
            host, port, container_port = spec.split(":")
            out[int(container_port)] = f"{host}:{port}"
    return out


def test_every_port_is_loopback_in_the_dev_range():
    for host_side in _published().values():
        host, port = host_side.split(":")
        assert host == "127.0.0.1"
        assert 26000 <= int(port) <= 26999


def test_the_readiness_gate_waits_on_the_published_ports():
    published = _published()
    expected = [published[5432], published[5672], published[9000]]
    env = _environment(SYSTEMD / "protea-dev-wait-infra.service")
    assert env["PROTEA_WAIT_ENDPOINTS"].split() == expected


@pytest.mark.parametrize("path", PROCESS_UNITS, ids=lambda p: p.name)
def test_dev_processes_are_clock_gated_and_stop_on_exit_78(path):
    keys = _keys(path)
    disk, clock = keys["ExecStartPre"]
    assert disk == DISK_GATE
    assert clock.split()[1].endswith("/deploy/dev/protea-dev-clock-gate.sh")
    assert keys["RestartPreventExitStatus"] == ["78"]
    (timeout,) = keys["TimeoutStartSec"]
    assert _seconds(timeout) > GATE_CAP_SECONDS
    assert keys["EnvironmentFile"] == ["%h/.secrets/protea-dev.env"]
    assert _environment(path)["PROTEA_ENVIRONMENT"] == "dev"


def test_the_gate_cap_here_is_the_scripts_cap():
    script = (DEV / "protea-dev-clock-gate.sh").read_text()
    assert f"budget > {GATE_CAP_SECONDS}" in script


def test_no_secret_value_is_written_into_a_unit():
    secret_names = re.compile(r"(PASSWORD|_PASS|SECRET|_KEY)\b", re.IGNORECASE)
    for path in [*CONTAINERS, *UNITS]:
        for name in _environment(path):
            assert not secret_names.search(name), f"{path.name}: {name}"
