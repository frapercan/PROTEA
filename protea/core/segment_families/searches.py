"""The MMseqs2 passes of the segment families (ADR-D65, decision 2), with the laboratory's settings.

``search`` runs three searches once (sensitivity 7.5, at most 1,000 targets per
query, E-value 0.1, the index in one split): the donor cores against themselves
with their alignments kept for the profiles, their reversed cores against them
(the decoys), and the target cores against them. Each is written as
``hits-<kind>.parquet``: query and target core indices, E-value, bits and both
coverages.

``families SCOPE`` applies ``protea.core.segment_families.algorithms`` at that
scope, building one profile per group of two or more from its representative's
alignments within the scope, searched against the representatives and the
reversed representatives. It writes ``families-<scope>.parquet`` (every segment
with its group, family, clan and mask) and its record.

The binary must hash to the sha256 the job names before it runs.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from protea.core.file_stage import file_sha256, read_record, write_record
from protea.core.segment_families import algorithms as fam
from protea.core.segment_families.algorithms import Hits
from protea.core.segment_families.build import FamiliesRefused

FORMAT = "query,target,evalue,bits,qstart,qend,qlen,tstart,tend,tlen"
COMMON = ["-s", "7.5", "--max-seqs", "1000", "-e", "0.1", "--split", "1"]
_M8_TYPES = {
    "query": pa.int64(),
    "target": pa.int64(),
    "evalue": pa.float64(),
    "bits": pa.float32(),
    **{c: pa.int32() for c in ("qstart", "qend", "qlen", "tstart", "tend", "tlen")},
}


@dataclass(frozen=True)
class Tool:
    """The pinned binary, its expected sha256 and the job's resources."""

    mmseqs: Path
    sha256: str
    threads: int
    tmp: Path


def checked_version(tool: Tool) -> dict[str, str]:
    found = file_sha256(tool.mmseqs)
    if found != tool.sha256:
        raise FamiliesRefused(f"{tool.mmseqs} hashes to {found}, not {tool.sha256}")
    version = subprocess.run(
        [str(tool.mmseqs), "version"], capture_output=True, text=True, check=True, timeout=60
    ).stdout.strip()
    return {"version": version, "binary_sha256": found}


def _run(tool: Tool, args: list[Any], out: Path, commands: list[list[str]]) -> float:
    """One MMseqs2 step, logged; its command kept with file names only; a search's temporary removed."""
    tool.tmp.mkdir(parents=True, exist_ok=True)
    command = [str(tool.mmseqs), *map(str, args)]
    commands.append([Path(p).name if "/" in p else p for p in command])
    started = time.monotonic()
    with open(out / "mmseqs.log", "a", encoding="utf-8") as log:
        log.write("$ " + " ".join(command) + "\n")
        log.flush()
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    if args[0] == "search":
        shutil.rmtree(args[4], ignore_errors=True)
    return round(time.monotonic() - started, 1)


def m8_to_hits(m8: Path, parquet: Path) -> int:
    """MMseqs2's tab file as hits (query, target, E-value, bits, coverages); the tab file removed."""
    names = FORMAT.split(",")
    if m8.stat().st_size == 0:  # no hit at all, which the decoys may well give
        empty = {
            "q": pa.int64(),
            "t": pa.int64(),
            "e": pa.float64(),
            "bits": pa.float32(),
            "qcov": pa.float32(),
            "tcov": pa.float32(),
        }
        pq.write_table(
            pa.table({k: pa.array([], type=v) for k, v in empty.items()}),
            parquet,
            compression="zstd",
        )
        m8.unlink()
        return 0
    table = pcsv.read_csv(
        m8,
        read_options=pcsv.ReadOptions(column_names=names),
        parse_options=pcsv.ParseOptions(delimiter="\t"),
        convert_options=pcsv.ConvertOptions(column_types=_M8_TYPES),
    )

    def cov(s: str, e: str, n: str) -> pa.Array:
        span = pc.add(pc.subtract(table.column(e), table.column(s)), 1)
        return pc.divide(pc.cast(span, pa.float32()), pc.cast(table.column(n), pa.float32()))

    hits = pa.table(
        {
            "q": table.column("query"),
            "t": table.column("target"),
            "e": table.column("evalue"),
            "bits": table.column("bits"),
            "qcov": cov("qstart", "qend", "qlen"),
            "tcov": cov("tstart", "tend", "tlen"),
        }
    )
    pq.write_table(hits, parquet, compression="zstd")
    m8.unlink()
    return hits.num_rows


def read_hits(path: Path) -> Hits:
    t = pq.read_table(path)
    return Hits(*(t.column(c).to_numpy() for c in ("q", "t", "e", "bits", "qcov", "tcov")))


def search_pass(out: Path, tool: Tool) -> dict[str, Any]:
    """The three searches of the donors' cores, written as hits, and ``search.json``."""
    mmseqs = checked_version(tool)
    db, seconds = out / "db", {}
    commands: list[list[str]] = []
    db.mkdir(exist_ok=True)
    t = ["--threads", tool.threads]
    _run(tool, ["createdb", out / "donors.fasta", db / "nuc", "--compressed", "1"], out, commands)
    _run(tool, ["reverseseq", db / "nuc", db / "inv", "--compressed", "1"], out, commands)
    _run(tool, ["createdb", out / "targets.fasta", db / "qry", "--compressed", "1"], out, commands)
    counts = {}
    for kind, query, extra in (
        ("real", "nuc", ["-a"]),
        ("decoy", "inv", []),
        ("queries", "qry", []),
    ):
        aln = db / f"aln-{kind}"
        args = [
            "search",
            db / query,
            db / "nuc",
            aln,
            tool.tmp / kind,
            *extra,
            *COMMON,
            *t,
            "--compressed",
            "1",
        ]
        seconds[kind] = _run(tool, args, out, commands)
        _run(
            tool,
            [
                "convertalis",
                db / query,
                db / "nuc",
                aln,
                out / f"{kind}.m8",
                "--format-output",
                FORMAT,
                *t,
            ],
            out,
            commands,
        )
        counts[kind] = m8_to_hits(out / f"{kind}.m8", out / f"hits-{kind}.parquet")
    record = {
        "pass": "search",
        "mmseqs": mmseqs,
        "commands": commands,
        "seconds": seconds,
        "hits": counts,
    }
    write_record(out / "search.json", record)
    return record


def _lookup(db: Path) -> dict[int, str]:
    """Core index -> MMseqs2's internal key, from the donors' database lookup."""
    out = {}
    for line in (db / "nuc.lookup").read_text().splitlines():
        key, name, _ = line.split("\t")
        out[int(name)] = key
    return out


def _profile_hits(
    out: Path,
    tool: Tool,
    scope: str,
    reps: tuple[list[int], list[int], float],
    commands: list[list[str]],
) -> tuple[Hits, Hits]:
    """Profiles of the groups of two or more against the representatives and their reverses."""
    db, t = out / "db", ["--threads", tool.threads]
    representatives, profiled, emax = reps
    key = _lookup(db)
    (out / f"reps-{scope}.txt").write_text("".join(f"{key[r]}\n" for r in representatives))
    (out / f"reps-prof-{scope}.txt").write_text("".join(f"{key[r]}\n" for r in profiled))
    rep, inv, prof = db / f"rep-{scope}", db / f"repinv-{scope}", db / f"prof-{scope}"
    _run(tool, ["createsubdb", out / f"reps-{scope}.txt", db / "nuc", rep], out, commands)
    _run(tool, ["reverseseq", rep, inv], out, commands)
    _run(
        tool,
        ["createsubdb", out / f"reps-prof-{scope}.txt", db / "aln-real", db / f"alnrep-{scope}"],
        out,
        commands,
    )
    _run(
        tool,
        [
            "result2profile",
            db / "nuc",
            db / "nuc",
            db / f"alnrep-{scope}",
            prof,
            "--e-profile",
            emax,
            *t,
        ],
        out,
        commands,
    )
    found = []
    for name, target in (("prof", rep), ("profinv", inv)):
        aln = db / f"aln{name}-{scope}"
        _run(
            tool,
            ["search", prof, target, aln, tool.tmp / f"{name}-{scope}", *COMMON, *t],
            out,
            commands,
        )
        _run(
            tool,
            ["convertalis", prof, target, aln, out / f"{name}.m8", "--format-output", FORMAT, *t],
            out,
            commands,
        )
        m8_to_hits(out / f"{name}.m8", out / f"hits-{name}-{scope}.parquet")
        found.append(read_hits(out / f"hits-{name}-{scope}.parquet"))
    return found[0], found[1]


def _segment_table(
    out: Path, columns: dict[str, tuple[np.ndarray, np.ndarray, Any]], scope: str
) -> None:
    segments = pq.read_table(out / "segments.parquet")
    donor = segments.column("donor").to_numpy(zero_copy_only=False)
    index = segments.column("index").to_numpy()
    extra = {}
    for name, (on_donors, on_targets, fill) in columns.items():
        values = np.full(len(index), fill, dtype=on_donors.dtype)
        values[donor] = on_donors[index[donor]]
        values[~donor] = on_targets[index[~donor]]
        extra[name] = values
    table = segments.append_column("scope", pa.array([scope] * len(index)))
    for name, values in extra.items():
        table = table.append_column(name, pa.array(values))
    pq.write_table(table, out / f"families-{scope}.parquet", compression="zstd")


def _donor_families(
    out: Path, tool: Tool, scope: str, n_d: int, commands: list[list[str]]
) -> tuple[np.ndarray, np.ndarray, np.ndarray, Hits, dict[str, Any]]:
    """(family, group, masked, partial links, counts) of every donor core at ``scope``."""
    emax = fam.scope_evalue(scope, n_d)
    masked = fam.decoy_mask(read_hits(out / "hits-decoy.parquet"), n_d)
    full, partial = fam.links(read_hits(out / "hits-real.parquet"), emax, masked)
    group = fam.set_cover(
        [k for k in range(n_d) if not masked[k]], zip(full.q.tolist(), full.t.tolist(), strict=True)
    )
    size = Counter(group.values())
    reps = sorted(size)
    profiled = [r for r in reps if size[r] >= 2]
    nominal = (
        fam.STRICT_CONSTANT / max(len(profiled), 1) if scope == "strict" else fam.FULL_BUDGET / n_d
    )
    e_profile, family_of_rep = nominal, {r: r for r in reps}
    if profiled:
        prof, prof_rev = _profile_hits(out, tool, scope, (reps, profiled, emax), commands)
        e_profile = fam.profile_evalue(prof_rev, nominal)
        family_of_rep = fam.join_families(reps, prof, e_profile)
    family = np.full(n_d, -1, dtype=np.int64)
    group_of = np.full(n_d, -1, dtype=np.int64)
    for k, r in group.items():
        family[k], group_of[k] = family_of_rep[r], r
    counts = {
        "evalue": emax,
        "masked_cores": int(masked.sum()),
        "links": len(full),
        "partial_links": len(partial),
        "groups": len(reps),
        "singleton_groups": sum(1 for r in reps if size[r] == 1),
        "profile_step": {"profiles": len(profiled), "nominal": nominal, "evalue": e_profile},
        "families": len(set(family_of_rep.values())),
    }
    return family, group_of, masked, partial, counts


def families_pass(out: Path, tool: Tool, scope: str) -> dict[str, Any]:
    """Families at ``scope``: ``families-<scope>.parquet`` and ``families-<scope>.json``."""
    started, mmseqs = time.monotonic(), checked_version(tool)
    commands: list[list[str]] = []
    prepare = read_record(out / "prepare.json")
    n_d, n_t = int(prepare["donor_cores"]), int(prepare["target_cores"])
    family, group_of, masked, partial, counts = _donor_families(out, tool, scope, n_d, commands)
    clan_of = fam.clans(family, partial)
    target_family = fam.target_families(
        read_hits(out / "hits-queries.parquet"), counts["evalue"], masked, family, n_t
    )

    def clan(f: np.ndarray) -> np.ndarray:
        return np.array([clan_of.get(int(x), int(x)) if x >= 0 else -1 for x in f], dtype=np.int64)

    _segment_table(
        out,
        {
            "group": (group_of, np.full(n_t, -1, dtype=np.int64), -1),
            "family": (family, target_family, -1),
            "clan": (clan(family), clan(target_family), -1),
            "masked": (masked, np.zeros(n_t, dtype=bool), False),
        },
        scope,
    )
    sizes = Counter(family[family >= 0].tolist())
    in_big = [sizes.get(int(f), 0) >= 2 for f in family]
    record = {
        "pass": "families",
        "scope": scope,
        "mmseqs": mmseqs,
        "commands": commands,
        **counts,
        "families_of_two_or_more": sum(1 for n in sizes.values() if n >= 2),
        "donor_cores_in_a_family_of_two_or_more": float(np.mean(in_big)) if n_d else 0.0,
        "target_cores_with_a_family": float((target_family >= 0).mean()) if n_t else 0.0,
        "seconds": round(time.monotonic() - started, 1),
    }
    write_record(out / f"families-{scope}.json", record)
    return record


__all__ = ["COMMON", "FORMAT", "Tool", "families_pass", "m8_to_hits", "read_hits", "search_pass"]
