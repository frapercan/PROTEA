"""Write the corpus record, its tables and its figures from the cut photo manifests.

Usage:
    python scripts/docs/corpus_figures.py [--pdf DIR] PHOTO_JSON [PHOTO_JSON ...]

Each argument is the manifest of one cut photo (``photo.json``, ADR-D59). Nothing is
typed by hand: every number on the corpus page is read from a manifest, and the record
keeps the sha256 of the manifest it came from, so a reader can check any figure against
the photo it describes. Re-running the script on the same manifests writes the same
bytes.

Writes, under ``docs/source``:
  _data/corpus-by-cut.json             every count, per cut, with its manifest's sha256
  _data/corpus-members.csv             the members table
  _data/corpus-rows.csv                the GOA rows table, by evidence channel
  _data/corpus-manifests.csv           each photo's manifest and its sha256
  _static/figures/corpus-members-{light,dark}.svg
  _static/figures/corpus-rows-{light,dark}.svg
and, with ``--pdf DIR``, the light figures as PDF in DIR, for the thesis.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parents[2] / "docs" / "source"
DATA, FIGURES = DOCS / "_data", DOCS / "_static" / "figures"

#: The evidence channels drawn, in ADR-D59's order. ``no_data`` (ND) is under 0.3 % of
#: the rows of every cut and is left to the table.
CHANNELS = (
    ("member_rows_truth", "Truth (experimental)"),
    ("member_rows_curated", "Curated"),
    ("member_rows_phylogenetic", "Phylogenetic (PAINT)"),
    ("member_rows_orthology_electronic", "Orthology, electronic"),
    ("member_rows_reviewed_field_electronic", "Reviewed fields, electronic"),
    ("member_rows_signature_electronic", "Signatures, electronic (mirror only)"),
)

#: The members of a photo, split into three parts that add up to all of them (ADR-D59):
#: a UniRef50 neighbour is in no other set, so it always lies outside the superset, and
#: what remains outside was admitted by a truth or curated row on an unreviewed entry.
MEMBER_PARTS = (
    (
        "Swiss-Prot at the cut (the proteins to annotate)",
        "Swiss-Prot at the cut",
        lambda c: c["members"]["superset"],
    ),
    (
        "Unreviewed, with experimental or curated rows",
        "Unreviewed, truth or curated",
        lambda c: c["members"]["outside_superset"] - c["members"]["by_neighbour"],
    ),
    (
        "UniRef50 neighbours (donors only)",
        "UniRef50 neighbours",
        lambda c: c["members"]["by_neighbour"],
    ),
)

#: The reference palette of the chart guide: categorical slots in fixed order, both
#: modes, with each mode's surface and text inks.
MODES = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "muted": "#52514e",
        "grid": "#e4e3df",
        "series": ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"),
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#f0efec",
        "muted": "#c3c2b7",
        "grid": "#383835",
        "series": ("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c"),
    },
}


def read_manifests(paths: list[Path]) -> list[dict]:
    """One entry per photo: cut, kind, date, the manifest's sha256 and its counts."""
    cuts = []
    for path in paths:
        raw = path.read_bytes()
        photo = json.loads(raw)
        counts = photo["counts"]
        cuts.append(
            {
                "cut_release": photo["cut_release"],
                "kind": photo["kind"],
                "cut_date": photo["cut_date"],
                "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "members": {
                    k: counts["members"][k]
                    for k in (
                        "members",
                        "superset",
                        "outside_superset",
                        "by_truth",
                        "by_curated_only",
                        "by_neighbour",
                    )
                },
                "rows": {k: v for k, v in counts["rows"].items() if k.startswith("member_rows")},
            }
        )
    return sorted(cuts, key=lambda c: c["cut_release"])


def write_tables(cuts: list[dict]) -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    record = {"source": "cut photo manifests (ADR-D59), one entry per photo", "cuts": cuts}
    (DATA / "corpus-by-cut.json").write_text(json.dumps(record, indent=1) + "\n")
    # The two count tables are written with one column per cut, so they stay narrow
    # enough to read on a page: a set or a channel per row, a cut per column.
    head = ["", *[f"{c['cut_release']} ({c['cut_date'][:4]})" for c in cuts]]
    member_rows = [
        ("Members", "members"),
        ("Swiss-Prot at the cut", "superset"),
        ("Outside Swiss-Prot", "outside_superset"),
        ("Admitted by truth", "by_truth"),
        ("Admitted by curation only", "by_curated_only"),
        ("UniRef50 neighbours", "by_neighbour"),
    ]
    with (DATA / "corpus-members.csv").open("w", newline="") as fh:
        out = csv.writer(fh, lineterminator="\n")
        out.writerow(head)
        for label, key in member_rows:
            out.writerow([label, *[f"{c['members'][key]:,}" for c in cuts]])
    with (DATA / "corpus-rows.csv").open("w", newline="") as fh:
        out = csv.writer(fh, lineterminator="\n")
        out.writerow(head)
        out.writerow(["All rows", *[f"{c['rows']['member_rows']:,}" for c in cuts]])
        for key, label in (*CHANNELS, ("member_rows_no_data", "No data (ND)")):
            out.writerow([label, *[f"{c['rows'][key]:,}" for c in cuts]])
    with (DATA / "corpus-manifests.csv").open("w", newline="") as fh:
        out = csv.writer(fh, lineterminator="\n")
        out.writerow(["Cut", "Photo", "Date", "Manifest sha256, first 16 hex digits"])
        for c in cuts:
            out.writerow(
                [
                    c["cut_release"],
                    c["kind"].replace("_", " "),
                    c["cut_date"],
                    c["manifest_sha256"][:16],
                ]
            )


def _axes(plt, palette: dict):
    """One chart surface in the mode's colours. The title is the caption's job, in the
    documentation and in the thesis alike, so the figure carries none."""
    fig, ax = plt.subplots(figsize=(7.2, 3.8), dpi=100)
    fig.patch.set_facecolor(palette["surface"])
    ax.set_facecolor(palette["surface"])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(palette["muted"])
    ax.tick_params(colors=palette["muted"], length=0)
    ax.yaxis.grid(True, color=palette["grid"], linewidth=0.8)
    ax.set_axisbelow(True)
    return fig, ax


def _stacked(
    ax, palette: dict, cuts: list[dict], layers: list[tuple[str, str, list[float]]], direct: bool
) -> None:
    """Stacked columns, one per cut, a 2px surface gap between segments, the total on top.

    Each layer is (legend label, short label, values). With ``direct`` (three series or
    fewer), each segment of the last column is also named beside it by its short label, so
    identity never rests on colour alone; the legend stays.
    """
    xs = range(len(cuts))
    base = [0.0] * len(cuts)
    for colour, (label, short, values) in zip(palette["series"], layers, strict=False):
        ax.bar(
            xs,
            values,
            bottom=base,
            width=0.3,
            color=colour,
            edgecolor=palette["surface"],
            linewidth=2,
            label=label,
        )
        if direct:
            last = len(cuts) - 1
            ax.text(
                last + 0.22,
                base[last] + values[last] / 2,
                short,
                ha="left",
                va="center",
                fontsize=8.5,
                color=palette["ink"],
            )
        base = [b + v for b, v in zip(base, values, strict=True)]
    for x, total in zip(xs, base, strict=True):
        ax.text(
            x, total, f"{total:.2f} M", ha="center", va="bottom", fontsize=9, color=palette["ink"]
        )
    ax.set_xticks(list(xs), [f"{c['cut_release']}\n{c['cut_date'][:4]}" for c in cuts])
    if direct:
        ax.set_xlim(-0.5, len(cuts) + 0.7)
    ax.set_ylabel("millions", color=palette["muted"])
    legend = ax.legend(frameon=False, fontsize=8.5, loc="upper left", ncol=1 if direct else 2)
    for text in legend.get_texts():
        text.set_color(palette["ink"])


def draw(cuts: list[dict], pdf_dir: Path | None) -> None:
    import matplotlib

    matplotlib.use("svg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"svg.hashsalt": "protea-corpus", "svg.fonttype": "none", "font.size": 9})
    full = [c for c in cuts if c["kind"] == "full"]
    FIGURES.mkdir(parents=True, exist_ok=True)
    for mode, palette in MODES.items():
        fig, ax = _axes(plt, palette)
        _stacked(
            ax,
            palette,
            full,
            [(label, short, [part(c) / 1e6 for c in full]) for label, short, part in MEMBER_PARTS],
            direct=True,
        )
        _save(fig, f"corpus-members-{mode}", mode, pdf_dir)
        fig, ax = _axes(plt, palette)
        _stacked(
            ax,
            palette,
            full,
            [(label, label, [c["rows"][k] / 1e6 for c in full]) for k, label in CHANNELS],
            direct=False,
        )
        _save(fig, f"corpus-rows-{mode}", mode, pdf_dir)
        plt.close("all")


def _save(fig, stem: str, mode: str, pdf_dir: Path | None) -> None:
    fig.tight_layout()
    fig.savefig(FIGURES / f"{stem}.svg", metadata={"Date": None, "Creator": None})
    if mode == "light" and pdf_dir is not None:
        pdf_dir.mkdir(parents=True, exist_ok=True)
        fig.savefig(
            pdf_dir / f"{stem.removesuffix('-light')}.pdf",
            metadata={"CreationDate": None, "Creator": None, "Producer": None},
        )


def main() -> int:
    args = sys.argv[1:]
    pdf_dir = None
    if args[:1] == ["--pdf"]:
        pdf_dir, args = Path(args[1]), args[2:]
    if not args:
        raise SystemExit(__doc__)
    cuts = read_manifests([Path(p) for p in args])
    write_tables(cuts)
    draw(cuts, pdf_dir)
    for c in cuts:
        print(c["cut_release"], c["kind"], c["members"]["members"], c["manifest_sha256"][:12])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
