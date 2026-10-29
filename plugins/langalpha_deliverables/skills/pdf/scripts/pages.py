#!/usr/bin/env python3
"""Merge, split, rotate, encrypt and decrypt PDFs through qpdf, then count the result.

Usage:
    python pages.py merge   --out <out.pdf> <in1.pdf> <in2.pdf> [...]
    python pages.py split   <in.pdf> --ranges 1-3,4-6 [--out DIR]
    python pages.py rotate  <in.pdf> --out <out.pdf> --angle 90 [--pages 1-3] [--absolute]
    python pages.py encrypt <in.pdf> --out <out.pdf> --user-password PW [--owner-password PW]
                            [--print full|low|none]
                            [--modify all|annotate|form|assembly|none] [--extract yes|no]
    python pages.py decrypt <in.pdf> --out <out.pdf> --password PW

qpdf does the page work because it carries AcroForm fields across a merge, which
pypdf's `append` does not: pypdf collapses two same-named fields into one, so the
two copies then fill as a single field. qpdf keeps both and renames the second to
`name+1`; the report lists every renamed field so the values file can be written
against the merged document.

`--ranges` items are separated by commas, one output file each. Use semicolons
when a single output needs a comma of its own: `1,3;2,4` makes two files. Two
items that would write the same file are refused rather than run: the second
overwrites the first and every count still adds up. A destination already taken
by a directory is refused the same way, before any of the set moves into place:
the files move one at a time, so a destination found unwritable partway through
would leave the set half replaced. `covers_every_page` is the union of the
ranges against the input, with `pages_missing` and `pages_repeated` naming the
pages no range asked for and the pages more than one did.

Encryption is AES-256 and nothing else. Only the owner password can override a
denied permission, so `--print`, `--modify` or `--extract` below their defaults
are refused unless `--owner-password` is given and differs from the user
password: reusing the user password there would hand every reader the key to the
restriction. With nothing denied there is no restriction to guard and an omitted
owner password reuses the user password, because the empty one qpdf would
otherwise see means "no owner password at all".

Every output is written to a fresh temporary sibling and moved onto the requested
path only after qpdf wrote it and pypdf could open it, so a file left over from an
earlier run is never reported as this run's result.

Every operation here rewrites the file, so a digital signature made over the input stops
verifying in the output. `signatures_present` names the signed fields on each side; it is a
report, not a gate, because the page work is still the work that was asked for. Deciding what
to do about a signed document is `forms.py`, which refuses to write one without
--drop-signatures.

Each subcommand takes only the options listed against it above. An option another
subcommand owns, a repeated option, and an option whose value is missing or is itself
an option are all refused before anything is written.

Inputs are never modified in place and every output path is checked against every
input. Exit code is 1 on a hard error or a page-count mismatch.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from pypdf import PdfReader
from pypdf.generic import DictionaryObject

# qpdf's own exit codes: 0 clean, 2 an error, 3 warnings it recovered from with the
# output still written. A run that only warns is a usable result, an error never is.
QPDF_WARNING = 3

PRINT = {"full": "--print=full", "low": "--print=low", "none": "--print=none"}
MODIFY = {
    "all": "--modify=all",
    "annotate": "--modify=annotate",
    "form": "--modify=form",
    "assembly": "--modify=assembly",
    "none": "--modify=none",
}
# Spelled out like the other two so the same typo that would be caught there is caught
# here: anything read as "not yes" takes extraction away, which is the opposite of what
# --extract true or --extract yse was asking for.
EXTRACT = {"yes": True, "no": False}


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def pdf_reader(path: Path) -> PdfReader:
    """A PdfReader over a file that may not be a readable PDF at all.

    pypdf raises out of the constructor on an empty, truncated or non-PDF file, and a
    traceback is not this script's contract: every other failure here is a JSON report.
    """
    try:
        return PdfReader(str(path))
    except Exception as exc:
        fail(f"cannot read {path} as a PDF: {type(exc).__name__}: {exc}", unreadable=True)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def qpdf(args: list[str], what: str, expect: Path | None = None) -> str:
    """Run qpdf writing to `expect`, which must be the last argument, and prove this run wrote it.

    The output is staged in a temporary sibling and moved into place only once it exists,
    is not empty and opens with pypdf. Checking `expect` directly would accept a file an
    earlier run left there, so a qpdf that wrote nothing would still look like a success.
    """
    if shutil.which("qpdf") is None:
        fail("qpdf is not on PATH")
    staged: Path | None = None
    if expect is not None:
        if args[-1] != str(expect):
            fail(f"{what}: the qpdf command does not end with its output path")
        expect.parent.mkdir(parents=True, exist_ok=True)
        handle, name = tempfile.mkstemp(dir=expect.parent, prefix=f".{expect.name}.", suffix=".tmp")
        os.close(handle)
        staged = Path(name)
        args = args[:-1] + [str(staged)]
    try:
        proc = subprocess.run(["qpdf"] + args, capture_output=True, text=True, timeout=900)
        wrote = staged is None or (staged.exists() and staged.stat().st_size > 0)
        if proc.returncode not in (0, QPDF_WARNING):
            fail(f"{what} failed: {proc.stderr.strip()[:500]}")
        if not wrote:
            fail(f"{what} produced no output at {expect}")
        if staged is not None:
            try:
                PdfReader(str(staged))
            except Exception as exc:
                fail(f"{what} wrote a file that does not open as a PDF: {exc}")
            os.replace(staged, expect)
            staged = None
        return proc.stderr.strip()
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)


def read_pdf(path: Path, password: str | None = None) -> PdfReader:
    reader = pdf_reader(path)
    if reader.is_encrypted:
        # A broken /Encrypt dictionary raises out of decrypt rather than returning 0,
        # and an unreadable file is a report like any other.
        try:
            opened = reader.decrypt(password or "")
        except Exception as exc:
            fail(f"cannot decrypt {path}: {type(exc).__name__}: {exc}", password_required=True)
        if not opened:
            fail(f"{path} is encrypted; supply the password", password_required=True)
    return reader


def inherited(node, key: str):
    """A field entry, from the field itself or from the group it sits in."""
    seen = 0
    while node is not None and seen < 32:
        if key in node:
            value = node[key]
            return value.get_object() if hasattr(value, "get_object") else value
        parent = node.get("/Parent")
        node = parent.get_object() if parent is not None else None
        seen += 1
    return None


def signed_fields(reader: PdfReader) -> list[str]:
    """Names of the signature fields that hold a signature rather than an empty placeholder.

    A signature is made over the bytes of the file it was signed in, so it no longer verifies
    once qpdf has written a new one. An empty placeholder signs nothing and is not listed.
    """
    try:
        form = reader.root_object.get("/AcroForm")
        form = form.get_object() if form is not None else None
    except Exception:
        return []
    if form is None:
        return []
    found: list[str] = []

    def walk(ref, prefix: str, depth: int) -> None:
        if depth > 24:
            return
        node = ref.get_object()
        name = str(node.get("/T") or "")
        qualified = f"{prefix}.{name}" if prefix and name else (name or prefix)
        kids = node.get("/Kids")
        children = [k for k in kids.get_object()] if kids is not None else []
        named = [k for k in children if "/T" in k.get_object()]
        if named:
            for kid in named:
                walk(kid, qualified, depth + 1)
            return
        value = inherited(node, "/V")
        if (
            str(inherited(node, "/FT") or "") == "/Sig"
            and isinstance(value, DictionaryObject)
            and "/ByteRange" in value
            and "/Contents" in value
        ):
            found.append(qualified)

    for ref in form.get("/Fields", []) or []:
        walk(ref, "", 0)
    return sorted(found)


def summary(path: Path, password: str | None = None) -> dict:
    reader = read_pdf(path, password)
    fields = {}
    try:
        fields = reader.get_fields() or {}
    except Exception:
        fields = {}
    return {
        "file": str(path),
        "sha256": sha256(path),
        "pages": len(reader.pages),
        "form_fields": sorted(fields.keys()),
        "signatures_present": signed_fields(reader),
        "encrypted": reader.is_encrypted,
    }


def check_distinct(out: Path, inputs: list[Path]) -> None:
    for src in inputs:
        if out.resolve() == src.resolve():
            fail(f"output {out} is also an input; inputs are never modified in place")


def merge(inputs: list[Path], out: Path) -> dict:
    for src in inputs:
        if not src.exists():
            fail(f"file not found: {src}")
    if len(inputs) < 2:
        fail("merge needs at least two input files")
    check_distinct(out, inputs)
    before = [summary(p) for p in inputs]
    out.parent.mkdir(parents=True, exist_ok=True)
    # The first input is the base document, so its AcroForm /DA and /DR survive;
    # `--empty --pages` drops them and leaves fields without a default appearance.
    warning = qpdf([str(inputs[0]), "--pages"] + [str(p) for p in inputs] + ["--", str(out)], "qpdf --pages merge", out)
    after = summary(out)
    expected = sum(b["pages"] for b in before)
    input_names = {n for b in before for n in b["form_fields"]}
    output_names = set(after["form_fields"])
    report = {
        "status": "ok" if after["pages"] == expected else "page_count_mismatch",
        "operation": "merge",
        "inputs": before,
        "output": after,
        "pages_expected": expected,
        "pages_actual": after["pages"],
        "form_fields": {
            "per_input": [len(b["form_fields"]) for b in before],
            "output": len(output_names),
            "renamed_by_qpdf": sorted(output_names - input_names),
            "missing_from_output": sorted(input_names - output_names),
        },
        "qpdf_warnings": warning,
        "notes": [
            "Run forms.py inspect on the merged file before writing values: colliding field names "
            "are renamed, so the names in the merged document are not the names in the inputs."
        ],
    }
    return report


def page_at(token: str, pages_in: int) -> int | None:
    """One end of a qpdf range: a page number, `z` for the last page, `rN` for the Nth from the end."""
    token = token.strip().lower()
    if token == "z":
        return pages_in
    if token.startswith("r") and token[1:].isdigit():
        page = pages_in - int(token[1:]) + 1
    elif token.isdigit():
        page = int(token)
    else:
        return None
    return page if 1 <= page <= pages_in else None


def expand_range(part: str, pages_in: int) -> list[int] | None:
    """The source pages one qpdf range selects, in order, or None for a spec not modelled here.

    Coverage is a claim about which pages came out, and that cannot be summed from page
    counts: `1-2,2-3` counts four pages of a four-page file while page 4 never left. qpdf
    owns this grammar, so an unfamiliar spec reports coverage as unknown rather than
    guessing at it, and every expansion is checked against the page count qpdf wrote.
    """
    groups, _, modifier = part.partition(":")
    if modifier and modifier.lower() not in ("even", "odd"):
        return None
    selected: list[int] = []
    for index, token in enumerate(groups.split(",")):
        token = token.strip()
        # qpdf refuses a leading exclusion as well: there is nothing yet to exclude from.
        if not token or (token[0] in "xX" and index == 0):
            return None
        exclude = token[0] in "xX"
        ends = [page_at(end, pages_in) for end in (token[1:] if exclude else token).split("-")]
        if len(ends) > 2 or any(end is None for end in ends):
            return None
        first, last = ends[0], ends[-1]
        step = 1 if last >= first else -1
        group = list(range(first, last + step, step))
        selected = [p for p in selected if p not in set(group)] if exclude else selected + group
    if modifier:
        # The modifier counts positions in the whole selection, not page numbers, and only
        # ever sits at the end: `1,3,4:even` is the second of the three, so page 3 alone.
        selected = selected[(0 if modifier.lower() == "odd" else 1)::2]
    return selected


def split(src: Path, ranges: str, out_dir: Path) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    parts = [p.strip() for p in (ranges.split(";") if ";" in ranges else ranges.split(",")) if p.strip()]
    if not parts:
        fail("split needs --ranges, for example 1-3,4-6")
    before = summary(src)
    pages_in = before["pages"]

    # One output file per range, so two ranges naming the same file means the second
    # silently overwrites the first while both still count towards the totals.
    plan: list[tuple[str, Path]] = []
    claimed: dict[Path, str] = {}
    collisions = []
    for part in parts:
        target = out_dir / f"{src.stem}_p{re.sub(r'[^0-9A-Za-z-]+', '_', part)}.pdf"
        if target in claimed:
            collisions.append({"range": part, "collides_with": claimed[target], "output": str(target)})
        claimed.setdefault(target, part)
        plan.append((part, target))
    if collisions:
        fail(
            "two ranges write the same file: "
            + "; ".join(f"{c['range']!r} and {c['collides_with']!r} both write {c['output']}" for c in collisions)
            + ". The second would overwrite the first while both still counted, so ask for each "
            "range once, or give the repeat its own --out directory.",
            duplicate_outputs=collisions,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    # The set moves into place one file at a time, so a destination that cannot take its
    # file has to be found before the first one moves: found on the second instead, the
    # first has already overwritten what was there and the refusal comes too late to undo.
    for part, target in plan:
        check_distinct(target, [src])
        if target.is_dir():
            fail(
                f"range {part!r} writes {target}, which already exists as a directory. Nothing was "
                "written; remove it, or give --out a directory of its own.",
                blocked_output=str(target),
            )
        if not os.access(target.parent, os.W_OK):
            fail(
                f"range {part!r} writes {target}, but {target.parent} cannot be written to. Nothing "
                "was written; give --out a directory this process can write to.",
                blocked_output=str(target),
            )
    # Every range is written into a staging directory first and the set moves into place
    # only once the last one exists, so a range qpdf refuses part way through the plan
    # leaves an earlier split set on disk untouched rather than half replaced.
    stage = Path(tempfile.mkdtemp(dir=out_dir, prefix=".split-"))
    outputs = []
    coverage_known = True
    try:
        for part, target in plan:
            staged = stage / target.name
            qpdf([str(src), "--pages", str(src), part, "--", str(staged)], f"qpdf --pages {part}", staged)
        # The checks above read the directory a moment before these moves, so something that
        # changes in between can still refuse one. Each file about to be overwritten is copied
        # into the staging directory first, and a move that fails puts the earlier ones back.
        replaced: list[tuple[Path, Path | None]] = []
        try:
            for _, target in plan:
                previous = stage / f"{target.name}.previous" if target.is_file() else None
                if previous is not None:
                    shutil.copy2(target, previous)
                os.replace(stage / target.name, target)
                replaced.append((target, previous))
        except OSError as exc:
            for done, previous in reversed(replaced):
                if previous is not None:
                    os.replace(previous, done)
                else:
                    done.unlink(missing_ok=True)
            fail(
                f"could not write {target}: {exc}. Every file this split had already replaced was "
                "put back, so the split set on disk is the one that was there before.",
                blocked_output=str(target),
            )
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    for part, target in plan:
        info = summary(target)
        info["range"] = part
        selected = expand_range(part, pages_in)
        if selected is not None and len(selected) == info["pages"]:
            info["pages_selected"] = selected
        else:
            coverage_known = False
        outputs.append(info)
    total = sum(o["pages"] for o in outputs)
    seen: dict[int, int] = {}
    for output in outputs:
        for page in output.get("pages_selected", []):
            seen[page] = seen.get(page, 0) + 1
    missing = [p for p in range(1, pages_in + 1) if p not in seen]
    repeated = sorted(p for p, times in seen.items() if times > 1)
    coverage = {
        "pages_selected": sorted(seen),
        "covers_every_page": not missing,
        "pages_missing": missing,
        "pages_repeated": repeated,
    } if coverage_known else {"pages_selected": None, "covers_every_page": None}
    return {
        "status": "ok",
        "operation": "split",
        "input": before,
        "ranges": parts,
        "outputs": outputs,
        "pages_in": pages_in,
        "pages_out_total": total,
        **coverage,
        "notes": [
            "covers_every_page is the union of the ranges against 1..pages_in, not their page "
            "counts: pages_missing names what no range asked for and pages_repeated what more than "
            "one did. Overlapping or partial ranges are legitimate, so read both against what was "
            "asked for."
            if coverage_known
            else "covers_every_page is null: one of these ranges is not in the subset of qpdf's "
            "grammar expanded here, so which pages came out was not computed. Read the per-output "
            "page counts instead.",
        ],
    }


def parse_angle(value) -> int:
    """A page turns in quarter circles, so anything else is a typo rather than a request.

    Converted here rather than at the call site: `int("x")` raises where nothing catches it,
    and a traceback is not what a caller reading this script's JSON can act on.
    """
    try:
        angle = int(str(value))
    except ValueError:
        fail(f"--angle must be a whole number of degrees, not {value!r}")
    if angle % 90 != 0:
        fail(f"--angle must be a multiple of 90; {angle} is not a quarter turn")
    return angle


def rotate(src: Path, out: Path, angle: int, pages: str | None, absolute: bool) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    # An empty --pages would drop out of the spec below and turn every page, which is the
    # opposite of what a caller asking for a selection meant. qpdf validates the rest of the
    # range grammar (`1-3`, `z`, `r2-r1`, `1-z:even`) and reports what it rejected.
    if pages is not None and not pages.strip():
        fail("--pages selects no page; give a page range such as 1-3, or drop --pages to rotate every page")
    check_distinct(out, [src])
    before = read_pdf(src)
    rotations_before = [int(p.rotation or 0) for p in before.pages]
    # qpdf's grammar is [+|-]angle with angle one of 0/90/180/270, the sign alone meaning
    # relative. So a negative angle keeps its own sign rather than gaining a "+", and both
    # forms reduce into that set: a bare "-90" would turn the page, not set it to 270.
    magnitude = abs(angle) % 360
    turn = str(angle % 360) if absolute else f"{-magnitude if angle < 0 else magnitude:+d}"
    spec = turn + (f":{pages}" if pages else "")
    out.parent.mkdir(parents=True, exist_ok=True)
    qpdf([str(src), f"--rotate={spec}", "--", str(out)], f"qpdf --rotate={spec}", out)
    after = read_pdf(out)
    rotations_after = [int(p.rotation or 0) for p in after.pages]
    changed = [
        {"page": i + 1, "before": b, "after": a}
        for i, (b, a) in enumerate(zip(rotations_before, rotations_after))
        if b != a
    ]
    return {
        "status": "ok" if len(after.pages) == len(before.pages) else "page_count_mismatch",
        "operation": "rotate",
        "input": {"file": str(src), "sha256": sha256(src), "pages": len(before.pages),
                  "signatures_present": signed_fields(before)},
        "output": {"file": str(out), "sha256": sha256(out), "pages": len(after.pages),
                   "signatures_present": signed_fields(after)},
        "angle": int(turn),
        "mode": "absolute" if absolute else "relative",
        "page_spec": pages or "all",
        "rotations_changed": changed,
        "notes": ["pdfplumber follows /Rotate but its reading order does not: a rotated page comes "
                  "back a word per line and out of order. Read such a page with extract.py --layout, "
                  "or reset it first with `rotate --absolute --angle 0`."],
    }


def denied_permissions(perms: dict) -> list[str]:
    """The permissions this run takes away; each is enforced by the owner password alone."""
    denied = []
    if perms["print"] != "full":
        denied.append(f"--print={perms['print']}")
    if perms["modify"] != "all":
        denied.append(f"--modify={perms['modify']}")
    if not perms["extract"]:
        denied.append("--extract=no")
    return denied


def encrypt(src: Path, out: Path, user: str, owner: str | None, perms: dict) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    check_distinct(out, [src])
    if not user:
        fail("--user-password is required; an empty user password leaves the file open to anyone")
    denied = denied_permissions(perms)
    if denied and not owner:
        fail(
            f"{', '.join(denied)} takes a permission away, and only the owner password enforces one: "
            "pass --owner-password. Without it the user password becomes the owner password too, so "
            "everyone who can open the file can lift the restriction.",
            permissions_denied=denied,
        )
    if denied and owner == user:
        fail(
            f"--owner-password is the same as --user-password, so {', '.join(denied)} would not hold: "
            "everyone who can open the file would hold the owner password. Use a different one.",
            permissions_denied=denied,
        )
    reused = not owner
    owner = owner or user
    before = summary(src)
    args = [
        str(src),
        "--encrypt",
        f"--user-password={user}",
        f"--owner-password={owner}",
        "--bits=256",
        PRINT[perms["print"]],
        MODIFY[perms["modify"]],
        f"--extract={'y' if perms['extract'] else 'n'}",
        "--",
        str(out),
    ]
    out.parent.mkdir(parents=True, exist_ok=True)
    warning = qpdf(args, "qpdf --encrypt", out)
    after = summary(out, user)
    shown = subprocess.run(
        ["qpdf", "--show-encryption", f"--password={user}", str(out)],
        capture_output=True, text=True, timeout=120,
    ).stdout.strip()
    info = ""
    if shutil.which("pdfinfo"):
        info_out = subprocess.run(
            ["pdfinfo", "-upw", user, str(out)], capture_output=True, text=True, timeout=120
        ).stdout
        info = next((line for line in info_out.splitlines() if line.startswith("Encrypted:")), "")
    reader = pdf_reader(out)
    return {
        "status": "ok" if reader.is_encrypted and after["pages"] == before["pages"] else "verification_failed",
        "operation": "encrypt",
        "input": before,
        "output": after,
        "algorithm": "AES-256",
        "encrypted": reader.is_encrypted,
        "owner_password_reused_user_password": reused,
        "permissions_requested": perms,
        "permissions_denied": denied,
        "pdfinfo": info,
        "qpdf_show_encryption": shown,
        "qpdf_warnings": warning,
        "notes": [
            "The user password opens the file; the owner password is what overrides a denied "
            "permission."
            + (
                " Nothing is denied here, so the owner password was set to the user password."
                if reused
                else " The owner password here is a different secret, so a reader holding only the "
                "user password cannot lift a restriction."
            ),
            "Encrypting two files separately with the same password does not produce one shared "
            "protection: decrypt both, merge, then encrypt the merged file once.",
        ],
    }


def decrypt(src: Path, out: Path, password: str) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    check_distinct(out, [src])
    before = summary(src, password)
    out.parent.mkdir(parents=True, exist_ok=True)
    qpdf([f"--password={password}", "--decrypt", str(src), str(out)], "qpdf --decrypt", out)
    after = summary(out)
    return {
        "status": "ok" if not pdf_reader(out).is_encrypted and after["pages"] == before["pages"] else "verification_failed",
        "operation": "decrypt",
        "input": before,
        "output": after,
        "notes": ["Decrypt into a working copy first; every other script here needs an unencrypted "
                  "file or its password, and qpdf page operations refuse encrypted input."],
    }



USAGE = {
    "merge": "pages.py merge --out <out.pdf> <in1.pdf> <in2.pdf> [...]",
    "split": "pages.py split <in.pdf> --ranges 1-3,4-6 [--out DIR]",
    "rotate": "pages.py rotate <in.pdf> --out <out.pdf> --angle 90 [--pages 1-3] [--absolute]",
    "encrypt": (
        "pages.py encrypt <in.pdf> --out <out.pdf> --user-password PW [--owner-password PW] "
        "[--print full|low|none] [--modify all|annotate|form|assembly|none] [--extract yes|no]"
    ),
    "decrypt": "pages.py decrypt <in.pdf> --out <out.pdf> --password PW",
}
# Per subcommand, the options it accepts and whether each takes a value. The table is
# per subcommand rather than shared so that a flag belonging to another one is an
# unknown option here: `rotate --ranges 1-3` used to parse and then turn every page.
TAKES_VALUE: dict[str, dict[str, bool]] = {
    "merge": {"--out": True},
    "split": {"--out": True, "--ranges": True},
    "rotate": {"--out": True, "--angle": True, "--pages": True, "--absolute": False},
    "encrypt": {
        "--out": True, "--user-password": True, "--owner-password": True,
        "--print": True, "--modify": True, "--extract": True, "--bits": True,
    },
    "decrypt": {"--out": True, "--password": True},
}


def parse_args(argv: list[str], takes_value: dict[str, bool], usage: str) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token `takes_value` does not name.

    A value is still read by position, so `--out x.pdf` works when x.pdf is also an input
    name, but it is never read off another option: `--out --angle 180` used to write a
    file called `--angle`, turn by the default 90 and drop the 180 as a stray positional,
    and report all of that as a success. A repeated option is the same kind of mistake,
    since the last one silently won and the file the first one named was never written.
    """
    args: list[str] = []
    flags: dict[str, str | bool] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if not token.startswith("--"):
            args.append(token)
            i += 1
        elif token not in takes_value:
            fail(f"unknown option: {token}; usage: {usage}")
        elif token in flags:
            fail(f"repeated option: {token}; usage: {usage}")
        elif not takes_value[token]:
            flags[token] = True
            i += 1
        elif i + 1 >= len(argv) or argv[i + 1].startswith("--"):
            fail(f"{token} requires a value; usage: {usage}")
        else:
            flags[token] = argv[i + 1]
            i += 2
    return args, flags


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    commands = tuple(TAKES_VALUE)
    if not argv or argv[0] not in commands:
        fail(f"usage: pages.py {'|'.join(commands)} <...>")
    command, argv = argv[0], argv[1:]
    positional, opts = parse_args(argv, TAKES_VALUE[command], USAGE[command])

    def option(flag: str, default: str | None = None) -> str | None:
        return str(opts[flag]) if flag in opts else default

    out = option("--out")

    if command == "merge":
        if not out:
            fail("merge needs --out <file.pdf>")
        report = merge([Path(p).expanduser().resolve() for p in positional], Path(out).expanduser().resolve())
    elif command == "split":
        if not positional:
            fail("split needs an input file")
        src = Path(positional[0]).expanduser().resolve()
        out_dir = Path(out).expanduser().resolve() if out else src.with_name(src.stem + "_split")
        report = split(src, option("--ranges", ""), out_dir)
    elif command == "rotate":
        if not positional or not out:
            fail("rotate needs an input file and --out <file.pdf>")
        report = rotate(
            Path(positional[0]).expanduser().resolve(),
            Path(out).expanduser().resolve(),
            parse_angle(option("--angle", "90")),
            option("--pages"),
            "--absolute" in opts,
        )
    elif command == "encrypt":
        if not positional or not out:
            fail("encrypt needs an input file and --out <file.pdf>")
        printing = option("--print", "full")
        modify = option("--modify", "all")
        extract = option("--extract", "yes")
        if printing not in PRINT or modify not in MODIFY or extract not in EXTRACT:
            fail(
                f"--print must be one of {sorted(PRINT)}; --modify one of {sorted(MODIFY)}; "
                f"--extract one of {sorted(EXTRACT)}"
            )
        if option("--bits", "256") != "256":
            fail(f"--bits {option('--bits')} is not offered; this encrypts with AES-256 only")
        report = encrypt(
            Path(positional[0]).expanduser().resolve(),
            Path(out).expanduser().resolve(),
            option("--user-password", ""),
            option("--owner-password"),
            {"print": printing, "modify": modify, "extract": EXTRACT[extract]},
        )
    else:
        if not positional or not out:
            fail("decrypt needs an input file and --out <file.pdf>")
        report = decrypt(
            Path(positional[0]).expanduser().resolve(),
            Path(out).expanduser().resolve(),
            option("--password", ""),
        )

    print(json.dumps(report, indent=2))
    if report["status"] != "ok":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
