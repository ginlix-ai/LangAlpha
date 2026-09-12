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
when a single output needs a comma of its own: `1,3;2,4` makes two files.

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

PRINT = {"full": "--print=full", "low": "--print=low", "none": "--print=none"}
MODIFY = {
    "all": "--modify=all",
    "annotate": "--modify=annotate",
    "form": "--modify=form",
    "assembly": "--modify=assembly",
    "none": "--modify=none",
}


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


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
    Exit 2 is a warning qpdf recovered from, but only when it still wrote the file.
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
        if proc.returncode > 2 or (proc.returncode and not wrote):
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
    reader = PdfReader(str(path))
    if reader.is_encrypted and not reader.decrypt(password or ""):
        fail(f"{path} is encrypted; supply the password", password_required=True)
    return reader


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


def split(src: Path, ranges: str, out_dir: Path) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    parts = [p.strip() for p in (ranges.split(";") if ";" in ranges else ranges.split(",")) if p.strip()]
    if not parts:
        fail("split needs --ranges, for example 1-3,4-6")
    before = summary(src)
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for part in parts:
        stem = re.sub(r"[^0-9A-Za-z-]+", "_", part)
        target = out_dir / f"{src.stem}_p{stem}.pdf"
        check_distinct(target, [src])
        qpdf([str(src), "--pages", str(src), part, "--", str(target)], f"qpdf --pages {part}", target)
        info = summary(target)
        info["range"] = part
        outputs.append(info)
    total = sum(o["pages"] for o in outputs)
    return {
        "status": "ok",
        "operation": "split",
        "input": before,
        "ranges": parts,
        "outputs": outputs,
        "pages_in": before["pages"],
        "pages_out_total": total,
        "covers_every_page": total == before["pages"],
        "notes": [
            "pages_out_total equals pages_in only when the ranges tile the document exactly; "
            "overlapping or partial ranges are legitimate, so check this against what was asked."
        ],
    }


def rotate(src: Path, out: Path, angle: int, pages: str | None, absolute: bool) -> dict:
    if not src.exists():
        fail(f"file not found: {src}")
    if angle % 90 != 0:
        fail("--angle must be a multiple of 90")
    check_distinct(out, [src])
    before = read_pdf(src)
    rotations_before = [int(p.rotation or 0) for p in before.pages]
    spec = f"{'' if absolute else '+'}{angle}" + (f":{pages}" if pages else "")
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
        "input": {"file": str(src), "sha256": sha256(src), "pages": len(before.pages)},
        "output": {"file": str(out), "sha256": sha256(out), "pages": len(after.pages)},
        "angle": angle,
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
    reader = PdfReader(str(out))
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
        "status": "ok" if not PdfReader(str(out)).is_encrypted and after["pages"] == before["pages"] else "verification_failed",
        "operation": "decrypt",
        "input": before,
        "output": after,
        "notes": ["Decrypt into a working copy first; every other script here needs an unencrypted "
                  "file or its password, and qpdf page operations refuse encrypted input."],
    }



def parse_args(argv: list[str], with_value: tuple[str, ...], switches: tuple[str, ...]) -> tuple[dict, list[str]]:
    """Flag values are taken by position, so `--out x.pdf` still works when x.pdf is also an input name."""
    opts: dict[str, object] = {}
    positional: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        if token in with_value:
            if i + 1 >= len(argv):
                fail(f"{token} needs a value")
            opts[token] = argv[i + 1]
            i += 2
        elif token in switches:
            opts[token] = True
            i += 1
        elif token.startswith("--"):
            fail(f"unknown option {token}")
        else:
            positional.append(token)
            i += 1
    return opts, positional


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    commands = ("merge", "split", "rotate", "encrypt", "decrypt")
    if not argv or argv[0] not in commands:
        fail(f"usage: pages.py {'|'.join(commands)} <...>")
    command, argv = argv[0], argv[1:]
    opts, positional = parse_args(
        argv,
        ("--out", "--ranges", "--angle", "--pages", "--user-password", "--owner-password",
         "--password", "--bits", "--print", "--modify", "--extract"),
        ("--absolute",),
    )

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
            int(option("--angle", "90")),
            option("--pages"),
            "--absolute" in opts,
        )
    elif command == "encrypt":
        if not positional or not out:
            fail("encrypt needs an input file and --out <file.pdf>")
        printing = option("--print", "full")
        modify = option("--modify", "all")
        if printing not in PRINT or modify not in MODIFY:
            fail(f"--print must be one of {sorted(PRINT)}; --modify one of {sorted(MODIFY)}")
        if option("--bits", "256") != "256":
            fail(f"--bits {option('--bits')} is not offered; this encrypts with AES-256 only")
        report = encrypt(
            Path(positional[0]).expanduser().resolve(),
            Path(out).expanduser().resolve(),
            option("--user-password", ""),
            option("--owner-password"),
            {"print": printing, "modify": modify, "extract": option("--extract", "yes") == "yes"},
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
