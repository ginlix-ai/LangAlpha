#!/usr/bin/env python3
"""Inspect, fill and flatten AcroForm PDFs, verifying the result each time.

Usage:
    python forms.py inspect <file.pdf> [--password PW]
    python forms.py fill    <in.pdf> --values values.json --out <out.pdf> [--password PW]
                            [--need-appearances] [--truncate] [--complete]
    python forms.py flatten <in.pdf> --out <out.pdf> [--password PW] [--engine qpdf|pypdf]

`inspect` lists every field with its type, current value, allowed options, page,
rectangle and the required and read-only flags. It distinguishes three states
that look identical from the outside: no AcroForm at all, an AcroForm carrying
zero fields, and a real fillable form. The first two mean the blanks on the page
are painted, not fillable, so filling them is a drawing job, not a form job.

`fill` writes values through pypdf with appearance regeneration, then reopens the
output and reads every field back. A write that a viewer would show as empty is
reported as a verification failure, never as success. Checkbox and radio values
are matched against the on-state names the file actually declares, so `true` and
`Yes` resolve to `/Yes` when that is what the widget calls its on state, and a
state the file does not declare is rejected rather than written. A multi-select list
box takes a JSON array and matches every entry against the file's own options; a field
that is not multi-select rejects an array.

A text value longer than the field's /MaxLen is rejected the same way, because a
viewer accepts the write and then draws only what fits: pass --truncate to cut it
here instead, which lists the field under `truncated`. Every required field left
empty after the fill is listed under `required_empty`, whether or not the values
file mentioned it, and --complete turns a non-empty list into an error. Values
stored and appearance verified are two separate results: `appearance_verified`
goes false when a stored value is missing from the text layer, which means the
value is in the file but not on the page.

That check needs `pdftotext`, so `text_layer_check` says whether it ran:
`checked`, `not_applicable` when no text or choice value was written, or
`skipped` with a reason when pdftotext is absent or failed. A skipped check also
makes `appearance_verified` false, because a check that did not run proves
nothing; `status` stays `ok`, since the values did land in the file.

A password field's value is replaced with `<redacted>` everywhere these reports
print it. The real value is still written and still read back and compared.

NeedAppearances is left off by default. It tells a viewer to throw away every
stored appearance and redraw the widget itself, and poppler's redraw loses the
tick on a checkbox and the dot on a radio button: the values are still in the
file and the page looks blank. The appearance streams written here are the ones
the render and the text layer are then checked against. Pass --need-appearances
when a particular viewer needs to recompute, and render the result to see what
that costs.

`flatten` bakes the values into the page content and removes the form. After it
runs there is no /AcroForm and no widget annotation left, and `pdftotext` still
finds every text and choice value: both are checked here, and `text_layer_check`
again says whether the second one could run. Without qpdf the pypdf engine takes
over; `engine` names the one that ran and a warning names what it can lose.

An encrypted input comes back encrypted: same permission flags, same algorithm strength,
confirmed by reopening the output, and `output_encrypted` says so. Only the password that
opened the file is knowable, so the other one of the pair can change. Opened with the user
password, a random owner password replaces the original, nobody holds the override and
`owner_password` reads `replaced`. Opened with the owner password, that password becomes
the output's user password too and `owner_password` reads `reused`. A file that opens with
no password keeps opening with none, and qpdf-engine flatten copies the encryption
dictionary whole, so it changes neither password. Taking protection off is
`pages.py decrypt`, when the user asks for it, not a side effect of filling a form.

Inputs are never written to. The output path must differ from the input.
Exit code is 1 for a hard error (bad file, unknown field, a value the field cannot
hold, verification failure, or an incomplete form under --complete).
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from pypdf import PasswordType, PdfReader, PdfWriter
from pypdf.generic import ArrayObject, DictionaryObject, NameObject

FF_READONLY = 1 << 0
FF_REQUIRED = 1 << 1
FF_NOEXPORT = 1 << 2
FF_MULTILINE = 1 << 12
FF_PASSWORD = 1 << 13
FF_RADIO = 1 << 15
FF_PUSHBUTTON = 1 << 16
FF_COMBO = 1 << 17
FF_MULTISELECT = 1 << 21

TRUTHY = {"true", "yes", "on", "1", "x", "checked"}
FALSY = {"false", "no", "off", "0", "", "unchecked"}
# What a password field's value is replaced with everywhere this script prints JSON. The
# real value still goes into the PDF and is still read back and checked; it is the report
# that must not carry it, because the report is logged, pasted and handed on.
REDACTED = "<redacted>"
# The encryption dictionary's /R, and the pypdf algorithm that writes the same strength back.
ENCRYPTION_BY_REVISION = {2: "RC4-40", 3: "RC4-128", 4: "AES-128", 5: "AES-256", 6: "AES-256"}
# Both of these are the same limit: a file hands back the permission flags and the revision, but
# only the one password that opened it. Whichever of the pair is unknown has to be replaced.
OWNER_REPLACED = (
    "the output keeps the input's user password, permissions and algorithm, but not its owner "
    "password: that one cannot be read out of a file opened with the user password. A random one "
    "replaced it and is recorded nowhere, so the restrictions stand and nobody holds the override. "
    "pages.py encrypt sets a fresh pair when the user asks for one."
)
USER_REPLACED = (
    "this input was opened with its owner password, so the output takes that password as its user "
    "password too: the input's own user password cannot be read out of it. Anyone who opened the "
    "input with the user password will not open this output with it, so hand on the password you "
    "were given, or set a fresh pair with pages.py encrypt."
)
# The conventional short tags an appearance stream uses for the standard 14 fonts.
# A checkbox draws its tick as ZapfDingbats text, so an undefined /ZaDb renders an empty box.
STANDARD_TAGS = {
    "Helv": "Helvetica", "HeBo": "Helvetica-Bold", "HeOb": "Helvetica-Oblique",
    "TiRo": "Times-Roman", "TiBo": "Times-Bold", "TiIt": "Times-Italic",
    "Cour": "Courier", "CoBo": "Courier-Bold", "Symb": "Symbol", "ZaDb": "ZapfDingbats",
}
FONT_TAG = re.compile(rb"/([A-Za-z][A-Za-z0-9]*)\s+[\d.]+\s+Tf")


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def as_bool(value) -> bool:
    """pypdf's BooleanObject has no __bool__, so bool(BooleanObject(False)) is True."""
    return bool(getattr(value, "value", value))


def blank(value) -> bool:
    """An unticked checkbox reads back as `/Off`, which is as empty as an empty string."""
    if isinstance(value, list):
        return not any(str(v).strip() for v in value)
    return value is None or str(value).strip() in ("", "/Off")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def open_reader(path: Path, password: str | None) -> PdfReader:
    if not path.exists():
        fail(f"file not found: {path}")
    reader = PdfReader(str(path))
    if reader.is_encrypted:
        try:
            ok = reader.decrypt(password or "")
        except Exception as exc:
            fail(f"cannot decrypt {path}: {exc}", password_required=True)
        if not ok:
            fail(f"{path} is encrypted; pass --password", password_required=True)
    return reader


def encryption_profile(reader: PdfReader, password: str | None) -> dict | None:
    """What it takes to write the output back under the input's own protection.

    None of it survives the write: a pypdf writer starts unencrypted, so the revision, the
    permission bits and which of the two passwords opened the file have to be read off the
    input first. Decrypting a second time is how pypdf reports which password matched.
    """
    if not reader.is_encrypted:
        return None
    revision = int(reader.trailer["/Encrypt"].get_object().get("/R", 2))
    # A rejected attempt costs nothing and leaves the reader decrypted; the supplied password
    # goes last so the reader ends on the key it came in with.
    opens_empty = reader.decrypt("") != PasswordType.NOT_DECRYPTED
    return {
        "algorithm": ENCRYPTION_BY_REVISION.get(revision, "AES-256"),
        "permissions": reader.user_access_permissions,
        "opens_empty": opens_empty,
        "owner_opened": reader.decrypt(password or "") == PasswordType.OWNER_PASSWORD,
    }


def apply_protection(writer: PdfWriter, profile: dict | None, password: str | None) -> str | None:
    """Re-encrypt the output as the input was. Returns how the owner password was set, or None.

    The owner password cannot be read out of a file opened with the user password, so a random
    one takes its place: the restrictions stay enforced and the override goes to nobody, which
    is the safe direction. Reusing the supplied password there would hand every holder of it
    the right to lift the restrictions it is meant to enforce. A file that opens with no
    password keeps opening with none.
    """
    if profile is None:
        return None
    user = "" if profile["opens_empty"] else (password or "")
    reused = profile["owner_opened"]
    owner = (password or "") if reused else secrets.token_urlsafe(24)
    try:
        writer.encrypt(user, owner, permissions_flag=profile["permissions"], algorithm=profile["algorithm"])
    except Exception as exc:
        fail(f"cannot write the output under the input's protection ({profile['algorithm']}): {exc}")
    return "reused" if reused else "replaced"


def describe_protection(profile: dict | None) -> str:
    if profile is None:
        return "no encryption"
    return f"{profile['algorithm']} with permission flags {int(profile['permissions'] or 0)}"


def field_kind(ft: str, flags: int) -> str:
    if ft == "/Btn":
        if flags & FF_PUSHBUTTON:
            return "pushbutton"
        return "radio" if flags & FF_RADIO else "checkbox"
    if ft == "/Ch":
        return "dropdown" if flags & FF_COMBO else "listbox"
    if ft == "/Tx":
        return "text"
    if ft == "/Sig":
        return "signature"
    return "unknown"


def inherited(node, key: str):
    seen = 0
    while node is not None and seen < 32:
        if key in node:
            return node[key]
        parent = node.get("/Parent")
        node = parent.get_object() if parent is not None else None
        seen += 1
    return None


def to_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def to_value(value):
    """A multi-select list box stores /V as an array; every other field stores one string."""
    if isinstance(value, ArrayObject):
        return [to_text(v.get_object()) for v in value]
    return to_text(value)


def widget_pages(reader: PdfReader) -> dict[int, int]:
    """Annotation object number to 1-based page number, so a field can name its page."""
    index: dict[int, int] = {}
    for pno, page in enumerate(reader.pages, start=1):
        for ref in page.get("/Annots", []) or []:
            idnum = getattr(ref, "idnum", None)
            if idnum is not None:
                index[idnum] = pno
    return index


def on_states(widget) -> list[str]:
    ap = widget.get("/AP")
    if ap is None:
        return []
    normal = ap.get_object().get("/N")
    if normal is None:
        return []
    try:
        return [str(k) for k in normal.get_object().keys() if str(k) != "/Off"]
    except Exception:
        return []


def choice_options(node) -> list[dict]:
    opt = inherited(node, "/Opt")
    if opt is None:
        return []
    out = []
    for entry in opt.get_object():
        entry = entry.get_object()
        if isinstance(entry, (list, tuple)) or hasattr(entry, "__getitem__") and not isinstance(entry, str):
            try:
                pair = list(entry)
                if len(pair) >= 2:
                    out.append({"value": to_text(pair[0]), "label": to_text(pair[1])})
                    continue
                if len(pair) == 1:
                    out.append({"value": to_text(pair[0]), "label": to_text(pair[0])})
                    continue
            except TypeError:
                pass
        out.append({"value": to_text(entry), "label": to_text(entry)})
    return out


def collect_fields(reader: PdfReader) -> tuple[list[dict], dict]:
    """Walk /AcroForm /Fields, returning one entry per terminal field."""
    root = reader.trailer.get("/Root", {})
    acroform = root.get("/AcroForm")
    state = {"has_acroform": acroform is not None, "xfa": False, "need_appearances": False}
    if acroform is None:
        return [], state
    acroform = acroform.get_object()
    state["xfa"] = "/XFA" in acroform
    state["need_appearances"] = as_bool(acroform.get("/NeedAppearances", False))
    pages = widget_pages(reader)
    fields: list[dict] = []

    def walk(ref, prefix: str, depth: int) -> None:
        if depth > 24:
            return
        node = ref.get_object()
        name = to_text(node.get("/T")) or ""
        qualified = f"{prefix}.{name}" if prefix and name else (name or prefix)
        kids = node.get("/Kids")
        kid_objects = [k for k in kids.get_object()] if kids is not None else []
        # A kid with its own /T is a field in its own right; a kid without one is
        # just this field's widget on some page.
        child_fields = [k for k in kid_objects if "/T" in k.get_object()]
        if child_fields:
            for kid in child_fields:
                walk(kid, qualified, depth + 1)
            return
        widgets = kid_objects or [ref]
        ft = str(inherited(node, "/FT") or "")
        flags = int(inherited(node, "/Ff") or 0)
        kind = field_kind(ft, flags)
        value = inherited(node, "/V")
        default = inherited(node, "/DV")
        states: list[str] = []
        for w in widgets:
            for s in on_states(w.get_object()):
                if s not in states:
                    states.append(s)
        entry: dict = {
            "name": qualified,
            "leaf_name": name,
            "type": kind,
            "pdf_type": ft or None,
            "value": to_value(value),
            "default": to_value(default),
            "required": bool(flags & FF_REQUIRED),
            "readonly": bool(flags & FF_READONLY),
            "flags": flags,
            "widgets": len(widgets),
        }
        if kind == "text":
            entry["multiline"] = bool(flags & FF_MULTILINE)
            entry["password"] = bool(flags & FF_PASSWORD)
            maxlen = inherited(node, "/MaxLen")
            if maxlen is not None:
                entry["max_length"] = int(maxlen)
        if kind in ("checkbox", "radio"):
            entry["options"] = [s.lstrip("/") for s in states]
            entry["on_states"] = states
        if kind in ("dropdown", "listbox"):
            options = choice_options(node)
            entry["options"] = [o["value"] for o in options]
            entry["option_labels"] = options
            entry["multiselect"] = bool(flags & FF_MULTISELECT)
        placements = []
        for w in widgets:
            idnum = getattr(w, "idnum", None)
            obj = w.get_object()
            rect = obj.get("/Rect")
            placements.append(
                {
                    "page": pages.get(idnum),
                    "rect": [round(float(v), 2) for v in rect] if rect is not None else None,
                    "on_state": str(obj.get("/AS")) if obj.get("/AS") is not None else None,
                }
            )
        entry["page"] = placements[0]["page"] if placements else None
        entry["rect"] = placements[0]["rect"] if placements else None
        if len(placements) > 1:
            entry["placements"] = placements
        fields.append(entry)

    for ref in acroform.get("/Fields", []) or []:
        walk(ref, "", 0)
    return fields, state


def secret_names(fields: list[dict]) -> set[str]:
    return {f["name"] for f in fields if f.get("password")}


def form_report(path: Path, password: str | None) -> dict:
    reader = open_reader(path, password)
    fields, state = collect_fields(reader)
    redacted = sorted(secret_names(fields))
    for f in fields:
        if f.get("password"):
            for key in ("value", "default"):
                if f[key] is not None:
                    f[key] = REDACTED
    notes: list[str] = []
    if not state["has_acroform"]:
        form_state = "no_acroform"
        notes.append(
            "No AcroForm. Blanks visible on the page were drawn or flattened, so there is nothing "
            "to fill: either ask the user for the fillable original, or draw the values with "
            "reportlab and stamp them over the page."
        )
    elif not fields:
        form_state = "acroform_without_fields"
        notes.append("AcroForm present but it declares zero fields; treat it as a flat page.")
    else:
        form_state = "acroform_with_fields"
    if state["xfa"]:
        notes.append(
            "XFA form. The AcroForm layer may be a shell that an XFA-aware viewer ignores; values "
            "written here can be invisible there. Ask the user for an AcroForm version."
        )
    unnamed = [f for f in fields if not f["leaf_name"]]
    if unnamed:
        notes.append(f"{len(unnamed)} field(s) have no /T name and can only be addressed by their parent group.")
    if redacted:
        notes.append(
            f"{redacted} are password fields; their stored values are shown as {REDACTED!r} here and "
            "in every other report from this script. Read them from the PDF itself if you need them."
        )
    return {
        "status": "ok",
        "operation": "inspect",
        "file": str(path),
        "sha256": sha256(path),
        "form_state": form_state,
        "has_acroform": state["has_acroform"],
        "xfa": state["xfa"],
        "need_appearances": state["need_appearances"],
        "field_count": len(fields),
        "pages": len(reader.pages),
        "fields": fields,
        "redacted_fields": redacted,
        "notes": notes,
    }


def standard_font(base: str) -> DictionaryObject:
    font = DictionaryObject()
    font[NameObject("/Type")] = NameObject("/Font")
    font[NameObject("/Subtype")] = NameObject("/Type1")
    font[NameObject("/BaseFont")] = NameObject(f"/{base}")
    if base not in ("Symbol", "ZapfDingbats"):
        font[NameObject("/Encoding")] = NameObject("/WinAnsiEncoding")
    return font


def repair_appearance_fonts(writer: PdfWriter) -> tuple[list[str], list[str]]:
    """Define the fonts the form names but never declares.

    With NeedAppearances set, a viewer throws away the stored appearance and redraws
    each widget from its /DA string and /MK caption, resolving font tags through the
    AcroForm /DR. reportlab writes a checkbox whose tick is ZapfDingbats and leaves
    /ZaDb undefined, so the redraw fails silently: the field reads back as checked and
    the box renders empty. Poppler says `Unknown font tag`, most viewers say nothing.
    Filling in the standard-14 definitions fixes the render without touching a value.
    """
    repaired: list[str] = []
    unknown: list[str] = []
    tags: set[str] = {"Helv", "ZaDb"}  # /DA default and the checkbox caption font

    def scan(obj) -> None:
        da = obj.get("/DA")
        if da is not None:
            found = FONT_TAG.findall(str(da.get_object()).encode("latin-1", "replace"))
            tags.update(m.decode("latin-1") for m in found)

    root = writer.root_object
    acroform = root.get("/AcroForm")
    if acroform is None:
        return [], []
    acroform = acroform.get_object()
    scan(acroform)

    for page in writer.pages:
        for ref in page.get("/Annots", []) or []:
            annot = ref.get_object()
            scan(annot)
            parent = annot.get("/Parent")
            if parent is not None:
                scan(parent.get_object())
            appearance = annot.get("/AP")
            if appearance is None:
                continue
            streams = []
            for value in appearance.get_object().values():
                value = value.get_object()
                if hasattr(value, "get_data"):
                    streams.append(value)
                elif hasattr(value, "values"):
                    streams += [v.get_object() for v in value.values() if hasattr(v.get_object(), "get_data")]
            for stream in streams:
                try:
                    data = stream.get_data()
                except Exception:
                    continue
                stream_tags = {m.decode("latin-1") for m in FONT_TAG.findall(data)}
                if not stream_tags:
                    continue
                tags.update(stream_tags)
                resources = stream.get("/Resources")
                if resources is None:
                    resources = DictionaryObject()
                    stream[NameObject("/Resources")] = resources
                resources = resources.get_object()
                fonts = resources.get("/Font")
                if fonts is None:
                    fonts = DictionaryObject()
                    resources[NameObject("/Font")] = fonts
                fonts = fonts.get_object()
                for tag in sorted(stream_tags):
                    if NameObject(f"/{tag}") in fonts or tag not in STANDARD_TAGS:
                        continue
                    fonts[NameObject(f"/{tag}")] = standard_font(STANDARD_TAGS[tag])
                    repaired.append(f"appearance:/{tag}")

    resources = acroform.get("/DR")
    if resources is None:
        resources = DictionaryObject()
        acroform[NameObject("/DR")] = resources
    resources = resources.get_object()
    fonts = resources.get("/Font")
    if fonts is None:
        fonts = DictionaryObject()
        resources[NameObject("/Font")] = fonts
    fonts = fonts.get_object()
    for tag in sorted(tags):
        if NameObject(f"/{tag}") in fonts:
            continue
        if tag in STANDARD_TAGS:
            fonts[NameObject(f"/{tag}")] = standard_font(STANDARD_TAGS[tag])
            repaired.append(f"DR:/{tag}")
        else:
            unknown.append(tag)
    return sorted(set(repaired)), sorted(set(unknown))


def match_option(options: list[dict], raw) -> str | None:
    """Match one value against a choice field's export values first, then its display labels."""
    text = "" if raw is None else str(raw)
    for opt in options:
        if text == opt["value"]:
            return opt["value"]
    for opt in options:
        if text == opt["label"]:
            return opt["value"]
    return None


def normalize(entry: dict, raw) -> tuple[str | list[str] | None, str | None]:
    """Return (value to write, error). Buttons resolve to the on-state name the file declares."""
    kind = entry["type"]
    if kind in ("pushbutton", "signature"):
        return None, f"{entry['name']}: a {kind} field cannot be filled"
    if entry["readonly"]:
        return None, f"{entry['name']}: field is read-only in the PDF"
    if kind == "text":
        if isinstance(raw, (list, tuple)):
            return None, f"{entry['name']}: takes one value, not a list"
        return ("" if raw is None else str(raw)), None
    if kind in ("checkbox", "radio"):
        states = entry.get("on_states") or []
        if isinstance(raw, bool):
            if not raw:
                return "/Off", None
            if not states:
                return None, f"{entry['name']}: no on-state in the file; cannot check it"
            return states[0], None
        text = str(raw).strip()
        lowered = text.lower().lstrip("/")
        for s in states:
            if text == s or text == s.lstrip("/") or lowered == s.lower().lstrip("/"):
                return s, None
        if lowered in FALSY or lowered == "off":
            return "/Off", None
        if lowered in TRUTHY and states:
            return states[0], None
        return None, f"{entry['name']}: {text!r} is not an on-state; the file declares {states + ['/Off']}"
    if kind in ("dropdown", "listbox"):
        options = entry.get("option_labels") or []
        allowed = [o["value"] for o in options]
        if isinstance(raw, (list, tuple)):
            if not entry.get("multiselect"):
                return None, f"{entry['name']}: takes one selection, not a list; it is not multi-select"
            picked: list[str] = []
            for item in raw:
                value = str(item) if not allowed else match_option(options, item)
                if value is None:
                    return None, f"{entry['name']}: {str(item)!r} is not an option; allowed export values are {allowed}"
                if value not in picked:
                    picked.append(value)
            return picked, None
        text = "" if raw is None else str(raw)
        if not allowed:
            return text, None
        value = match_option(options, text)
        if value is None:
            return None, f"{entry['name']}: {text!r} is not an option; allowed export values are {allowed}"
        return value, None
    return ("" if raw is None else str(raw)), None


def written_strings(value) -> list[str]:
    """The strings one written value should put in the text layer, one per selection."""
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    return [s for s in (str(v) for v in items) if s.strip()]


def same_value(got, wanted) -> bool:
    """Read-back comparison: a button state comes back with its slash, a multi-select as a list."""

    def parts(value) -> list[str]:
        items = value if isinstance(value, list) else [value]
        return [str(v).lstrip("/") for v in items]

    return parts(got) == parts(wanted)


def read_text_layer(path: Path, password: str | None) -> tuple[str, str | None, str]:
    """Return (state, reason, text), where state is "checked" or "skipped".

    An absent or failing pdftotext leaves no text to compare against. Treating that as an
    empty set of missing values would report a verification that never ran as a passed one,
    so the caller is told the check was skipped and why.
    """
    if shutil.which("pdftotext") is None:
        return "skipped", "pdftotext is not on PATH", ""
    proc = subprocess.run(
        ["pdftotext"] + (["-upw", password, "-opw", password] if password else []) + [str(path), "-"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        return "skipped", f"pdftotext exited {proc.returncode}: {proc.stderr.strip()[:200]}", ""
    return "checked", None, proc.stdout


def fill(
    src: Path,
    values_path: Path,
    out: Path,
    password: str | None,
    need_appearances: bool,
    truncate: bool,
    complete: bool,
) -> dict:
    if out.resolve() == src.resolve():
        fail("output path must differ from the input; inputs are never modified in place")
    try:
        payload = json.loads(values_path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"cannot read {values_path}: {exc}")
    if isinstance(payload, dict) and "fields" in payload and isinstance(payload["fields"], dict):
        payload = payload["fields"]
    if not isinstance(payload, dict):
        fail("values file must be a JSON object mapping field name to value")

    reader = open_reader(src, password)
    profile = encryption_profile(reader, password)
    fields, state = collect_fields(reader)
    if not state["has_acroform"]:
        fail("no AcroForm in this file; there are no fields to fill", form_state="no_acroform")
    if not fields:
        fail("AcroForm declares zero fields; nothing to fill", form_state="acroform_without_fields")
    by_name: dict[str, dict] = {}
    for f in fields:
        by_name[f["name"]] = f
        by_name.setdefault(f["leaf_name"], f)

    unknown = [k for k in payload if k not in by_name]
    if unknown:
        fail(
            f"unknown field name(s): {unknown}",
            known_fields=sorted({f["name"] for f in fields}),
        )
    secret = secret_names(fields)

    def shown(name: str, value):
        return REDACTED if name in secret and value is not None else value

    resolved: dict[str, str | list[str]] = {}
    errors: list[str] = []
    plan: list[dict] = []
    truncated: list[str] = []
    for key, raw in payload.items():
        entry = by_name[key]
        value, error = normalize(entry, raw)
        if error:
            errors.append(error)
            continue
        limit = entry.get("max_length")
        if limit is not None and len(value) > limit:
            if not truncate:
                errors.append(
                    f"{entry['name']}: the value is {len(value)} characters and the field's max_length "
                    f"is {limit}; shorten it or pass --truncate to cut it to {limit}"
                )
                continue
            value = value[:limit]
            truncated.append(entry["name"])
        resolved[entry["name"]] = value
        plan.append({
            "field": entry["name"],
            "type": entry["type"],
            "requested": shown(entry["name"], raw),
            "writing": shown(entry["name"], value),
        })
    if errors:
        fail("; ".join(errors), rejected=errors)

    writer = PdfWriter(clone_from=reader)
    writer.update_page_form_field_values(None, resolved, auto_regenerate=True)
    writer.set_need_appearances_writer(need_appearances)
    repaired_fonts, unknown_fonts = repair_appearance_fonts(writer)
    owner_password = apply_protection(writer, profile, password)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("wb") as fh:
        writer.write(fh)

    # Independent read of the result: pypdf's own report on the writer is not evidence.
    check_reader = open_reader(out, password)
    after_profile = encryption_profile(check_reader, password)
    protection_ok = profile is None or (
        after_profile is not None
        and after_profile["permissions"] == profile["permissions"]
        and after_profile["algorithm"] == profile["algorithm"]
    )
    after, after_state = collect_fields(check_reader)
    after_by_name = {f["name"]: f for f in after}
    verification = []
    ok = True
    for field_name, wanted in resolved.items():
        entry = after_by_name.get(field_name)
        got = entry["value"] if entry else None
        matched = got is not None and same_value(got, wanted)
        if not matched:
            ok = False
        verification.append({
            "field": field_name,
            "expected": shown(field_name, wanted),
            "read_back": shown(field_name, got),
            "ok": matched,
        })
    if not protection_ok:
        ok = False
    required_empty = sorted(
        f["name"] for f in after
        if f["required"] and f["type"] not in ("pushbutton", "signature") and blank(f["value"])
    )

    warnings: list[str] = []
    text_fields = [
        f["name"]
        for f in fields
        if f["name"] in resolved and f["type"] in ("text", "dropdown", "listbox") and written_strings(resolved[f["name"]])
    ]
    missing_in_text: list[str] = []
    text_layer_check, check_reason = "not_applicable", None
    if text_fields:
        text_layer_check, check_reason, layer = read_text_layer(out, password)
    if text_layer_check == "checked":
        labels = {}
        for f in fields:
            if f["name"] in resolved:
                written = written_strings(resolved[f["name"]])
                for opt in f.get("option_labels") or []:
                    if opt["value"] in written:
                        labels[opt["value"]] = opt["label"]
        missing_in_text = [
            n for n in text_fields
            if any(v not in layer and labels.get(v, v) not in layer for v in written_strings(resolved[n]))
        ]
        listed = [shown(n, resolved[n]) for n in missing_in_text]
        if missing_in_text and after_state["need_appearances"]:
            warnings.append(
                f"values {listed} are stored in the field but do not appear in the text layer; "
                "the viewer will draw them from NeedAppearances. Render the page and look before delivering."
            )
        elif missing_in_text:
            warnings.append(
                f"values {listed} are stored in the field but the stored appearance does not "
                "carry the text: too long for the box, or a font the appearance cannot draw. Nothing will "
                "redraw it, so render the page and look at what the field actually shows."
            )
    elif text_layer_check == "skipped":
        warnings.append(
            f"the text layer was not read ({check_reason}), so nothing here has seen the written values "
            "on the page. appearance_verified is false because the check did not run, not because a "
            "value is missing: render the page and look at every field."
        )
    if owner_password == "replaced":
        warnings.append(OWNER_REPLACED)
    elif owner_password == "reused" and not profile["opens_empty"]:
        warnings.append(USER_REPLACED)
    if not protection_ok:
        warnings.append(
            f"the output reopens as {describe_protection(after_profile)} and the input is "
            f"{describe_protection(profile)}; the protection did not survive the write intact."
        )
    if after_state["xfa"]:
        warnings.append("XFA form: an XFA-aware viewer may ignore these AcroForm values.")
    for f in fields:
        if f["name"] not in resolved or f["type"] not in ("dropdown", "listbox"):
            continue
        for opt in f.get("option_labels") or []:
            if opt["value"] in written_strings(resolved[f["name"]]) and opt["label"] != opt["value"]:
                warnings.append(
                    f"{f['name']}: the stored appearance draws the export value {opt['value']!r}; the "
                    f"display label is {opt['label']!r}. Acrobat shows the label, a renderer reading the "
                    "stored appearance shows the export value."
                )
    if need_appearances:
        warnings.append(
            "NeedAppearances is on: viewers redraw every widget themselves and some drop the tick "
            "on a checkbox and the dot on a radio button. Render the page and confirm."
        )
    if unknown_fonts:
        warnings.append(
            f"appearance streams name undefined font tag(s) {unknown_fonts}; those glyphs may not "
            "render. Check the rendered page."
        )
    if truncated:
        warnings.append(
            f"values for {truncated} were cut to the field's max_length; the delivered form says less "
            "than the values file did. Read the written values back before delivering."
        )
    if required_empty and not complete:
        warnings.append(
            f"required field(s) {required_empty} are still empty, so the form is not complete. "
            "Pass --complete to make that an error instead of a warning."
        )

    status = "ok" if ok else "verification_failed"
    message = None
    if complete and required_empty:
        status = "error"
        message = f"required field(s) left empty: {required_empty}"
    report = {
        "status": status,
        **({"message": message} if message else {}),
        "operation": "fill",
        "input": str(src),
        "input_sha256": sha256(src),
        "output": str(out),
        "output_sha256": sha256(out),
        "input_encrypted": profile is not None,
        "output_encrypted": check_reader.is_encrypted,
        **({"owner_password": owner_password} if owner_password else {}),
        "fields_written": len(resolved),
        "fields_total": len(fields),
        "need_appearances": after_state["need_appearances"],
        "appearance_fonts_repaired": repaired_fonts,
        "plan": plan,
        "verification": verification,
        "text_layer_check": text_layer_check,
        **({"text_layer_check_reason": check_reason} if check_reason else {}),
        "text_layer_missing": [shown(n, resolved[n]) for n in missing_in_text],
        "appearance_verified": text_layer_check != "skipped" and not missing_in_text,
        "truncated": truncated,
        "required_empty": required_empty,
        "untouched_fields": sorted({f["name"] for f in fields} - set(resolved)),
        "warnings": warnings,
        "notes": ["Render the filled page and look at it: a value can be stored and still be clipped, "
                  "wrong-sized, or drawn outside its box."],
    }
    print(json.dumps(report, indent=2))
    sys.exit(0 if status == "ok" else 1)


def flatten_qpdf(src: Path, out: Path, password: str | None) -> list[str]:
    if shutil.which("qpdf") is None:
        return []
    notes = []
    with tempfile.TemporaryDirectory(prefix="flatten_") as tmp:
        staged = Path(tmp) / "appearances.pdf"
        base = ["qpdf"] + ([f"--password={password}"] if password else [])
        first = subprocess.run(
            base + ["--generate-appearances", str(src), str(staged)],
            capture_output=True, text=True, timeout=300,
        )
        if first.returncode > 2 or not staged.exists():
            fail(f"qpdf --generate-appearances failed: {first.stderr.strip()[:400]}")
        if first.stderr.strip():
            notes.append(f"qpdf --generate-appearances: {first.stderr.strip()[:200]}")
        second = subprocess.run(
            base + ["--flatten-annotations=all", str(staged), str(out)],
            capture_output=True, text=True, timeout=300,
        )
        if second.returncode > 2 or not out.exists():
            fail(f"qpdf --flatten-annotations failed: {second.stderr.strip()[:400]}")
        if second.stderr.strip():
            notes.append(f"qpdf --flatten-annotations: {second.stderr.strip()[:200]}")
    return notes


def flatten_pypdf(
    reader: PdfReader, out: Path, values: dict[str, str | list[str]], profile: dict | None, password: str | None
) -> tuple[list[str], str | None]:
    """Fallback when qpdf is unavailable: draw the appearances into the page, then drop the form.

    Clones the already-decrypted reader, so an encrypted input flattens like any other.
    pypdf names the stamped XObject after the field, so every widget of a radio group
    gets the same name and the last one written wins: the selected button can come out
    unselected. qpdf --flatten-annotations=all has no such problem and is the default.
    """
    writer = PdfWriter(clone_from=reader)
    if values:
        writer.update_page_form_field_values(None, values, auto_regenerate=False, flatten=True)
    for page in writer.pages:
        annots = page.get("/Annots")
        if annots is None:
            continue
        keep = [a for a in annots if str(a.get_object().get("/Subtype", "")) != "/Widget"]
        if keep:
            page[NameObject("/Annots")] = ArrayObject(keep)
        else:
            del page[NameObject("/Annots")]
    root = writer.root_object
    if "/AcroForm" in root:
        del root[NameObject("/AcroForm")]
    owner_password = apply_protection(writer, profile, password)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("wb") as fh:
        writer.write(fh)
    return ["flattened with pypdf; qpdf --flatten-annotations=all is the preferred engine"], owner_password


def flatten(src: Path, out: Path, password: str | None, engine: str) -> dict:
    if out.resolve() == src.resolve():
        fail("output path must differ from the input; inputs are never modified in place")
    reader = open_reader(src, password)
    profile = encryption_profile(reader, password)
    before, before_state = collect_fields(reader)
    if not before_state["has_acroform"]:
        fail("no AcroForm in this file; there is nothing to flatten", form_state="no_acroform")
    secret = secret_names(before)

    def shown(name: str, value: str) -> str:
        return REDACTED if name in secret else value

    values = {f["name"]: f["value"] for f in before if f["value"] is not None}
    text_fields = [
        (f["name"], value)
        for f in before if f["type"] in ("text", "dropdown", "listbox")
        for value in written_strings(f["value"])
    ]
    labels = {}
    for f in before:
        for opt in f.get("option_labels") or []:
            if opt["value"] in written_strings(f["value"]):
                labels[opt["value"]] = opt["label"]
    pages_before = len(reader.pages)

    out.parent.mkdir(parents=True, exist_ok=True)
    fell_back = engine != "pypdf" and shutil.which("qpdf") is None
    if engine == "pypdf" or fell_back:
        notes, owner_password = flatten_pypdf(reader, out, {k: v for k, v in values.items() if v}, profile, password)
        engine_used = "pypdf"
    else:
        notes = flatten_qpdf(src, out, password)
        # qpdf copies the encryption dictionary over, /O and all, so both passwords still open it.
        owner_password = "reused" if profile else None
        engine_used = "qpdf"

    check = open_reader(out, password)
    after_profile = encryption_profile(check, password)
    protection_ok = profile is None or (
        after_profile is not None
        and after_profile["permissions"] == profile["permissions"]
        and after_profile["algorithm"] == profile["algorithm"]
    )
    after, after_state = collect_fields(check)
    widget_count = 0
    for page in check.pages:
        for ref in page.get("/Annots", []) or []:
            if str(ref.get_object().get("/Subtype", "")) == "/Widget":
                widget_count += 1
    found, missing = [], []
    text_layer_check, check_reason = "not_applicable", None
    if text_fields:
        text_layer_check, check_reason, layer = read_text_layer(out, password)
    if text_layer_check == "checked":
        for name, value in text_fields:
            bucket = found if (value in layer or labels.get(value, value) in layer) else missing
            bucket.append(shown(name, value))
    warnings: list[str] = []
    if text_layer_check == "skipped":
        warnings.append(
            f"the text layer was not read ({check_reason}), so nothing here has confirmed the baked "
            "values survived onto the page. Render the output and read it before delivering."
        )
    if owner_password == "replaced":
        warnings.append(OWNER_REPLACED)
    elif engine_used == "pypdf" and owner_password == "reused" and not profile["opens_empty"]:
        warnings.append(USER_REPLACED)
    if not protection_ok:
        warnings.append(
            f"the output reopens as {describe_protection(after_profile)} and the input is "
            f"{describe_protection(profile)}; the protection did not survive the flatten intact."
        )
    checked = [f["name"] for f in before if f["type"] in ("checkbox", "radio") and f["value"] and str(f["value"]) != "/Off"]
    if engine_used == "pypdf":
        lead = "qpdf is not on PATH, so the pypdf fallback ran. " if fell_back else ""
        detail = f" The selected state of {checked} is what to look at." if checked else ""
        warnings.append(
            f"{lead}pypdf stamps every widget of a group under one XObject name and the last one "
            f"written wins, so a selected checkbox or radio button can come out unselected.{detail} "
            "Render the output and confirm every box and button, or flatten with qpdf."
        )
    ok = (
        (not after_state["has_acroform"])
        and widget_count == 0
        and not missing
        and len(check.pages) == pages_before
        and protection_ok
    )
    report = {
        "status": "ok" if ok else "verification_failed",
        "operation": "flatten",
        "engine": engine_used,
        "engine_requested": engine,
        "input": str(src),
        "input_sha256": sha256(src),
        "output": str(out),
        "output_sha256": sha256(out),
        "input_encrypted": profile is not None,
        "output_encrypted": check.is_encrypted,
        **({"owner_password": owner_password} if owner_password else {}),
        "fields_before": len(before),
        "fields_after": len(after),
        "acroform_after": after_state["has_acroform"],
        "widget_annotations_after": widget_count,
        "pages_before": pages_before,
        "pages_after": len(check.pages),
        "values_baked": [shown(name, value) for name, value in text_fields],
        "text_layer_check": text_layer_check,
        **({"text_layer_check_reason": check_reason} if check_reason else {}),
        "text_layer_found": found,
        "text_layer_missing": missing,
        "appearance_verified": text_layer_check != "skipped" and not missing,
        "checked_buttons": checked,
        "warnings": warnings,
        "notes": notes + [
            "Flattening is one-way: the values become page content and can no longer be edited as "
            "fields. Keep the filled, unflattened file alongside it.",
            "Checkbox and radio ticks are drawn glyphs, not text, so the text-layer check cannot see "
            "them. Render the flattened page and look at every box.",
        ],
    }
    print(json.dumps(report, indent=2))
    sys.exit(0 if ok else 1)



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
    if not argv or argv[0] not in ("inspect", "fill", "flatten"):
        fail("usage: forms.py inspect|fill|flatten <file.pdf> [...]")
    command, argv = argv[0], argv[1:]
    opts, positional = parse_args(
        argv,
        ("--password", "--values", "--out", "--engine"),
        ("--need-appearances", "--truncate", "--complete"),
    )
    password = opts.get("--password")
    values = opts.get("--values")
    out = opts.get("--out")
    engine = opts.get("--engine", "qpdf")
    if not positional:
        fail(f"usage: forms.py {command} <file.pdf> [...]")
    src = Path(positional[0]).expanduser().resolve()

    if command == "inspect":
        print(json.dumps(form_report(src, password), indent=2))
        return
    if out is None:
        fail(f"forms.py {command} needs --out <file.pdf>")
    if command == "fill":
        if values is None:
            fail("forms.py fill needs --values <values.json>")
        fill(
            src,
            Path(values).expanduser().resolve(),
            Path(out).expanduser().resolve(),
            password,
            "--need-appearances" in opts,
            "--truncate" in opts,
            "--complete" in opts,
        )
    else:
        if engine not in ("qpdf", "pypdf"):
            fail("--engine must be qpdf or pypdf")
        flatten(src, Path(out).expanduser().resolve(), password, engine)


if __name__ == "__main__":
    main(sys.argv[1:])
