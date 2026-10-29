#!/usr/bin/env python3
"""Inspect, fill and flatten AcroForm PDFs, verifying the result each time.

Usage:
    python forms.py inspect <file.pdf> [--password PW]
    python forms.py fill    <in.pdf> --values values.json --out <out.pdf> [--password PW]
                            [--need-appearances] [--truncate] [--complete] [--drop-signatures]
    python forms.py flatten <in.pdf> --out <out.pdf> [--password PW] [--engine qpdf|pypdf]
                            [--drop-signatures]

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
that is not multi-select rejects an array. A combo box carrying the Edit flag reads
`editable: true` and takes typed text outside its own options, which is what a viewer
would store in it; every other choice field takes only the options the file declares.

A text value longer than the field's /MaxLen is rejected the same way, because a
viewer accepts the write and then draws only what fits: pass --truncate to cut it
here instead, which lists the field under `truncated`. Every required field left
empty after the fill is listed under `required_empty`, whether or not the values
file mentioned it, and --complete turns a non-empty list into an error. Values
stored and appearance verified are two separate results: `appearance_verified`
goes false when the text inside the field's own box is not the value written
there, which means the value is in the file but not on the page. The test is
equality once whitespace is dropped, not containment, so an appearance still
reading `Annual` does not verify a field refilled with `Ann`; `text_layer_missing`
names the field, what it holds and what its box says. The same words printed
elsewhere on the page are somebody else's text and do not verify this field.

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

A signed document is refused before anything is written. Every write here rebuilds the whole
file, which moves the bytes the signature was made over: the signature stops verifying while the
page goes on showing the signature block. `fill` and `flatten` both fail with the field names, and
--drop-signatures is the way through: it takes the signature and the appearance its widget draws
out of the output, lists them under `signatures_dropped`, and delivers a document that no longer
claims to be signed. A signature field with nothing in it is a placeholder, not a signature, and
never blocks. Nothing here signs a document.

An encrypted input comes back encrypted: same permission flags, same cipher, read off the
encryption dictionary's own crypt filter rather than its revision, confirmed by reopening
the output, and `output_encrypted` says so. Only the password that opened the file is
knowable, so the other one of the pair can change. Opened with the user password, a random
owner password replaces the original, nobody holds the override and `owner_password` reads
`replaced`. The other direction is refused rather than written: opened with the owner
password of a file whose user password is a different string, `fill` and `flatten` exit 1,
because that user password cannot be read out of the file, the output would take the
supplied one in its place, and it would then open for nobody who held the original. Pass the
user password instead; the permission flags and the cipher come out the same either way. One
password that is both the user and the owner password loses nothing, still works, and reads
`reused`. A file that opens with no password keeps opening with none, and qpdf-engine flatten
copies the encryption dictionary whole, so it changes neither password and has nothing to
refuse. qpdf writes no RC4 crypt filter, so a legacy RC4-128 file flattened with that engine
comes back AES-128 and the check refuses it; flatten that one with --engine pypdf. Taking
protection off is `pages.py decrypt`, when the user asks for it, not a side effect of filling
a form.

A field is addressed by its qualified name, or by its leaf name when exactly one
field carries it: a leaf two fields share, `name` under both `billing` and `shipping`,
is refused with both qualified names rather than resolved to one of them.

Every output is written to a temporary sibling and moved onto the requested path only
once the result verifies, so a rerun that fails leaves the file already there untouched
and reports `output_written: false`. A flatten whose values the text layer cannot find
is still delivered, because the answer to that one is to render the page and look.

Inputs are never written to. The output path must differ from the input.
Exit code is 1 for a hard error (bad file, unknown field, a value the field cannot
hold, verification failure, or an incomplete form under --complete).
"""

from __future__ import annotations

import atexit
import hashlib
import html
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from pypdf import PasswordType, PdfReader, PdfWriter
from pypdf._encryption import AlgV4, AlgV5
from pypdf.generic import (
    ArrayObject,
    DecodedStreamObject,
    DictionaryObject,
    FloatObject,
    NameObject,
    TextStringObject,
)

# qpdf's own exit codes: 0 clean, 2 an error, 3 warnings it recovered from with the
# output still written. A run that only warns is a usable result, an error never is.
QPDF_WARNING = 3

FF_READONLY = 1 << 0
FF_REQUIRED = 1 << 1
FF_NOEXPORT = 1 << 2
FF_MULTILINE = 1 << 12
FF_PASSWORD = 1 << 13
FF_RADIO = 1 << 15
FF_PUSHBUTTON = 1 << 16
FF_COMBO = 1 << 17
FF_EDIT = 1 << 18
FF_MULTISELECT = 1 << 21
# An annotation's /F Hidden and NoView bits. A widget carrying either is not on the page
# a viewer draws, so flattening must not put it there.
HIDDEN_ANNOT = (1 << 1) | (1 << 5)

TRUTHY = {"true", "yes", "on", "1", "x", "checked"}
FALSY = {"false", "no", "off", "0", "", "unchecked"}
# One word of pdftotext -bbox: its text and the box poppler drew it in.
WORD_BOX = re.compile(
    r'<word xMin="([-\d.eE+]+)" yMin="([-\d.eE+]+)" xMax="([-\d.eE+]+)" yMax="([-\d.eE+]+)">(.*?)</word>',
    re.DOTALL,
)
# Slack in points around a widget's box. A glyph sits a little outside the box it was drawn
# for, and an appearance a point off its rect is still this field's answer.
WIDGET_PAD = 2.0
# What a password field's value is replaced with everywhere this script prints JSON. The
# real value still goes into the PDF and is still read back and checked; it is the report
# that must not carry it, because the report is logged, pasted and handed on.
REDACTED = "<redacted>"
# A crypt filter's /CFM, and the pypdf algorithm that writes the same cipher back. /R cannot
# tell these apart: revision 4 covers both RC4-128 and AES-128, so reading the revision alone
# rewrites an RC4 file as AES and then reports that nothing changed.
CIPHER_BY_CFM = {"/V2": "RC4-128", "/AESV2": "AES-128", "/AESV3": "AES-256"}
# Both of these come off the same limit: a file hands back the permission flags and the cipher,
# but only the one password that opened it. The unknown half is either the owner password, which a
# random one can take over without costing anyone access, or the user password, which nothing can.
OWNER_REPLACED = (
    "the output keeps the input's user password, permissions and algorithm, but not its owner "
    "password: that one cannot be read out of a file opened with the user password. A random one "
    "replaced it and is recorded nowhere, so the restrictions stand and nobody holds the override. "
    "pages.py encrypt sets a fresh pair when the user asks for one."
)
USER_LOCKOUT = (
    "this file was opened with its owner password and its user password is a different string, "
    "which cannot be read out of the file. Writing the output would silently change who can open "
    "it: the password passed here would take the user password's place, and everyone holding the "
    "real one would be shut out of a file with nothing in it to say so. Run this again with "
    "--password set to the user password, which keeps the same permission flags and the same "
    "cipher and replaces only the owner password, with a random one nobody holds."
)
SIGNATURES_DROPPED = (
    "signature(s) {names} were removed on the way out. This write moves the bytes the signature was "
    "made over, so it could not have verified afterwards either way; the output is unsigned and draws "
    "no signature block, and the document has to be signed again by whoever signed it."
)
# The conventional short tags an appearance stream uses for the standard 14 fonts.
# A checkbox draws its tick as ZapfDingbats text, so an undefined /ZaDb renders an empty box.
STANDARD_TAGS = {
    "Helv": "Helvetica", "HeBo": "Helvetica-Bold", "HeOb": "Helvetica-Oblique",
    "TiRo": "Times-Roman", "TiBo": "Times-Bold", "TiIt": "Times-Italic",
    "Cour": "Courier", "CoBo": "Courier-Bold", "Symb": "Symbol", "ZaDb": "ZapfDingbats",
}
FONT_TAG = re.compile(rb"/([A-Za-z][A-Za-z0-9]*)\s+[\d.]+\s+Tf")
# pypdf marks a list box's selected row with `<rect> re` followed by `<grey> rg s`: `rg`
# sets the fill colour and `s` strokes, so the row is drawn as an outline in the stroke
# colour rather than a band, and the field's own clip trims that outline away to nothing.
# Both operators also sit inside the BT/ET text object, where a path is undefined and
# poppler drops it. The rectangles are the right rectangles; only the painting is wrong,
# so these match the pair exactly and nothing else, and a stream without it is left alone.
NUMBER = rb"-?\d*\.?\d+(?:[eE][-+]?\d+)?"
SELECTION_RECT = re.compile(rb"^\s*((?:" + NUMBER + rb" ){3}" + NUMBER + rb") re\s*$")
SELECTION_PAINT = re.compile(rb"^\s*(?:" + NUMBER + rb" ){2}" + NUMBER + rb" rg s\s*$")
# The band the repaint fills instead: the light blue Acrobat draws behind a selected row,
# pale enough that the row's own text still reads over it.
SELECTION_FILL = b"0.6 0.76 0.86"


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
    reader = pdf_reader(path)
    if reader.is_encrypted:
        try:
            ok = reader.decrypt(password or "")
        except Exception as exc:
            fail(f"cannot decrypt {path}: {exc}", password_required=True)
        if not ok:
            fail(f"{path} is encrypted; pass --password", password_required=True)
    return reader


def encryption_cipher(encrypt) -> str:
    """The cipher the encryption dictionary actually names, as a pypdf algorithm.

    From /V 4 on, the cipher lives in the standard crypt filter's /CFM and not in /V or /R,
    which is the only thing separating revision-4 RC4-128 from AES-128.
    """
    version = int(encrypt.get("/V", 0) or 0)
    if version >= 4:
        crypt_filters = encrypt.get("/CF")
        std = crypt_filters.get_object().get("/StdCF") if crypt_filters is not None else None
        cfm = str(std.get_object().get("/CFM", "")) if std is not None else ""
        return CIPHER_BY_CFM.get(cfm) or ("AES-256" if version >= 5 else "AES-128")
    # /V 1 is 40-bit RC4 and /V 2 carries its key length in /Length. pypdf writes only the two
    # strengths Acrobat ever produced, so anything longer than 40 bits is written as 128.
    return "RC4-40" if version <= 1 or int(encrypt.get("/Length", 40) or 40) <= 40 else "RC4-128"


def opens_as_user(reader: PdfReader, password: str | None) -> bool:
    """Whether `password` is this file's user password, and not only its owner password.

    `decrypt` cannot answer that: pypdf verifies the owner password first and returns on the
    first match, so a password that is both reads OWNER and never USER. Only the /U half of the
    encryption dictionary separates them, and pypdf's own algorithms are what read it.
    """
    encryption = reader._encryption
    pwd = encryption._encode_password(password or "")
    if encryption.V <= 4:
        return bool(AlgV4.verify_user_password(
            pwd, encryption.R, encryption.Length, encryption.values.O, encryption.values.U,
            encryption.P, encryption.id1_entry, encryption.EncryptMetadata,
        ))
    return bool(AlgV5.verify_user_password(encryption.R, pwd, encryption.values.U, encryption.values.UE))


def encryption_profile(reader: PdfReader, password: str | None) -> dict | None:
    """What it takes to write the output back under the input's own protection.

    None of it survives the write: a pypdf writer starts unencrypted, so the cipher, the
    permission bits and which of the two passwords opened the file have to be read off the
    input first. Decrypting a second time is how pypdf reports which password matched.
    """
    if not reader.is_encrypted:
        return None
    encrypt = reader.trailer["/Encrypt"].get_object()
    # A rejected attempt costs nothing and leaves the reader decrypted; the supplied password
    # goes last so the reader ends on the key it came in with.
    opens_empty = reader.decrypt("") != PasswordType.NOT_DECRYPTED
    return {
        "algorithm": encryption_cipher(encrypt),
        "permissions": reader.user_access_permissions,
        "opens_empty": opens_empty,
        "user_opened": opens_as_user(reader, password),
        # Last, so the reader ends on the key it came in with.
        "owner_opened": reader.decrypt(password or "") == PasswordType.OWNER_PASSWORD,
    }


def apply_protection(writer: PdfWriter, profile: dict | None, password: str | None) -> str | None:
    """Re-encrypt the output as the input was. Returns how the owner password was set, or None.

    The owner password cannot be read out of a file opened with the user password, so a random
    one takes its place: the restrictions stay enforced and the override goes to nobody, which
    is the safe direction. Reusing the supplied password there would hand every holder of it
    the right to lift the restrictions it is meant to enforce. A file that opens with no
    password keeps opening with none.

    The mirror of that is not a safe direction and is refused here instead: an unreadable user
    password cannot be stood in for, because anything written in its place takes the file away
    from everyone who holds the real one. The refusal lives in this function so that it covers
    every write that rewrites the pair, and only those.
    """
    if profile is None:
        return None
    if profile["owner_opened"] and not profile["opens_empty"] and not profile["user_opened"]:
        fail(USER_LOCKOUT, user_password_required=True)
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


def holder(node, key: str):
    """The node in this field's parent chain that carries `key`, or None.

    Deleting an inherited entry has to happen on the node that holds it, which is not
    always the terminal field; reading one does not care, so `inherited` is this plus
    the lookup.
    """
    seen = 0
    while node is not None and seen < 32:
        if key in node:
            return node
        parent = node.get("/Parent")
        node = parent.get_object() if parent is not None else None
        seen += 1
    return None


def inherited(node, key: str):
    owner = holder(node, key)
    return owner[key] if owner is not None else None


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


def acroform_of(pdf) -> DictionaryObject | None:
    """The /AcroForm dictionary of a reader or a writer, or None when the file carries no form."""
    form = pdf.root_object.get("/AcroForm")
    return form.get_object() if form is not None else None


def terminal_fields(acroform) -> list[tuple[str, DictionaryObject, list]]:
    """Every terminal field under /Fields, as (qualified name, field node, widget references).

    One walk for every caller: a signature nested two groups down has to be found by the
    same traversal that names the fields, or it is missed by exactly the reports that
    would have caught it.
    """
    found: list[tuple[str, DictionaryObject, list]] = []

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
        found.append((qualified, node, kid_objects or [ref]))

    for ref in acroform.get("/Fields", []) or []:
        walk(ref, "", 0)
    return found


def collect_fields(reader: PdfReader) -> tuple[list[dict], dict]:
    """Walk /AcroForm /Fields, returning one entry per terminal field."""
    acroform = acroform_of(reader)
    state = {"has_acroform": acroform is not None, "xfa": False, "need_appearances": False}
    if acroform is None:
        return [], state
    state["xfa"] = "/XFA" in acroform
    state["need_appearances"] = as_bool(acroform.get("/NeedAppearances", False))
    pages = widget_pages(reader)
    fields: list[dict] = []
    for qualified, node, widgets in terminal_fields(acroform):
        name = to_text(node.get("/T")) or ""
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
            if kind == "dropdown" and flags & FF_EDIT:
                # The Edit flag turns a combo box into a text box with a list attached: the
                # viewer stores whatever was typed, so /Opt here suggests rather than
                # constrains and a value outside it is the field working as designed.
                entry["editable"] = True
        if kind == "signature":
            # A signed field's /V is the signature dictionary itself, /Contents and all:
            # printing it buries the report in bytes nobody can read, and it is not a value
            # anything here could write back. Whether the field is signed is the useful part.
            entry["signed"] = is_signed(value)
            entry["value"] = None
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
    return fields, state


def is_signed(value) -> bool:
    """True when a signature field's /V is a signature and not an empty placeholder.

    A signature covers a byte range of the file it was made in and stores the bytes it
    signed with; a field carrying neither signs nothing, so rewriting the file past it
    breaks nothing.
    """
    value = value.get_object() if value is not None else None
    return isinstance(value, DictionaryObject) and "/ByteRange" in value and "/Contents" in value


def populated_signatures(acroform) -> list[tuple[str, DictionaryObject, list]]:
    """The terminal fields that hold a signature, shaped as `terminal_fields` returns them."""
    if acroform is None:
        return []
    return [
        (qualified, node, widgets)
        for qualified, node, widgets in terminal_fields(acroform)
        if str(inherited(node, "/FT") or "") == "/Sig" and is_signed(inherited(node, "/V"))
    ]


def guard_signatures(reader: PdfReader, drop: bool) -> list[str]:
    """Refuse to rewrite a signed document unless the caller asked for the signature to go.

    A signature is made over the bytes of the file it was signed in, and every write here
    rebuilds the file, so there is no version of this that keeps one. Failing closed is the
    only honest default: the alternative ships a document that still shows a signature block
    and no longer verifies, which is worse than either a refusal or an unsigned document.
    """
    signed = [name for name, _, _ in populated_signatures(acroform_of(reader))]
    if signed and not drop:
        fail(
            f"signed field(s) {signed} hold a signature over this file's bytes, and writing the file "
            "again breaks it while the page goes on showing the signature block. Ask for an unsigned "
            "copy of the form, or pass --drop-signatures to write anyway, which takes the signature "
            "and its appearance out of the output and reports them under signatures_dropped.",
            signed_fields=signed,
        )
    return signed


def blank_appearance(writer: PdfWriter, widget) -> DictionaryObject:
    """An appearance that draws nothing, in the widget's own box.

    Deleting /AP outright leaves a widget no renderer can draw, and qpdf then declines to
    flatten it and leaves it on the page as an annotation, which fails the flatten's own
    check. An empty stream shows nothing and flattens like any other.
    """
    rect = [float(v.get_object()) for v in (widget.get("/Rect") or [])]
    width, height = (abs(rect[2] - rect[0]), abs(rect[3] - rect[1])) if len(rect) == 4 else (0.0, 0.0)
    stream = DecodedStreamObject()
    stream.set_data(b"")
    stream.update({
        NameObject("/Type"): NameObject("/XObject"),
        NameObject("/Subtype"): NameObject("/Form"),
        NameObject("/BBox"): ArrayObject(
            [FloatObject(0), FloatObject(0), FloatObject(width), FloatObject(height)]
        ),
        NameObject("/Resources"): DictionaryObject(),
    })
    appearance = DictionaryObject()
    appearance[NameObject("/N")] = writer._add_object(stream)
    return appearance


def strip_signatures(writer: PdfWriter) -> list[str]:
    """Take every signature and the block its widget draws out of the writer, returning the names.

    The appearance goes with the value: a page that still shows `Signed by ...` on a document
    nothing can verify is the failure this whole path exists to avoid.
    """
    dropped: list[str] = []
    for qualified, node, widgets in populated_signatures(acroform_of(writer)):
        owner = holder(node, "/V")
        if owner is not None:
            del owner[NameObject("/V")]
        for ref in widgets:
            widget = ref.get_object()
            if "/AS" in widget:
                del widget[NameObject("/AS")]
            if "/AP" in widget:
                widget[NameObject("/AP")] = blank_appearance(writer, widget)
        dropped.append(qualified)
    return dropped


def unsigned_copy(
    reader: PdfReader, profile: dict | None, password: str | None
) -> tuple[Path, list[str], str | None]:
    """A temporary copy with the signatures stripped, for an engine that reads a path.

    qpdf draws a widget's appearance into the page whatever the widget is, so the signature
    block has to be gone before it runs rather than after.
    """
    writer = PdfWriter(clone_from=reader)
    dropped = strip_signatures(writer)
    owner_password = apply_protection(writer, profile, password)
    directory = Path(tempfile.mkdtemp(prefix="unsigned_"))
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    staged = directory / "unsigned.pdf"
    with staged.open("wb") as fh:
        writer.write(fh)
    return staged, dropped, owner_password


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
        # A field with no /Opt at all has nothing to match against, and an editable combo
        # accepts typed text by design, so for both the value written is the value given.
        free = not allowed or bool(entry.get("editable"))
        if isinstance(raw, (list, tuple)):
            if not entry.get("multiselect"):
                return None, f"{entry['name']}: takes one selection, not a list; it is not multi-select"
            picked: list[str] = []
            for item in raw:
                value = match_option(options, item)
                if value is None and free:
                    value = str(item)
                if value is None:
                    return None, f"{entry['name']}: {str(item)!r} is not an option; allowed export values are {allowed}"
                if value not in picked:
                    picked.append(value)
            return picked, None
        text = "" if raw is None else str(raw)
        value = match_option(options, text)
        if value is None and free:
            return text, None
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


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def read_text_boxes(path: Path, password: str | None) -> tuple[str, str | None, list[list[tuple]]]:
    """Return (state, reason, placed words per page), where state is "checked" or "skipped".

    An absent or failing pdftotext leaves no text to compare against. Treating that as an
    empty set of missing values would report a verification that never ran as a passed one,
    so the caller is told the check was skipped and why.

    `-bbox` carries each word's box, which is what lets a written value be checked where
    its own widget sits rather than anywhere in the document.
    """
    if shutil.which("pdftotext") is None:
        return "skipped", "pdftotext is not on PATH", []
    proc = subprocess.run(
        ["pdftotext", "-bbox"] + (["-upw", password, "-opw", password] if password else []) + [str(path), "-"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        return "skipped", f"pdftotext exited {proc.returncode}: {proc.stderr.strip()[:200]}", []
    pages = [
        [
            (html.unescape(m.group(5)), float(m.group(1)), float(m.group(2)), float(m.group(3)), float(m.group(4)))
            for m in WORD_BOX.finditer(chunk)
        ]
        for chunk in proc.stdout.split("<page")[1:]
    ]
    return "checked", None, pages


def page_geometry(reader: PdfReader) -> list[tuple[tuple[float, float, float, float], int]]:
    """Per page, the MediaBox and the rotation: the two things that place a word box."""
    geometry = []
    for page in reader.pages:
        box = page.mediabox
        geometry.append((
            (float(box.left), float(box.bottom), float(box.right), float(box.top)),
            int(page.rotation or 0),
        ))
    return geometry


def widget_box(rect: list[float], mediabox: tuple[float, float, float, float], rotation: int) -> tuple:
    """A widget's /Rect in the frame pdftotext -bbox reports words in.

    That frame hangs off the top left corner of the MediaBox (poppler places text there
    whether or not a CropBox narrows the view) and it carries the page rotation, so a rect
    written in user space has to be turned the same way before the two can be compared.
    """
    mx0, my0, mx1, my1 = mediabox
    width, height = mx1 - mx0, my1 - my0
    x0, x1 = sorted((rect[0] - mx0, rect[2] - mx0))
    y0, y1 = sorted((rect[1] - my0, rect[3] - my0))
    turn = int(rotation) % 360
    if turn == 90:
        return y0, x0, y1, x1
    if turn == 180:
        return width - x1, y0, width - x0, y1
    if turn == 270:
        return height - y1, width - x1, height - y0, width - x0
    return x0, height - y1, x1, height - y0


def text_in_box(words: list[tuple], box: tuple) -> str:
    """The words drawn inside one widget box, in reading order.

    A word counts when its middle sits at the box's height and it overlaps the box at all
    across: that leaves out the line above and the label alongside, and keeps in a value
    drawn wider than the box it was given.
    """
    x0, y0, x1, y1 = box
    return normalize_space(" ".join(
        text
        for text, wx0, wy0, wx1, wy1 in words
        if y0 - WIDGET_PAD <= (wy0 + wy1) / 2 <= y1 + WIDGET_PAD
        and wx1 >= x0 - WIDGET_PAD
        and wx0 <= x1 + WIDGET_PAD
    ))


def drawn_text(fields: list[dict], pages: list[list[tuple]], geometry: list[tuple]) -> dict[str, dict]:
    """Field name to the text drawn where that field's own widgets sit, one entry per widget.

    The document-wide text layer answers the wrong question: a page that already reads
    "Annual report" confirms a field whose value "Ann" never reached the page, and a stale
    appearance drawing "No" passes for a field holding "Yes" as long as some other line
    says "Yes". Each widget keeps its own box, so a field repeated across pages is answered
    by whichever copy drew the value rather than by the two of them run together. A field
    whose widget is on no page has nowhere to look: `scope` reads "document" and the one
    box is the whole text layer, which can only prove the text is somewhere in the file.
    """
    whole = normalize_space(" ".join(word[0] for page in pages for word in page))
    drawn = {}
    for field in fields:
        placed = field.get("placements") or [{"page": field.get("page"), "rect": field.get("rect")}]
        local = [
            text_in_box(pages[place["page"] - 1], widget_box(place["rect"], *geometry[place["page"] - 1]))
            for place in placed
            if place.get("page") and place.get("rect") and place["page"] <= min(len(pages), len(geometry))
        ]
        drawn[field["name"]] = {"boxes": local, "scope": "widget"} if local else {"boxes": [whole], "scope": "document"}
    return drawn


def dense(text: str) -> str:
    """Every space gone, so what is left is the glyph sequence whatever the layout did to it."""
    return re.sub(r"\s+", "", text)


def drawn_summary(drawn: dict) -> str:
    """What the box says, for a report that has to explain why a value did not verify."""
    joined = " | ".join(box for box in drawn["boxes"] if box) or "(nothing)"
    return joined if len(joined) <= 200 else joined[:200] + "..."


def appearance_shows(field: dict, drawn: dict, written: list[str]) -> bool:
    """Does the text drawn where this field sits say what was written into it?

    Equality, not containment: the stale appearance of a field that read "Annual" contains
    the "Ann" written over it, so a containment test passes a page still showing the old
    value. Whitespace goes first on both sides, which is what lets a multiline value wrapped
    across lines and a comb field's one glyph per cell still match the value they were
    drawn from; the glyphs and their order are what the check is really after.

    Two shapes cannot be held to equality, and both stay containment tests. A field with no widget
    on a page has only the whole text layer to be found in. And a choice field's appearance
    need not be its value: a listbox draws every option it can show with the selection only
    highlighted, and a dropdown draws the display label wherever that differs from the
    export value, so either spelling of such a value counts. That label comes from this
    field's own options: an export value is unique only inside its own field, so two combos
    can each spell "1", one for "Annual" and one for "One". A map shared across the form lets
    the other field's label answer here, rejecting a box that draws the right label and
    passing one that draws a stale one.
    """
    wanted = [value for value in written if dense(value)]
    if not wanted:
        return True
    labels = {opt["value"]: opt["label"] for opt in field.get("option_labels") or []}
    lenient = (
        drawn["scope"] == "document"
        or field["type"] == "listbox"
        or any(labels.get(value, value) != value for value in wanted)
    )
    if lenient:
        return all(
            any(
                dense(value) in dense(box) or dense(labels.get(value, value)) in dense(box)
                for box in drawn["boxes"]
            )
            for value in wanted
        )
    # One widget drawing the value answers the field; another copy of it can be hidden or
    # empty without that meaning the value never reached the page.
    return any(dense(box) == dense(" ".join(wanted)) for box in drawn["boxes"])


def stage_output(out: Path) -> Path:
    """Return a temporary sibling of `out` to write into.

    A deliverable is only replaced by a result that verified: writing straight to `out`
    would destroy the previous one the moment the writer opens it. The sibling shares the
    directory, so the move onto `out` is atomic, and it is removed at exit if it is still
    there, which is every path that did not move it.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=out.parent, prefix=f".{out.name}.", suffix=".tmp")
    os.close(handle)
    staged = Path(name)
    atexit.register(staged.unlink, missing_ok=True)
    return staged


def repaint_selection(widget) -> bool:
    """Redraw a list box widget's selected rows as filled bands, returning whether it changed.

    The bands move ahead of the `BT` because a fill drawn after a row's text would cover
    it, and because a path belongs outside a text object at all. `n` ends the clip path
    pypdf leaves pending on its `W`, so the bands are clipped to the field's interior the
    way the stroked outlines were, and the `q`/`Q` keeps the fill colour off the text.
    """
    stream = visible_appearance(widget)
    if stream is None:
        return False
    try:
        data = stream.get_data()
    except Exception:
        return False
    lines = data.split(b"\n")
    start = next((i for i, line in enumerate(lines) if line.strip() == b"BT"), None)
    if start is None:
        return False
    head, body, rects = lines[:start], [], []
    index = start
    while index < len(lines):
        rect = SELECTION_RECT.match(lines[index])
        if rect and index + 1 < len(lines) and SELECTION_PAINT.match(lines[index + 1]):
            rects.append(rect.group(1))
            index += 2
            continue
        body.append(lines[index])
        index += 1
    if not rects:
        return False
    bands = [b"q", SELECTION_FILL + b" rg", *(rect + b" re" for rect in rects), b"f", b"Q"]
    clip = max((i for i, line in enumerate(head) if line.strip() == b"W"), default=-1)
    head = head[: clip + 1] + [b"n"] + bands + head[clip + 1 :] if clip >= 0 else head + bands
    try:
        stream.set_data(b"\n".join(head + body))
    except Exception:
        return False
    return True


def write_field_values(
    writer: PdfWriter,
    resolved: dict[str, str | list[str]],
    regenerate: bool = True,
    stamp: bool = False,
) -> None:
    """Write every resolved value, drawing a list box's options as the labels it shows.

    pypdf draws a list box by joining its /Opt, which the spec lets a file write as plain
    strings or as [export value, label] pairs; joining the pairs raises TypeError from
    inside pypdf and the run dies with a traceback, printing no report at all. A list box
    shows its labels and stores its export values, so the generator is handed the flat
    array of labels the simple form would carry and the label of the selection, which is
    also what marks the selected row. The pairs and the export values go back before the
    file is written, so it keeps the /Opt and the values it came with.
    """
    acroform = acroform_of(writer)
    terminals = terminal_fields(acroform) if acroform else []
    nodes = {name: node for name, node, _ in terminals}
    widgets = {name: refs for name, _, refs in terminals}
    stand_ins: dict[str, str | list[str]] = dict(resolved)
    swapped: list[tuple] = []
    listboxes: list[str] = []
    for name, value in resolved.items():
        node = nodes.get(name)
        if node is None:
            continue
        kind = field_kind(str(inherited(node, "/FT") or ""), int(inherited(node, "/Ff") or 0))
        if kind != "listbox":
            continue
        listboxes.append(name)
        owner = holder(node, "/Opt")
        if owner is None:
            continue
        entries = owner["/Opt"].get_object()
        if all(isinstance(entry.get_object(), (str, bytes)) for entry in entries):
            continue
        options = choice_options(node)
        labels = {o["value"]: o["label"] for o in options}
        stand_in = (
            [labels.get(v, v) for v in value] if isinstance(value, list) else labels.get(value, value)
        )
        owner[NameObject("/Opt")] = ArrayObject([TextStringObject(o["label"]) for o in options])
        stand_ins[name] = stand_in
        swapped.append((owner, entries, node, value, stand_in))

    writer.update_page_form_field_values(None, stand_ins, auto_regenerate=regenerate, flatten=stamp)

    # Every list box here has just been redrawn, selected rows and all, so this is where the
    # unpainted ones get their band. Stamping took the same stream object into the page, so
    # a flatten through this path is repainted by the same pass.
    for name in listboxes:
        for ref in widgets.get(name, []):
            repaint_selection(ref.get_object())

    for owner, entries, node, value, stand_in in swapped:
        owner[NameObject("/Opt")] = entries
        # Only where the stand-in is what the field now holds: a widget on no page is
        # never visited, and writing the value here would report a fill it never got.
        if same_value(to_value(node.get("/V")), stand_in):
            node[NameObject("/V")] = (
                ArrayObject([TextStringObject(v) for v in value])
                if isinstance(value, list)
                else TextStringObject(value)
            )


def fill(
    src: Path,
    values_path: Path,
    out: Path,
    password: str | None,
    need_appearances: bool,
    truncate: bool,
    complete: bool,
    drop_signatures: bool,
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
    guard_signatures(reader, drop_signatures)
    by_name: dict[str, dict] = {f["name"]: f for f in fields}
    # A leaf name stands in for the qualified name only while it points at one field.
    # Shared by two, billing.name and shipping.name, it names neither: resolving it to
    # whichever came first fills an arbitrary field and reports success for it.
    leaves: dict[str, list[dict]] = {}
    for f in fields:
        if f["leaf_name"] and f["leaf_name"] not in by_name:
            leaves.setdefault(f["leaf_name"], []).append(f)
    by_name.update({leaf: group[0] for leaf, group in leaves.items() if len(group) == 1})
    ambiguous = {
        leaf: sorted(f["name"] for f in group)
        for leaf, group in leaves.items()
        if len(group) > 1 and leaf in payload
    }
    if ambiguous:
        fail(
            f"ambiguous field name(s): {ambiguous}; each is the leaf of more than one qualified "
            "name, so write the qualified name of the field you mean",
            ambiguous_fields=ambiguous,
        )

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
    signatures_dropped = strip_signatures(writer)
    write_field_values(writer, resolved)
    writer.set_need_appearances_writer(need_appearances)
    repaired_fonts, unknown_fonts = repair_appearance_fonts(writer)
    owner_password = apply_protection(writer, profile, password)
    staged = stage_output(out)
    with staged.open("wb") as fh:
        writer.write(fh)

    # Independent read of the result: pypdf's own report on the writer is not evidence.
    check_reader = open_reader(staged, password)
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
    missing_in_text: list[dict] = []
    text_layer_check, check_reason = "not_applicable", None
    if text_fields:
        text_layer_check, check_reason, words = read_text_boxes(staged, password)
    if text_layer_check == "checked":
        by_field = {f["name"]: f for f in fields}
        drawn = drawn_text(fields, words, page_geometry(check_reader))
        missing_in_text = [
            {
                "field": n,
                "expected": shown(n, resolved[n]),
                "drawn": shown(n, drawn_summary(drawn[n])),
            }
            for n in text_fields
            if not appearance_shows(by_field[n], drawn[n], written_strings(resolved[n]))
        ]
        listed = [entry["field"] for entry in missing_in_text]
        if missing_in_text and after_state["need_appearances"]:
            warnings.append(
                f"field(s) {listed} hold their value but no text layer sits inside their own box; the "
                "viewer will draw them from NeedAppearances. Render the page and look before delivering."
            )
        elif missing_in_text:
            warnings.append(
                f"field(s) {listed} hold their value but the text drawn in their own box is not it: "
                "text_layer_missing says what the box does say. A stale appearance the write did not "
                "redraw, a value too long for the box, a hidden widget, or a font the appearance "
                "cannot draw. Nothing will redraw it, so render the page and look at what the field "
                "actually shows."
            )
    elif text_layer_check == "skipped":
        warnings.append(
            f"the text layer was not read ({check_reason}), so nothing here has seen the written values "
            "on the page. appearance_verified is false because the check did not run, not because a "
            "value is missing: render the page and look at every field."
        )
    if owner_password == "replaced":
        warnings.append(OWNER_REPLACED)
    if not protection_ok:
        warnings.append(
            f"the output reopens as {describe_protection(after_profile)} and the input is "
            f"{describe_protection(profile)}; the protection did not survive the write intact."
        )
    if after_state["xfa"]:
        warnings.append("XFA form: an XFA-aware viewer may ignore these AcroForm values.")
    for f in fields:
        # A list box draws its labels, so only a combo box's box disagrees with Acrobat.
        if f["name"] not in resolved or f["type"] != "dropdown":
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
    if signatures_dropped:
        warnings.append(SIGNATURES_DROPPED.format(names=signatures_dropped))
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
    written = status == "ok"
    if written:
        os.replace(staged, out)
    else:
        warnings.append(
            f"nothing was written to {out}: this fill did not verify, so the result was discarded "
            "instead of replacing whatever is already at that path."
        )
    report = {
        "status": status,
        **({"message": message} if message else {}),
        "operation": "fill",
        "input": str(src),
        "input_sha256": sha256(src),
        "output": str(out),
        "output_written": written,
        **({"output_sha256": sha256(out)} if written else {}),
        "input_encrypted": profile is not None,
        "output_encrypted": check_reader.is_encrypted,
        **({"owner_password": owner_password} if owner_password else {}),
        "fields_written": len(resolved),
        "fields_total": len(fields),
        "signatures_dropped": signatures_dropped,
        "need_appearances": after_state["need_appearances"],
        "appearance_fonts_repaired": repaired_fonts,
        "plan": plan,
        "verification": verification,
        "text_layer_check": text_layer_check,
        **({"text_layer_check_reason": check_reason} if check_reason else {}),
        "text_layer_missing": missing_in_text,
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
        wrote = staged.exists() and staged.stat().st_size > 0
        if first.returncode not in (0, QPDF_WARNING) or not wrote:
            fail(f"qpdf --generate-appearances failed: {first.stderr.strip()[:400]}")
        if first.stderr.strip():
            notes.append(f"qpdf --generate-appearances: {first.stderr.strip()[:200]}")
        second = subprocess.run(
            base + ["--flatten-annotations=all", str(staged), str(out)],
            capture_output=True, text=True, timeout=300,
        )
        wrote = out.exists() and out.stat().st_size > 0
        if second.returncode not in (0, QPDF_WARNING) or not wrote:
            fail(f"qpdf --flatten-annotations failed: {second.stderr.strip()[:400]}")
        if second.stderr.strip():
            notes.append(f"qpdf --flatten-annotations: {second.stderr.strip()[:200]}")
    return notes


def visible_appearance(annot):
    """The appearance stream a viewer is drawing for this widget right now.

    /AP /N is the stream itself for a text field and one stream per state for a
    button, picked by the widget's /AS.
    """
    appearance = annot.get("/AP")
    stream = appearance.get_object().get("/N") if appearance is not None else None
    stream = stream.get_object() if stream is not None else None
    if stream is not None and not hasattr(stream, "get_data"):
        state = annot.get("/AS")
        stream = stream.get(state) if state is not None else None
        stream = stream.get_object() if stream is not None else None
    return stream if hasattr(stream, "get_data") else None


def stamped_streams(page) -> set[int]:
    """Object numbers of the appearance streams already drawn into this page."""
    resources = page.get("/Resources")
    xobjects = resources.get_object().get("/XObject") if resources is not None else None
    values = xobjects.get_object().values() if xobjects is not None else []
    return {ref.idnum for ref in values if hasattr(ref, "idnum")}


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
        write_field_values(writer, values, regenerate=False, stamp=True)
    for page in writer.pages:
        annots = page.get("/Annots")
        if annots is None:
            continue
        drawn = stamped_streams(page)
        for ref in annots:
            annot = ref.get_object()
            rect = annot.get("/Rect")
            if str(annot.get("/Subtype", "")) != "/Widget" or rect is None:
                continue
            if int(annot.get("/F", 0)) & HIDDEN_ANNOT:
                continue
            stream = visible_appearance(annot)
            reference = getattr(stream, "indirect_reference", None)
            if stream is None or (reference is not None and reference.idnum in drawn):
                continue
            # A field with nothing in it carries no value to write, so the update above
            # drew nothing for it, and the border and background it shows on the page
            # live in the appearance stream that leaves with the widget. Stamping goes
            # through the same helper pypdf uses for the values, so a filled field and
            # an empty one land the same way.
            writer._add_apstream_object(
                page, stream, f"w{getattr(ref, 'idnum', 0)}",
                float(rect[0].get_object()), float(rect[1].get_object()),
            )
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


def flatten(src: Path, out: Path, password: str | None, engine: str, drop_signatures: bool) -> dict:
    if out.resolve() == src.resolve():
        fail("output path must differ from the input; inputs are never modified in place")
    reader = open_reader(src, password)
    profile = encryption_profile(reader, password)
    guard_signatures(reader, drop_signatures)
    signatures_dropped: list[str] = []
    strip_owner: str | None = None
    engine_source = src
    if populated_signatures(acroform_of(reader)):
        engine_source, signatures_dropped, strip_owner = unsigned_copy(reader, profile, password)
        reader = open_reader(engine_source, password)
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
    pages_before = len(reader.pages)

    staged = stage_output(out)
    fell_back = engine != "pypdf" and shutil.which("qpdf") is None
    if engine == "pypdf" or fell_back:
        notes, owner_password = flatten_pypdf(reader, staged, {k: v for k, v in values.items() if v}, profile, password)
        engine_used = "pypdf"
    else:
        notes = flatten_qpdf(engine_source, staged, password)
        # qpdf copies the encryption dictionary over, /O and all, so both passwords still open it,
        # including the pair a signature strip had to write on the way past.
        owner_password = strip_owner or ("reused" if profile else None)
        engine_used = "qpdf"

    check = open_reader(staged, password)
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
    found: list[str] = []
    missing: list[dict] = []
    text_layer_check, check_reason = "not_applicable", None
    if text_fields:
        text_layer_check, check_reason, words = read_text_boxes(staged, password)
    if text_layer_check == "checked":
        # The widgets are gone from the output, so where each value should have landed comes
        # from the fields as they were before the flatten.
        drawn = drawn_text(before, words, page_geometry(check))
        for f in before:
            values = written_strings(f["value"]) if f["type"] in ("text", "dropdown", "listbox") else []
            if not values:
                continue
            name = f["name"]
            if appearance_shows(f, drawn[name], values):
                found += [shown(name, value) for value in values]
            else:
                missing.append({
                    "field": name,
                    "expected": shown(name, ", ".join(values)),
                    "drawn": shown(name, drawn_summary(drawn[name])),
                })
    warnings: list[str] = []
    if text_layer_check == "skipped":
        warnings.append(
            f"the text layer was not read ({check_reason}), so nothing here has confirmed the baked "
            "values survived onto the page. Render the output and read it before delivering."
        )
    if owner_password == "replaced":
        warnings.append(OWNER_REPLACED)
    if not protection_ok:
        warnings.append(
            f"the output reopens as {describe_protection(after_profile)} and the input is "
            f"{describe_protection(profile)}; the protection did not survive the flatten intact."
        )
    if signatures_dropped:
        warnings.append(SIGNATURES_DROPPED.format(names=signatures_dropped))
    checked = [f["name"] for f in before if f["type"] in ("checkbox", "radio") and f["value"] and str(f["value"]) != "/Off"]
    if engine_used == "pypdf":
        lead = "qpdf is not on PATH, so the pypdf fallback ran. " if fell_back else ""
        detail = f" The selected state of {checked} is what to look at." if checked else ""
        warnings.append(
            f"{lead}pypdf stamps every widget of a group under one XObject name and the last one "
            f"written wins, so a selected checkbox or radio button can come out unselected.{detail} "
            "Render the output and confirm every box and button, or flatten with qpdf."
        )
    # The form is gone and the pages survived: that is a file worth delivering. A value the
    # text layer cannot find still fails the run, but the answer to that one is to render the
    # page and look at it, which needs the file.
    written = (
        (not after_state["has_acroform"])
        and widget_count == 0
        and len(check.pages) == pages_before
        and protection_ok
    )
    ok = written and not missing
    if written:
        os.replace(staged, out)
    else:
        warnings.append(
            f"nothing was written to {out}: the flatten did not verify, so the result was discarded "
            "instead of replacing whatever is already at that path."
        )
    report = {
        "status": "ok" if ok else "verification_failed",
        "operation": "flatten",
        "engine": engine_used,
        "engine_requested": engine,
        "input": str(src),
        "input_sha256": sha256(src),
        "output": str(out),
        "output_written": written,
        **({"output_sha256": sha256(out)} if written else {}),
        "input_encrypted": profile is not None,
        "output_encrypted": check.is_encrypted,
        **({"owner_password": owner_password} if owner_password else {}),
        "fields_before": len(before),
        "fields_after": len(after),
        "signatures_dropped": signatures_dropped,
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



USAGE = {
    "inspect": "forms.py inspect <file.pdf> [--password PW]",
    "fill": (
        "forms.py fill <in.pdf> --values values.json --out <out.pdf> [--password PW] "
        "[--need-appearances] [--truncate] [--complete] [--drop-signatures]"
    ),
    "flatten": (
        "forms.py flatten <in.pdf> --out <out.pdf> [--password PW] [--engine qpdf|pypdf] "
        "[--drop-signatures]"
    ),
}
# Per subcommand, the options it accepts and whether each takes a value. The table is
# per subcommand rather than shared so that a flag belonging to another one is an
# unknown option here: `flatten --truncate` used to parse and then be ignored.
TAKES_VALUE: dict[str, dict[str, bool]] = {
    "inspect": {"--password": True},
    "fill": {
        "--values": True, "--out": True, "--password": True,
        "--need-appearances": False, "--truncate": False, "--complete": False,
        "--drop-signatures": False,
    },
    "flatten": {
        "--out": True, "--password": True, "--engine": True, "--drop-signatures": False,
    },
}


def parse_args(argv: list[str], takes_value: dict[str, bool], usage: str) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token `takes_value` does not name.

    A value is still read by position, so `--out x.pdf` works when x.pdf is also an input
    name, but it is never read off another option: `--out --truncate` used to write the
    filled form to a file called `--truncate`, without the truncation it had been asked
    for, and report that as a success. A repeated option is the same kind of mistake,
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
        fail(f"usage: forms.py {'|'.join(commands)} <file.pdf> [...]")
    command, argv = argv[0], argv[1:]
    positional, opts = parse_args(argv, TAKES_VALUE[command], USAGE[command])
    password = opts.get("--password")
    values = opts.get("--values")
    out = opts.get("--out")
    engine = opts.get("--engine", "qpdf")
    if not positional:
        fail(f"usage: {USAGE[command]}")
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
            "--drop-signatures" in opts,
        )
    else:
        if engine not in ("qpdf", "pypdf"):
            fail("--engine must be qpdf or pypdf")
        flatten(src, Path(out).expanduser().resolve(), password, engine, "--drop-signatures" in opts)


if __name__ == "__main__":
    main(sys.argv[1:])
