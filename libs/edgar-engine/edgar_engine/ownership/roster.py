"""An issuer's insiders and the common shares each last reported holding, from Forms 3, 4 and 5.

A position is one owner's holding of one class of common stock (a lettered class, depositary shares or non-voting
shares), held directly or by one nature of indirect ownership (a trust, a spouse, an LLC). Every Table I line that states an amount owned reports it: a transaction
line's amount after the transaction, a holdings line's amount held. A filing restates only the lines it touches, so
each position takes its amount from the latest report that states it, ordered by report date, then filing date and
accession. An amendment is part of the report it amends (the latest original of the same form, owner, issuer and
period filed before it), and restates only the titles it corrects: a title ("Common Stock", "Restricted Common
Stock") whose amounts it changes or adds to takes its lines from the amendment, since a corrected amount moves every
later balance; the titles it leaves alone, or only repeats (a price corrected), keep the lines of the report. Within
the report each title's latest-dated transaction line is its balance, each holdings line is a holding of its own (two
trusts both described "By Trust" are two lines) except one that repeats that balance, and the position is the sum.

No filing reliably marks an exit: filers tick the "no longer subject to Section 16" box while still in office, so
that mark (owner_rows' not_subject_to_section16) is kept as data and never drops anyone. A roster keeps whoever
filed, and ``since`` drops owners whose latest filing is older. A joint filing's
lines belong to every owner it names, and the same block counts once in ``insider_shares``.
"""
from __future__ import annotations

import re

import pandas as pd

ROSTER_COLUMNS = ("issuer_cik", "owner_cik", "owner_name", "is_director", "is_officer", "is_ten_percent_owner",
                  "is_other", "officer_title", "shares_direct", "as_of_direct", "shares_indirect", "as_of_indirect",
                  "indirect_detail", "latest_transaction", "latest_transaction_date", "latest_filing_date",
                  "latest_accession")
POSITION_COLUMNS = ("issuer_cik", "owner_cik", "security_class", "direct_indirect", "nature", "nature_as_filed",
                    "shares", "as_of", "accession", "lines", "owner_ciks")
INSIDER_DEFINITIONS = ("A", "B", "C")

# Table I titles that are not common stock: preferred, debt, rights, and units that are not yet shares.
_NOT_COMMON = re.compile(
    r"prefer|warrant|debenture|\bnotes?\b|\brights?\b|\boptions?\b|\bbonds?\b|\b(?:stock|share)\s+units?|"
    r"\b[rpd]sus?\b|performance\s+(?:stock\s+|share\s+)?units?|deferred\s+(?:stock\s+|share\s+)?units?|phantom|"
    r"dividend\s+equivalent",
    re.IGNORECASE)
_CLASS = re.compile(r"\b(?:class|series|cl\.?)\s+([a-z0-9]{1,2}(?:-(?:\d+|[a-z0-9]{1,2}))?)\b", re.IGNORECASE)
_SPACE = re.compile(r"\s+")
_FOOTNOTE_REF = re.compile(
    r"\(\s*\d+\s*\)|\[\s*\d+\s*\]|\(?\s*\b(?:see\s+(?:the\s+)?(?:foot)?notes?|footnotes?)\b[\s\d,&/()-]*(?:and[\s\d,()-]*)*",
    re.IGNORECASE)
_BY = re.compile(r"^by\s+")
_PUNCT = re.compile(r"[.,]")
# A depositary title is common stock only when it names American depositary shares or the common or ordinary
# shares it stands for; "Series C Depositary Shares" are a fraction of a preferred share.
_DEPOSITARY = re.compile(r"deposit[ao]ry", re.IGNORECASE)
_DEPOSITARY_COMMON = re.compile(r"american|common|ordinary", re.IGNORECASE)
# Depositary shares are a class of their own, since one stands for some number of the shares it represents. A
# title naming them only in a parenthesis ("Ordinary Shares (represented by ADSs)") is the shares themselves.
_ADS = re.compile(r"deposit[ao]ry\s+(?:shares?|receipts?)|\bad[sr]s?\b", re.IGNORECASE)
_PARENTHESIS = re.compile(r"\([^()]*\)")
# Non-voting shares are a class of their own, held beside the voting ones and often reported apart from them.
_NON_VOTING = re.compile(r"\bnon[\s-]?voting\b", re.IGNORECASE)
# Words that make a title another holding of its class within one report: restricted or unvested and performance
# shares are held beside the plain ones, so a report's line for each is a balance of its own. They are not classes,
# since a filer may rename its whole balance "Restricted Stock" from one report to the next.
_VARIANTS = (("restricted", re.compile(r"\b(?:restricted|unvested)\b", re.IGNORECASE)),
             ("performance", re.compile(r"\bperformance\b", re.IGNORECASE)))
_KEY = ["issuer_cik", "owner_cik", "security_class", "direct_indirect", "nature"]
_HOLDING = ["issuer_cik", "owner_cik", "direct_indirect", "nature"]
_LINE_COLUMNS = ["issuer_cik", "accession", "form", "filed", "period", "owner_ciks", "seq", "security_title",
                 "direct_indirect", "nature_of_ownership"]


def is_common(title: object) -> bool:
    """Whether a Table I security title counts as common stock (or a partnership's common units)."""
    if not isinstance(title, str) or _NOT_COMMON.search(title):
        return False
    return not _DEPOSITARY.search(title) or bool(_DEPOSITARY_COMMON.search(title))


def security_class(title: object) -> str:
    """The class or series a title names ("Class A Common Stock" -> "A", "CL B" -> "B", "Class M-1" -> "M-1",
    "Class A-100" -> "A-100"),
    "ADS" for depositary shares, "NON-VOTING" for non-voting shares of no named class, "" for a single class."""
    if not isinstance(title, str):
        return ""
    if _ADS.search(_PARENTHESIS.sub(" ", title)):
        return "ADS"
    m = _CLASS.search(title)
    return m.group(1).upper() if m else "NON-VOTING" if _NON_VOTING.search(title) else ""


def _variant(title: object) -> str:
    """The words of _VARIANTS a title holds, "" for the plain shares of its class."""
    text, found = title if isinstance(title, str) else "", []
    for name, pattern in _VARIANTS:
        if pattern.search(text):
            found.append(name)
            text = pattern.sub(" ", text)
    return ",".join(found)


def _nature(text: object) -> str:
    """A nature of ownership as a key: case, spacing, commas, periods, footnote references and a leading "by"
    dropped, so "By Samsara BioCapital, L.P. (1)" and "Samsara BioCapital LP" name one holding."""
    if not isinstance(text, str):
        return ""
    s = _PUNCT.sub("", _FOOTNOTE_REF.sub(" ", text.lower()))
    return _BY.sub("", _SPACE.sub(" ", s).strip(" ;:-"))


def _by_owner(lines: pd.DataFrame) -> pd.DataFrame:
    """One row per (line, owner): a joint filing's line belongs to each owner in ``owner_ciks``."""
    ciks = lines.owner_ciks.fillna("").astype(str).str.split(";")
    out = lines.assign(owner_cik=ciks).explode("owner_cik")
    out = out[out.owner_cik.str.isdigit().fillna(False).astype(bool)]
    return out.assign(owner_cik=out.owner_cik.astype("int64"))


def _dated(lines: pd.DataFrame) -> pd.Series:
    """Each line's transaction date, missing when it falls after the filing date, which no transaction can."""
    date, filed = lines.date, lines.filed
    return date.where(date.notna() & (filed.isna() | (date.fillna("") <= filed.fillna(""))))


def _table_i(frame: pd.DataFrame, amount: str, kind: str) -> pd.DataFrame | None:
    """The common-stock lines of Table I that state an amount owned, one row per line and owner; None for none."""
    if frame.empty or amount not in frame:
        return None
    t = frame[~frame.derivative.astype(bool)]
    t = t.loc[t[amount].notna() & t.security_title.map(is_common) & t.issuer_cik.notna(),
              _LINE_COLUMNS + [amount] + (["date"] if kind == "t" else [])]
    t = t.rename(columns={amount: "amount"}).assign(kind=kind, **({} if kind == "t" else {"date": None}))
    return _by_owner(t) if len(t) else None


def _report_dates(rows: pd.DataFrame, lines: pd.DataFrame) -> pd.Series:
    """Each line's report date: its filing's last transaction date in either table, or its period if later, so a
    Form 4 whose period is an old Table II grant still reports today's Table I balance."""
    last = rows.assign(date=_dated(rows)).dropna(subset=["date"]).groupby("accession").date.max() \
        if len(rows) and "date" in rows else pd.Series(dtype=object)
    stated = lines.period.fillna(lines.filed)
    report = lines.accession.map(last).fillna(stated)
    return report.where((report > stated) | stated.isna(), stated)


def _fold_amendments(lines: pd.DataFrame) -> pd.DataFrame:
    """Lines with the report each belongs to (``_rep``, its ``_rep_filed``, "" when its filing date is unknown, and
    its ``_report`` date, the latest of its filings'), keeping for each title of a position the lines of the filing
    that last corrected it. An amendment corrects a title when one of its lines changes the amount of the line it stands for, or has none to stand for:
    a transaction line stands for the one of the same date, a holdings line for the one in the same place, both
    counted in document order. An amendment that only repeats amounts leaves the title to the filing before it."""
    reports = lines.drop_duplicates(["accession", "owner_cik"])
    form = reports.form.fillna("").str.upper()
    reports = reports.assign(_base=form.str.removesuffix("/A"), _period=reports.period.fillna(""),
                             _original=reports.accession.where(~form.str.endswith("/A")))
    reports = reports.sort_values(["filed", "accession"], kind="stable")
    rep = reports._original.groupby([reports.issuer_cik, reports.owner_cik, reports._period, reports._base]).ffill()
    reports = reports.assign(_rep=rep.fillna(reports.accession))
    each = ["_rep", "owner_cik"]
    reports = (reports[["accession", "owner_cik", "_rep"]]
               .merge(reports[["accession", "owner_cik", "filed"]].fillna({"filed": ""})
                      .rename(columns={"accession": "_rep", "filed": "_rep_filed"}), on=each)
               .merge(reports.groupby(each, as_index=False)._report.max(), on=each))
    lines = lines.drop(columns="_report").merge(reports, on=["accession", "owner_cik"])
    title = ["_rep"] + _KEY + ["_variant"]
    place = title + ["kind", "_on"]
    lines = lines.assign(_on=lines._date.where(lines.kind == "t", "")).sort_values(["accession", "seq"], kind="stable")
    lines = lines.assign(_n=lines.groupby(["accession"] + place, dropna=False).cumcount())
    lines = lines.sort_values(["filed", "accession"], kind="stable")
    repeats = (lines.amount - lines.groupby(place + ["_n"], dropna=False).amount.shift()).abs() < 0.005
    corrects = (lines.accession != lines._rep) & ~repeats
    last = lines.accession.where(corrects).groupby([lines[c] for c in title]).transform("last")
    return lines[lines.accession == last.fillna(lines._rep)]


def _latest_reports(lines: pd.DataFrame) -> pd.DataFrame:
    """The lines of each position's latest report: by report date, then the report's filing date and accession (an
    amended report keeps its original's)."""
    order = ["_report", "_rep_filed", "_rep"]
    lines = lines.sort_values(order, kind="stable")
    latest = lines.groupby(_KEY)[order].transform("last")
    return lines[(lines[order] == latest).all(axis=1)]


def _balances(lines: pd.DataFrame) -> pd.DataFrame:
    """The lines a position sums in its report: each title's latest-dated transaction line (the last in document
    order on its date), and every holdings line except one equal to that balance, which only repeats it."""
    key = _KEY + ["_variant"]
    txn = lines[lines.kind == "t"].sort_values(["_date", "seq"], kind="stable").groupby(key, sort=False).tail(1)
    held = lines[lines.kind == "h"].sort_values("seq", kind="stable")
    balance = held[key].merge(txn[key + ["amount"]].rename(columns={"amount": "_balance"}), on=key, how="left")
    repeats = pd.Series(((held.amount.to_numpy() - balance._balance.to_numpy()) ** 2 < 0.005 ** 2), index=held.index)
    first = repeats & (repeats.groupby([held[k] for k in key]).cumsum() == 1)
    return pd.concat([txn, held[~first]])


def _drop_renamed(out: pd.DataFrame, lines: pd.DataFrame) -> pd.DataFrame:
    """Positions less those a renamed title replaced. A title naming no class and one naming a class, for the same
    holding, are one position under two names unless a filing of the issuer names both (*lines* are the common-stock
    lines of Table I, whether or not they state an amount). Only the later is kept (by report, then by filing within
    one, an undated filing last as amendments are linked, so an amendment's title replaces its original's): the
    unqualified title is dropped for any later class, and a class is dropped for a later unqualified title when it
    is the only class that could match. Non-voting shares of no named class pair the same way with the classes the
    issuer's titles call non-voting, and plain titles with the rest; depositary shares are never merged."""
    filed_together = {(issuer, plain, c)
                      for (issuer, _), classes in lines.groupby(["issuer_cik", "accession"]).security_class.agg(set).items()
                      for plain in ("", "NON-VOTING") if plain in classes for c in classes}
    non_voting = {(i, c) for i, c, t in zip(lines.issuer_cik, lines.security_class, lines.security_title)
                  if c not in ("", "ADS", "NON-VOTING") and _NON_VOTING.search(t)}
    stamp = dict(zip(out.index, zip(out.as_of, out._rep_filed, out._rep, out.filed.isna(), out.filed.fillna(""),
                                     out.accession)))
    drop = []
    for (issuer, *_), g in out.groupby(_HOLDING):
        for plain_class in ("", "NON-VOTING"):
            plain = g.index[g.security_class == plain_class]
            if plain.empty:
                continue
            named = [i for i, c in g.security_class.items()
                     if c not in ("", "ADS", "NON-VOTING") and ((issuer, c) in non_voting) == (plain_class != "")
                     and (issuer, plain_class, c) not in filed_together]
            if any(stamp[i] > stamp[plain[0]] for i in named):
                drop.append(plain[0])
            elif len(named) == 1:
                drop.append(named[0])
    return out.drop(index=drop)


def roster_positions(rows: pd.DataFrame, holdings: pd.DataFrame) -> pd.DataFrame:
    """Every owner's common-stock positions (POSITION_COLUMNS), each from the latest report stating it.

    rows are transaction_rows (or read_insider_dataset) and holdings are holding_rows (or read_insider_holdings) of
    any set of filings; either may be a frame with no columns. In steps: the common-stock lines of Table I (a
    transaction date after the filing date is no date), each filing's report date, amendments folded into the
    reports they amend, each position's latest report, the lines it sums there, and renamed titles merged (see the
    module docstring). ``lines`` names the lines a position was read from, so the same block in a joint filing can
    be told apart from equal amounts; ``accession`` is the latest filed of them, ``owner_ciks`` the owners that filing
    names (";"-joined, several for a joint filing) and ``as_of`` the report's date."""
    parts = [p for p in (_table_i(rows, "owned_after", "t"), _table_i(holdings, "owned", "h")) if p is not None]
    if not parts:
        return pd.DataFrame(columns=list(POSITION_COLUMNS))
    lines = pd.concat(parts, ignore_index=True)
    lines = lines.assign(issuer_cik=lines.issuer_cik.astype("int64"),
                         security_class=lines.security_title.map(security_class),
                         _variant=lines.security_title.map(_variant),
                         nature=lines.nature_of_ownership.map(_nature),
                         direct_indirect=lines.direct_indirect.fillna(""),
                         _report=_report_dates(rows, lines))
    lines = lines.assign(_date=_dated(lines).fillna(lines._report))
    chosen = _balances(_latest_reports(_fold_amendments(lines)))
    chosen = chosen.assign(_id=chosen.accession + "/" + chosen.kind + chosen.seq.astype(int).astype(str))
    latest = chosen.sort_values(["filed", "accession"], kind="stable").groupby(_KEY).tail(1)
    latest = latest[_KEY + ["accession", "owner_ciks", "filed"]]
    chosen = chosen.sort_values(["kind", "filed", "accession", "seq"], ascending=[False, True, True, True],
                                kind="stable")
    out = chosen.groupby(_KEY, as_index=False, sort=False).agg(
        shares=("amount", "sum"), as_of=("_report", "first"), _rep_filed=("_rep_filed", "first"),
        _rep=("_rep", "first"), lines=("_id", ";".join), nature_as_filed=("nature_of_ownership", "last"))
    out = out.merge(latest, on=_KEY)
    named = pd.concat([f.loc[~f.derivative.astype(bool) & f.issuer_cik.notna(),
                             ["issuer_cik", "accession", "security_title"]] for f in (rows, holdings) if not f.empty],
                      ignore_index=True)
    named = named[named.security_title.map(is_common)]
    named = named.assign(issuer_cik=named.issuer_cik.astype("int64"),
                         security_class=named.security_title.map(security_class))
    return _drop_renamed(out, named)[list(POSITION_COLUMNS)].sort_values(_KEY, ignore_index=True)


def _latest_filings(owners: pd.DataFrame) -> pd.DataFrame:
    o = owners[owners.owner_cik.notna() & owners.issuer_cik.notna()]
    o = o.assign(owner_cik=o.owner_cik.astype("int64"), issuer_cik=o.issuer_cik.astype("int64"))
    o = o.sort_values(["filed", "accession"], na_position="first")
    return o.groupby(["issuer_cik", "owner_cik"], as_index=False).tail(1)


def _latest_transactions(rows: pd.DataFrame) -> pd.DataFrame:
    """Each owner's latest transaction line: by transaction date (the period for a line with none, or with one
    after the filing date), then filing, then document order with Table I after Table II (an exercise then a sale
    on one day reads as the sale)."""
    cols = ["issuer_cik", "owner_cik", "latest_transaction", "latest_transaction_date"]
    t = rows[rows.code.notna() & rows.issuer_cik.notna()] if "code" in rows else rows
    if t.empty:
        return pd.DataFrame(columns=cols)
    t = _by_owner(t)
    t = t.assign(issuer_cik=t.issuer_cik.astype("int64"), _rank=(~t.derivative.astype(bool)).astype(int),
                 _date=_dated(t).fillna(t.period))
    t = t.sort_values(["_date", "filed", "accession", "_rank", "seq"]).groupby(["issuer_cik", "owner_cik"]).tail(1)
    return t.rename(columns={"code": "latest_transaction", "_date": "latest_transaction_date"})[cols]


def insider_roster(rows: pd.DataFrame, holdings: pd.DataFrame, owners: pd.DataFrame, *,
                   since: str | None = None) -> pd.DataFrame:
    """One row per (issuer, owner) who filed (ROSTER_COLUMNS), per issuer most shares (direct plus indirect)
    first, then by owner CIK.

    owners are owner_rows (or read_insider_owners) of the same filings. An owner's name, relationship and title
    come from their latest filing for the issuer (by filing date, then accession: ``latest_accession``), and
    ``since`` (YYYY-MM-DD) keeps the owners whose latest filing date is on or after it. shares_direct and shares_indirect are None when no report
    states such a position, and as_of_* is the latest report date among the reports they come from;
    indirect_detail lists the indirect positions holding shares as [{nature, shares}], largest first."""
    who = _latest_filings(owners)
    if since is not None:
        who = who[who.filed >= since]
    if who.empty:
        return pd.DataFrame(columns=list(ROSTER_COLUMNS))
    who = who.rename(columns={"filed": "latest_filing_date", "accession": "latest_accession"})
    pos = roster_positions(rows, holdings)
    key = ["issuer_cik", "owner_cik"]
    direct = pos[pos.direct_indirect == "D"].groupby(key).agg(shares_direct=("shares", "sum"),
                                                               as_of_direct=("as_of", "max"))
    ind = pos[pos.direct_indirect == "I"]
    indirect = ind.groupby(key).agg(shares_indirect=("shares", "sum"), as_of_indirect=("as_of", "max"))
    held = ind[ind.shares > 0].sort_values(["shares", "nature"], ascending=[False, True])
    detail = {k: [{"nature": n, "shares": s} for n, s in zip(g.nature_as_filed, g.shares)]
              for k, g in held.groupby(key, sort=False)}
    out = (who.merge(direct, on=key, how="left").merge(indirect, on=key, how="left")
           .merge(_latest_transactions(rows), on=key, how="left"))
    out["indirect_detail"] = [detail.get((i, o), []) for i, o in zip(out.issuer_cik, out.owner_cik)]
    out["_total"] = out.shares_direct.fillna(0) + out.shares_indirect.fillna(0)
    out = out.sort_values(["issuer_cik", "_total", "owner_cik"], ascending=[True, False, True], ignore_index=True)
    return out[list(ROSTER_COLUMNS)]


def insider_shares(roster: pd.DataFrame, positions: pd.DataFrame, definition: str = "B") -> pd.Series:
    """Common shares held by an issuer's insiders, per issuer CIK, under one definition:

    - A: directors' and officers' direct positions;
    - B: A plus their indirect positions;
    - C: B plus the positions of 10% owners who are neither, where equal blocks of one class that different reporters
      state pair up one for one (a fund, its manager and its partner on the board each report the fund's block): such
      a block counts as often as the one reporter stating it most often, less the times B already counts it. A
      reporter is an owner filing alone or every owner a joint filing names together (``owner_ciks``), counted or
      not, so a fund's own position and the line it shares with its manager are one block.

    Only the owners in *roster* count (so its window applies), and a block several owners report in one joint
    filing counts once, as a director's or officer's when one files it. An issuer whose insiders state no position is absent."""
    if definition not in INSIDER_DEFINITIONS:
        raise ValueError(f"definition must be one of {', '.join(INSIDER_DEFINITIONS)}, not {definition!r}")
    officer = roster.is_director.astype(bool) | roster.is_officer.astype(bool)
    insider = officer | roster.is_ten_percent_owner.astype(bool) if definition == "C" else officer
    who = roster.loc[insider, ["issuer_cik", "owner_cik"]].assign(_owner_only=~officer[insider])
    pos = positions.merge(who, on=["issuer_cik", "owner_cik"])
    pos = pos[pos.direct_indirect == "D"] if definition == "A" else pos[pos.direct_indirect.isin(["D", "I"])]
    pos = pos.sort_values("_owner_only", kind="stable").drop_duplicates(["issuer_cik", "lines"])
    if definition == "C":
        block = ["issuer_cik", "security_class", "shares"]
        counted, owners = pos[~pos._owner_only], pos[pos._owner_only]
        owners = owners.assign(_by=owners.owner_ciks.fillna("").astype(str).str.split(";").map(
            lambda ciks: " ".join(str(c) for c in sorted({int(c) for c in ciks if c.strip().isdigit()}))))
        most = (owners.groupby(block + ["_by"]).size().rename("_n").reset_index()
                .sort_values(["_n", "_by"], ascending=[False, True]).drop_duplicates(block))
        owners = owners.merge(most[block + ["_by"]], on=block + ["_by"]).sort_values("lines", kind="stable")
        seen = owners.join(counted.groupby(block).size().rename("_seen"), on=block)._seen.fillna(0)
        pos = pd.concat([counted, owners[owners.groupby(block).cumcount() >= seen]])
    return pos.groupby("issuer_cik").shares.sum()
