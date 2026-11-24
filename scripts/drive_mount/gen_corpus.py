#!/usr/bin/env python3
"""Deterministic Drive-like corpus for the FUSE search benchmark.

Standard library only, Python 3.9+. The needles are spelled from characters
and letters the vocabulary never uses, so their file counts are exact; the
generator also re-counts them in every written file and refuses to finish if
a count drifts from the plan.
"""
import argparse
import json
import os
import random
import sys

ASCII_NEEDLE = "QZX7NEEDLE4K2"      # exactly 25 files
COMMON_WORD = "revenue"             # ~30% of text files; not in the vocabulary
ZH2_NEEDLE = "鲲鹏"                 # exactly 40 files
ZH4_NEEDLE = "饕餮盛宴"             # exactly 15 files
ASCII_FILES, ZH2_FILES, ZH4_FILES = 25, 40, 15
COMMON_SHARE = 0.30

EN_WORDS = """
the of and to in a is that for it as was with be by on not he this are or his
from at which but have an they you were her she there been one all we their has
would when if so no what up out who them some could him into its then two more
time like other than these first new also any my see way may should well over
such because how most even much after year market price stock share bond yield
rate growth margin cash flow capital equity debt asset balance sheet income
earnings profit loss quarter annual fiscal guidance outlook forecast estimate
analyst report research company sector industry segment product customer demand
supply chain cost expense operating gross net basis point percent ratio multiple
valuation discount premium spread volatility risk return portfolio fund index
benchmark allocation exposure position hedge option future contract credit
default inflation policy central bank federal reserve treasury dollar euro yen
currency trade export import tariff economy consumer spending labor wage
employment housing energy oil gas power semiconductor software hardware cloud
data model platform service subscription retail bank insurance health pharma
biotech device auto battery solar wind utility telecom media content advertising
management board director executive chief officer strategy acquisition merger
deal buyback dividend payout coverage leverage liquidity covenant maturity
issuance underwriting pipeline backlog order shipment inventory unit volume mix
pricing promotion channel region china europe america asia japan india brazil
mexico canada germany france britain korea taiwan strong weak stable higher
lower rising falling improving declining robust modest material significant
notable recent prior current next long short term near medium view thesis
catalyst upside downside target base bull bear case scenario assumption driver
headwind tailwind pressure momentum trend cycle recovery slowdown expansion
contraction peak trough normalized adjusted reported organic constant sequential
year-over-year management commentary call transcript filing disclosure note
footnote schedule table figure exhibit appendix summary conclusion
""".split()

# Real Chinese finance terms and common words. None of them uses a character of
# either Chinese needle (checked at start).
ZH_WORDS = """
市盈率 市净率 净利润 营业收入 毛利率 净利率 现金流 自由现金流 资产负债表 利润表
股东权益 估值 分红 回购 市值 增长 季度 年度 同比 环比 预期 风险 投资 银行 利率
通胀 货币政策 财政政策 央行 债券 收益率 基金 股票 指数 板块 行业 公司 管理层
业绩 财报 公告 监管 证监会 交易所 港股 美股 半导体 新能源 消费 医药 地产 周期
成长 价值 龙头 产能 订单 毛利 费用 研发 销售 客户 供应链 出口 进口 汇率 人民币
美元 经济 市场 我们 认为 的 了 和 在 是 有 对 将 也 但 这 可能 继续 显著 提升
下降 维持 评级 买入 增持 中性 减持 目标价 上调 下调 一季度 二季度 三季度 四季度
全年 指引 盈利 亏损 杠杆 流动性 信用 违约 利差 波动 配置 仓位 对冲 期货 期权
宏观 微观 政策 改革 开放 需求 供给 价格 成本 库存 出货 份额 竞争 格局 壁垒
护城河 现金 负债 资本开支 折旧 摊销 税率 每股收益 股息率 回报率 净资产
""".split()

FIRST_NAMES = ["memo", "note", "brief", "draft", "review", "thesis", "call",
               "update", "model", "deck", "minutes", "digest", "recap", "plan"]
TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "JPM", "XOM",
           "BABA", "TSM", "ASML", "SAP", "TCEHY", "NIO", "BYDDY", "PDD", "JD"]
TOP_DIRS = ["research", "clients", "deals", "macro", "sectors", "archive",
            "meetings", "models"]
MID_WORDS = ["2021", "2022", "2023", "2024", "2025"]
LEAF_WORDS = ["q1", "q2", "q3", "q4"]


def check_vocab():
    en = " ".join(EN_WORDS)
    assert COMMON_WORD not in en, "common word leaks from vocabulary"
    assert ASCII_NEEDLE.lower() not in en.lower()
    zh_chars = set("".join(ZH_WORDS))
    for ch in ZH2_NEEDLE + ZH4_NEEDLE:
        assert ch not in zh_chars, "needle character %r is in the vocabulary" % ch
    assert not set(ZH2_NEEDLE) & set(ZH4_NEEDLE)


def en_paragraph(rng, nwords):
    words = rng.choices(EN_WORDS, k=nwords)
    out, i = [], 0
    while i < nwords:
        n = rng.randint(8, 20)
        s = words[i:i + n]
        i += n
        if s:
            s[0] = s[0].capitalize()
            out.append(" ".join(s) + ".")
    return " ".join(out)


def zh_paragraph(rng, nwords):
    words = rng.choices(ZH_WORDS, k=nwords)
    out, i = [], 0
    while i < nwords:
        n = rng.randint(6, 14)
        out.append("".join(words[i:i + n]) + rng.choice(["。", "，", "；"]))
        i += n
    return "".join(out)


def prose(rng, nbytes, zh_share=0.0):
    """Paragraphs until about ``nbytes`` UTF-8 bytes."""
    paras, size = [], 0
    while size < nbytes:
        if zh_share and rng.random() < zh_share:
            p = zh_paragraph(rng, rng.randint(30, 120))
        else:
            p = en_paragraph(rng, rng.randint(40, 180))
        paras.append(p)
        size += len(p.encode()) + 2
    return paras


def plant(rng, paras, sentences):
    for s in sentences:
        paras.insert(rng.randint(0, len(paras)), s)
    return paras


def needle_sentences(flags):
    out = []
    if "ascii" in flags:
        out.append("Reference code %s recorded for follow up." % ASCII_NEEDLE)
    if "common" in flags:
        out.append("Management discussed %s at length." % COMMON_WORD)
    if "zh2" in flags:
        out.append("市场关注%s概念。" % ZH2_NEEDLE)
    if "zh4" in flags:
        out.append("这是一场%s。" % ZH4_NEEDLE)
    return out


def make_md(rng, flags, zh):
    size = rng.randint(2 * 1024, 20 * 1024)
    paras = prose(rng, size, zh_share=0.35 if zh else 0.0)
    paras = plant(rng, paras, needle_sentences(flags))
    title = "# " + " ".join(rng.choices(EN_WORDS, k=5)).title()
    return (title + "\n\n" + "\n\n".join(paras) + "\n").encode()


def make_pdf_md(rng, flags, zh):
    size = rng.randint(10 * 1024, 80 * 1024)
    paras = prose(rng, size, zh_share=0.25 if zh else 0.0)
    paras = plant(rng, paras, needle_sentences(flags))
    out, page = [], 1
    for i, p in enumerate(paras):
        if i % 6 == 0:
            out.append("## Page %d" % page)
            page += 1
        out.append(p)
    return ("\n\n".join(out) + "\n").encode()


def make_pdf(rng):
    size = rng.randint(50 * 1024, 400 * 1024)
    while True:
        data = b"%PDF-1.4\n" + rng.randbytes(size)
        if not any(n.encode() in data for n in (ASCII_NEEDLE, COMMON_WORD, ZH2_NEEDLE, ZH4_NEEDLE)):
            return data


def make_csv(rng, flags):
    target = rng.randint(5 * 1024, 200 * 1024)
    rows = ["date,ticker,open,high,low,close,volume,note"]
    size = len(rows[0])
    extra = needle_sentences(flags)
    while size < target:
        o = rng.uniform(5, 900)
        note = rng.choice(EN_WORDS) if rng.random() < 0.2 else ""
        r = "2024-%02d-%02d,%s,%.2f,%.2f,%.2f,%.2f,%d,%s" % (
            rng.randint(1, 12), rng.randint(1, 28), rng.choice(TICKERS), o,
            o * 1.02, o * 0.98, o * rng.uniform(0.97, 1.03), rng.randint(1000, 9000000), note)
        rows.append(r)
        size += len(r) + 1
    for s in extra:
        rows.insert(rng.randint(1, len(rows)), "2024-06-30,%s,0,0,0,0,0,\"%s\"" % (rng.choice(TICKERS), s))
    return ("\n".join(rows) + "\n").encode()


def make_json(rng, flags):
    target = rng.randint(2 * 1024, 50 * 1024)
    items, size = [], 0
    while size < target:
        it = {"id": len(items), "ticker": rng.choice(TICKERS),
              "summary": en_paragraph(rng, rng.randint(10, 40)),
              "score": round(rng.random(), 4), "tags": rng.choices(EN_WORDS, k=3)}
        items.append(it)
        size += len(it["summary"]) + 80
    doc = {"title": " ".join(rng.choices(EN_WORDS, k=4)), "items": items}
    for k, s in enumerate(needle_sentences(flags)):
        doc["memo_%d" % k] = s
    return (json.dumps(doc, ensure_ascii=False, indent=1) + "\n").encode()


def make_txt(rng, flags):
    paras = prose(rng, rng.randint(1024, 30 * 1024))
    return ("\n\n".join(plant(rng, paras, needle_sentences(flags))) + "\n").encode()


def build_dirs():
    dirs = []
    for t in TOP_DIRS:
        dirs.append(t)
        for m in MID_WORDS:
            dirs.append("%s/%s" % (t, m))
            for leaf in LEAF_WORDS:
                dirs.append("%s/%s/%s" % (t, m, leaf))
    return dirs  # 8 + 40 + 160 = 208


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--files", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--expected", help="JSON of expected counts; default <out>.expected.json, outside the tree")
    a = ap.parse_args()
    check_vocab()
    rng = random.Random(a.seed)
    out = os.path.abspath(a.out)
    expected_path = a.expected or out.rstrip("/") + ".expected.json"
    if os.path.commonpath([out, os.path.abspath(expected_path)]) == out:
        sys.exit("--expected must be outside the corpus tree")
    if os.path.exists(out) and os.listdir(out):
        sys.exit("%s exists and is not empty" % out)

    n = a.files
    n_pdf_pairs = round(n * 0.15 / 2)
    n_csv = round(n * 0.10)
    n_md = round(n * 0.70)
    n_rest = n - n_md - 2 * n_pdf_pairs - n_csv
    n_json, n_txt = n_rest // 2, n_rest - n_rest // 2
    # Units: one per file, except a PDF pair, which shares a folder.
    units = (["md"] * n_md + ["pdf"] * n_pdf_pairs + ["csv"] * n_csv +
             ["json"] * n_json + ["txt"] * n_txt)
    rng.shuffle(units)

    # Text files that can carry needles: one slot per text file.
    text_slots = []  # (unit index, kind)
    for i, u in enumerate(units):
        text_slots.append((i, "pdfmd" if u == "pdf" else u))
    md_like = [s for s in text_slots if s[1] in ("md", "pdfmd")]
    flags = {i: set() for i in range(len(units))}
    for i, _ in rng.sample(text_slots, ASCII_FILES):
        flags[i].add("ascii")
    for i, _ in rng.sample(md_like, ZH2_FILES):
        flags[i].add("zh2")
    for i, _ in rng.sample(md_like, ZH4_FILES):
        flags[i].add("zh4")
    for i, _ in rng.sample(text_slots, round(len(text_slots) * COMMON_SHARE)):
        flags[i].add("common")

    dirs = build_dirs()
    leaves = [d for d in dirs if d.count("/") == 2]
    big = [leaves[3], leaves[97]]
    big_each = 1600 if n >= 10000 else max(1, int(n * 0.08))
    others = [d for d in dirs if d not in big and d.count("/") >= 1]
    placement, filled = [], {d: 0 for d in big}
    for u in units:
        d = None
        for b in big:
            if filled[b] < big_each:
                d = b
                break
        if d is None:
            d = rng.choice(others)
        else:
            filled[d] += 2 if u == "pdf" else 1
        placement.append(d)

    for d in dirs:
        os.makedirs(os.path.join(out, d), exist_ok=True)

    counts = {"ascii": 0, "ascii_md": 0, "common": 0, "zh2": 0, "zh4": 0}
    needles = {"ascii": ASCII_NEEDLE.encode(), "common": COMMON_WORD.encode(),
               "zh2": ZH2_NEEDLE.encode(), "zh4": ZH4_NEEDLE.encode()}
    by_type, total_bytes, total_files, text_files = {}, 0, 0, 0
    per_dir = {}
    mtime = 1700000000

    def write(rel, data, is_text):
        nonlocal total_bytes, total_files, text_files, mtime
        p = os.path.join(out, rel)
        with open(p, "wb") as f:
            f.write(data)
        mtime += 37
        os.utime(p, (mtime, mtime))
        total_bytes += len(data)
        total_files += 1
        per_dir[os.path.dirname(rel)] = per_dir.get(os.path.dirname(rel), 0) + 1
        if is_text:
            text_files += 1
        for k, nd in needles.items():
            if nd in data:
                counts[k] += 1
                if k == "ascii" and rel.endswith(".md"):
                    counts["ascii_md"] += 1

    for i, (u, d) in enumerate(zip(units, placement)):
        stem = "%s-%05d-%s-%s" % (rng.choice(FIRST_NAMES), i, rng.choice(EN_WORDS), rng.choice(EN_WORDS))
        f = flags[i]
        if u == "md":
            data = make_md(rng, f, zh=rng.random() < 0.30 or bool(f & {"zh2", "zh4"}))
            write("%s/%s.md" % (d, stem), data, True)
        elif u == "pdf":
            write("%s/%s.pdf" % (d, stem), make_pdf(rng), False)
            write("%s/%s.pdf.md" % (d, stem), make_pdf_md(rng, f, zh=rng.random() < 0.3), True)
            by_type["pdf"] = by_type.get("pdf", 0) + 1
            u = "pdf.md"
        elif u == "csv":
            write("%s/%s.csv" % (d, stem), make_csv(rng, f), True)
        elif u == "json":
            write("%s/%s.json" % (d, stem), make_json(rng, f), True)
        else:
            write("%s/%s.txt" % (d, stem), make_txt(rng, f), True)
        by_type[u] = by_type.get(u, 0) + 1

    planned_common = round(len(text_slots) * COMMON_SHARE)
    plan = {"ascii": ASCII_FILES, "zh2": ZH2_FILES, "zh4": ZH4_FILES, "common": planned_common}
    for k, v in plan.items():
        if counts[k] != v:
            sys.exit("needle %s: planned %d files, found %d" % (k, v, counts[k]))

    expected = {
        "seed": a.seed, "files": total_files, "dirs": len(dirs) + 1,  # + the root
        "total_bytes": total_bytes, "text_files": text_files, "by_type": by_type,
        "largest_dirs": sorted(per_dir.items(), key=lambda kv: -kv[1])[:3],
        "needles": {
            "ascii": {"token": ASCII_NEEDLE, "files": counts["ascii"], "md_files": counts["ascii_md"]},
            "common": {"token": COMMON_WORD, "files": counts["common"]},
            "zh2": {"token": ZH2_NEEDLE, "files": counts["zh2"]},
            "zh4": {"token": ZH4_NEEDLE, "files": counts["zh4"]},
        },
    }
    with open(expected_path, "w") as fh:
        json.dump(expected, fh, ensure_ascii=False, indent=1)
    print("corpus %s: %d files, %d dirs, %.1f MiB; expected counts in %s" % (
        out, total_files, len(dirs) + 1, total_bytes / 2**20, expected_path))


if __name__ == "__main__":
    main()
