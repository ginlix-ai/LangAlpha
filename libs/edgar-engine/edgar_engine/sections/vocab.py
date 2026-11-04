"""Ordered item lists per form, and the part each item belongs to."""

TENK = ["1", "1A", "1B", "1C", "2", "3", "4", "5", "6", "7", "7A", "8", "9", "9A", "9B", "9C",
        "10", "11", "12", "13", "14", "15", "16"]
PART_OF_10K = {**{i: "I" for i in ["1", "1A", "1B", "1C", "2", "3", "4"]},
               **{i: "II" for i in ["5", "6", "7", "7A", "8", "9", "9A", "9B", "9C"]},
               **{i: "III" for i in ["10", "11", "12", "13", "14"]}, "15": "IV", "16": "IV"}

TENQ_PARTS = {"I": ["1", "2", "3", "4"], "II": ["1", "1A", "2", "3", "4", "5", "6"]}
TENQ = [f"{p}-{i}" for p, items in TENQ_PARTS.items() for i in items]

TWENTYF = ["1", "2", "3", "4", "4A", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
           "16A", "16B", "16C", "16D", "16E", "16F", "16G", "16H", "16I", "16J", "16K", "17", "18", "19"]
PART_OF_20F = {i: ("I" if int("".join(ch for ch in i if ch.isdigit())) <= 12 else
                   "II" if int("".join(ch for ch in i if ch.isdigit())) <= 16 else "III") for i in TWENTYF}

EIGHTK = ["1.01", "1.02", "1.03", "1.04", "1.05", "2.01", "2.02", "2.03", "2.04", "2.05", "2.06", "3.01",
          "3.02", "3.03", "4.01", "4.02", "5.01", "5.02", "5.03", "5.04", "5.05", "5.06", "5.07", "5.08",
          "6.01", "6.02", "6.03", "6.04", "6.05", "7.01", "8.01", "9.01"]
EIGHTK_OLD = [str(i) for i in range(1, 13)]
EIGHTK_NEW_FROM = "2004-08-23"

ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10, "XI": 11,
         "XII": 12, "XIII": 13, "XIV": 14, "XV": 15, "XVI": 16, "XVII": 17, "XVIII": 18, "XIX": 19, "XX": 20}
PART_NUMERAL = {"I": "I", "II": "II", "III": "III", "IV": "IV", "1": "I", "2": "II", "3": "III", "4": "IV",
                "ONE": "I", "TWO": "II", "THREE": "III", "FOUR": "IV"}


def order_for(family: str, filing_date: str | None) -> list[str]:
    if family == "10-K":
        return TENK
    if family == "10-Q":
        return TENQ
    if family == "20-F":
        return TWENTYF
    if family == "8-K":
        return EIGHTK if (filing_date or "9999") >= EIGHTK_NEW_FROM else EIGHTK_OLD
    raise ValueError(family)


def part_of(family: str, item: str) -> str | None:
    if family == "10-K":
        return PART_OF_10K.get(item)
    if family == "20-F":
        return PART_OF_20F.get(item)
    if family == "10-Q":
        return item.split("-")[0]
    return None
