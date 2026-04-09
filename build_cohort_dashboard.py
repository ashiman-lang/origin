from __future__ import annotations

import csv
import json
import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import median


INPUT_CSV = Path("/Users/annashiman/Downloads/unified_customers (51).csv")
PRICES_CSV = Path("/Users/annashiman/Downloads/prices (3).csv")
PAYMENTS_CSV = Path("/Users/annashiman/Downloads/unified_payments (34).csv")
SPEND_CSV = Path("/Users/annashiman/Downloads/Spend.csv")
OUTPUT_HTML = Path("/Users/annashiman/Documents/Playground/cohort_dashboard_jan_apr.html")
PAGES_HTML = Path("/Users/annashiman/Documents/Playground/docs/index.html")
NOJEKYLL_FILE = Path("/Users/annashiman/Documents/Playground/docs/.nojekyll")
TARGET_MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MONTHLY_SPEND_OVERRIDE_USD = {
    "2026-01": 54845.58,
}
SPEND_OVERRIDE_TSV = """
2026-04-07	€1.487
2026-04-06	€2.308
2026-04-05	€2.506
2026-04-04	€1.768
2026-04-03	€1.739
2026-04-02	€1.759
2026-04-01	€1.130
2026-03-31	€1.191
2026-03-30	€2.348
2026-03-29	€1.565
2026-03-28	€1.535
2026-03-27	€1.430
2026-03-26	€1.480
2026-03-25	€1.470
2026-03-24	€1.642
2026-03-23	€1.718
2026-03-22	€1.917
2026-03-21	€1.395
2026-03-20	€1.522
2026-03-19	€1.397
2026-03-18	€1.811
2026-03-17	€1.696
2026-03-16	€1.986
2026-03-15	€2.522
2026-03-14	€1.986
2026-03-13	€2.468
2026-03-12	€2.483
2026-03-11	€2.341
2026-03-10	€2.442
2026-03-09	€2.403
2026-03-08	€2.312
2026-03-07	€1.775
2026-03-06	€1.837
2026-03-05	€1.832
2026-03-04	€1.937
2026-03-03	€1.950
2026-03-02	€2.007
2026-03-01	€2.291
2026-02-28	€1.822
2026-02-27	€2.137
2026-02-26	€1.940
2026-02-25	€1.847
2026-02-24	€2.281
2026-02-23	€2.463
2026-02-22	€2.669
2026-02-21	€2.134
2026-02-20	€2.243
2026-02-19	€2.092
2026-02-18	€2.106
2026-02-17	€1.915
2026-02-16	€1.933
2026-02-15	€2.048
2026-02-14	€1.865
2026-02-13	€1.837
2026-02-12	€1.749
2026-02-11	€1.804
2026-02-10	€1.699
2026-02-09	€1.775
2026-02-08	€1.885
2026-02-07	€1.568
2026-02-06	€1.651
2026-02-05	€1.652
2026-02-04	€1.772
2026-02-03	€2.020
2026-02-02	€2.027
2026-02-01	€1.927
2026-01-31	€1.612
2026-01-30	€1.663
2026-01-29	€2.250
2026-01-28	€2.663
2026-01-27	€2.810
2026-01-26	€2.699
2026-01-25	€2.819
2026-01-24	€2.421
2026-01-23	€2.558
2026-01-22	€2.266
2026-01-21	€1.776
2026-01-20	€1.661
2026-01-19	€1.659
2026-01-18	€1.569
2026-01-17	€1.319
2026-01-16	€1.379
2026-01-15	€1.399
2026-01-14	€1.389
2026-01-13	€1.449
2026-01-12	€1.292
2026-01-11	€1.255
2026-01-10	€1.276
2026-01-09	€1.678
2026-01-08	€1.307
2026-01-07	€1.607
2026-01-06	€1.638
2026-01-05	€2.093
2026-01-04	€1.708
2026-01-03	€1.249
2026-01-02	€1.499
2026-01-01	€882
""".strip()
DAILY_SPEND_OVERRIDE_EUR = {
    "2026-01-01": 881.72,
    "2026-01-02": 1499.20,
    "2026-01-03": 1248.77,
    "2026-01-04": 1707.62,
    "2026-01-05": 2093.06,
    "2026-01-06": 1638.25,
    "2026-01-07": 1607.15,
    "2026-01-08": 1307.33,
    "2026-01-09": 1677.67,
    "2026-01-10": 1276.02,
    "2026-01-11": 1255.29,
    "2026-01-12": 1291.55,
    "2026-01-13": 1449.35,
    "2026-01-14": 1389.31,
    "2026-01-15": 1398.84,
    "2026-01-16": 1378.77,
    "2026-01-17": 1318.58,
    "2026-01-18": 1568.52,
    "2026-01-19": 1659.23,
    "2026-01-20": 1661.47,
    "2026-01-21": 1776.00,
    "2026-01-22": 2266.03,
    "2026-01-23": 2558.28,
    "2026-01-24": 2420.84,
    "2026-01-25": 2819.02,
    "2026-01-26": 2699.41,
    "2026-01-27": 2810.19,
    "2026-01-28": 2663.36,
    "2026-01-29": 2249.50,
    "2026-01-30": 1663.32,
    "2026-01-31": 1611.93,
}
CURRENT_DATE = date(2026, 4, 8)
RETENTION_RATE_BENCHMARKS = {
    "monthly": {1: 0.55, 2: 0.38, 3: 0.20, 4: 0.14, 5: 0.07, 6: 0.05, 7: 0.03},
    "quarterly": {3: 0.50, 6: 0.28, 9: 0.18, 12: 0.12},
    "annual": {12: 0.30},
}
PLAN_REVENUE_SCHEDULES = {
    "monthly_19_60": {"family": "monthly", "intro": 19.60, "full_price": 39.99},
    "monthly_15_60": {"family": "monthly", "intro": 15.60, "full_price": 39.99},
    "quarterly_32_00": {"family": "quarterly", "intro": 32.00, "full_price": 79.99},
    "quarterly_23_99": {"family": "quarterly", "intro": 23.99, "full_price": 79.99},
    "annual_58_80": {"family": "annual", "intro": 58.80, "full_price": 119.99},
    "annual_46_80": {"family": "annual", "intro": 46.80, "full_price": 119.99},
}
BLANK_SPEND_PATTERNS = {
    "usd": [
        {"family": "monthly", "intro": 16.80, "renewal": 34.27},
        {"family": "monthly", "intro": 13.37, "renewal": 34.27},
        {"family": "quarterly", "intro": 27.42, "renewal": 57.92},
        {"family": "quarterly", "intro": 20.57, "renewal": 57.92},
        {"family": "annual", "intro": 50.37, "renewal": 0.0},
        {"family": "annual", "intro": 40.09, "renewal": 0.0},
    ],
    "eur": [
        {"family": "monthly", "intro": 10.78, "renewal": 21.93},
        {"family": "monthly", "intro": 7.71, "renewal": 21.42},
    ],
    "blank": [
        {"family": "monthly", "intro": 10.28, "renewal": 21.93},
        {"family": "monthly", "intro": 7.71, "renewal": 21.42},
        {"family": "monthly", "intro": 16.80, "renewal": 34.27},
        {"family": "quarterly", "intro": 27.42, "renewal": 57.92},
    ],
}
HIDDEN_UPSELL_UNITS = {
    "usd": 8.56,
    "gbp": 9.19,
    "chf": 10.82,
    "sek": 8.29,
}
ECB_USD_PER_EUR = {
    "2026-01": 1.1738238095238096,
    "2026-02": 1.182395,
    "2026-03": 1.1558318181818181,
    "2026-04": 1.159825,
}
ECB_CUR_PER_EUR = {
    "2026-01": {
        "AUD": 1.7304190476190475,
        "CAD": 1.6173000000000002,
        "CHF": 0.9271952380952381,
        "DKK": 7.470266666666666,
        "GBP": 0.8682809523809522,
        "MYR": 4.726219047619049,
        "NOK": 11.666966666666667,
        "SEK": 10.681490476190476,
        "SGD": 1.5026571428571427,
        "USD": 1.1738238095238096,
    },
    "2026-02": {
        "AUD": 1.6762899999999998,
        "CAD": 1.6140449999999997,
        "CHF": 0.9140349999999999,
        "DKK": 7.4702,
        "GBP": 0.870315,
        "MYR": 4.626055,
        "NOK": 11.320600000000002,
        "SEK": 10.635085,
        "SGD": 1.49794,
        "USD": 1.182395,
    },
    "2026-03": {
        "AUD": 1.646990909090909,
        "CAD": 1.584795454545455,
        "CHF": 0.9094272727272726,
        "DKK": 7.471686363636365,
        "GBP": 0.8663109090909092,
        "MYR": 4.568736363636363,
        "NOK": 11.165750000000003,
        "SEK": 10.761431818181817,
        "SGD": 1.4789999999999999,
        "USD": 1.1558318181818181,
    },
    "2026-04": {
        "AUD": 1.66785,
        "CAD": 1.611425,
        "CHF": 0.921725,
        "DKK": 7.472525,
        "GBP": 0.871293,
        "MYR": 4.660275,
        "NOK": 11.19325,
        "SEK": 10.9025,
        "SGD": 1.4864,
        "USD": 1.159825,
    },
}


@dataclass
class CohortMetrics:
    month_key: str
    cohort: str
    chart_label: str
    cohort_end_date: date
    spend_usd: float
    customers: int
    paid_customers: int
    customers_with_payment: int
    d0_revenue: float
    m1_revenue: float
    m2_revenue: float
    m3_revenue: float
    m6_revenue: float
    m12_revenue: float
    net_revenue: float
    refunded_volume: float
    dispute_losses: float
    upsell_count: int
    upsell_billing_count: int
    upsell_avg: float
    upsell_revenue: float
    active_customers: int
    past_due_customers: int
    refunded_customers: int
    repeat_customers: int
    three_plus_customers: int
    four_plus_customers: int
    avg_payment_count: float
    median_payment_count: float
    usd_revenue: float
    top_plan: str
    paid_rate: float
    active_rate: float
    repeat_rate: float
    three_plus_rate: float
    four_plus_rate: float
    refunded_rate: float


def parse_amount(value: str) -> float:
    raw = (value or "").strip()
    if not raw:
        return 0.0
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            return float(raw.replace(".", "").replace(",", "."))
        return float(raw.replace(",", ""))
    if "," in raw:
        return float(raw.replace(",", "."))
    return float(raw)


def parse_int(value: str) -> int:
    raw = (value or "").strip()
    return int(raw) if raw else 0


def load_price_catalog() -> dict[str, dict[str, str]]:
    cached = getattr(load_price_catalog, "_cache", None)
    if cached is not None:
        return cached

    with PRICES_CSV.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    catalog = {row["Price ID"]: row for row in rows}
    load_price_catalog._cache = catalog
    return catalog


def load_first_charge_lookup() -> dict[tuple[str, str], float]:
    cached = getattr(load_first_charge_lookup, "_cache", None)
    if cached is not None:
        return cached

    rows = load_rows()
    grouped: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        if parse_int(row.get("Payment Count", "")) != 1:
            continue
        key = (
            (row.get("Plan") or "(blank)").strip() or "(blank)",
            (row.get("Currency") or "").strip() or "blank",
        )
        value = round(parse_amount(row.get("Total Spend", "0")), 2)
        if value > 0:
            grouped[key].append(value)

    lookup: dict[tuple[str, str], float] = {}
    for key, values in grouped.items():
        counts = Counter(values)
        lookup[key] = min(value for value, count in counts.items() if count == max(counts.values()))

    load_first_charge_lookup._cache = lookup
    return lookup


def load_cumulative_charge_lookup() -> dict[tuple[str, str, int], float]:
    cached = getattr(load_cumulative_charge_lookup, "_cache", None)
    if cached is not None:
        return cached

    rows = load_rows()
    grouped: defaultdict[tuple[str, str, int], list[float]] = defaultdict(list)
    for row in rows:
        payment_count = parse_int(row.get("Payment Count", ""))
        if payment_count < 1:
            continue
        key = (
            (row.get("Plan") or "(blank)").strip() or "(blank)",
            (row.get("Currency") or "").strip() or "blank",
            payment_count,
        )
        value = round(parse_amount(row.get("Total Spend", "0")), 2)
        if value > 0:
            grouped[key].append(value)

    lookup: dict[tuple[str, str, int], float] = {}
    for key, values in grouped.items():
        counts = Counter(values)
        lookup[key] = min(value for value, count in counts.items() if count == max(counts.values()))

    load_cumulative_charge_lookup._cache = lookup
    return lookup


def is_core_subscription_plan(plan_id: str) -> bool:
    if not plan_id:
        return True

    meta = load_price_catalog().get(plan_id)
    if not meta:
        return True

    name = (meta.get("Product Name") or "").lower()
    return "subscription" in name and "add-on" not in name and "upgrade" not in name and "one-time" not in name


def plan_is_monthly(plan_id: str) -> bool:
    if not plan_id:
        return False
    meta = load_price_catalog().get(plan_id)
    if not meta:
        return False
    return (meta.get("Interval") or "") == "month" and (meta.get("Interval Count") or "") == "1"


def plan_is_quarterly(plan_id: str) -> bool:
    if not plan_id:
        return False
    meta = load_price_catalog().get(plan_id)
    if not meta:
        return False
    return (meta.get("Interval") or "") == "month" and (meta.get("Interval Count") or "") == "3"


def plan_is_annual(plan_id: str) -> bool:
    if not plan_id:
        return False
    meta = load_price_catalog().get(plan_id)
    if not meta:
        return False
    return (meta.get("Interval") or "") == "year" and (meta.get("Interval Count") or "") == "1"


def add_months(d: date, months: int) -> date:
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def cohort_month_end(month_key: str) -> date:
    year, month = map(int, month_key.split("-"))
    return date(year, month, calendar.monthrange(year, month)[1])


def milestone_is_closed_from_end(cohort_end: date, milestone_months: int) -> bool:
    return CURRENT_DATE >= add_months(cohort_end, milestone_months)


def classify_plan_family(plan_id: str, first_charge: float, currency: str) -> str | None:
    if plan_is_monthly(plan_id):
        return "monthly"
    if plan_is_quarterly(plan_id):
        return "quarterly"
    if plan_is_annual(plan_id):
        return "annual"

    code = (currency or "").strip().lower()
    if code in {"usd", "blank", ""}:
        if first_charge <= 19:
            return "monthly"
        if first_charge <= 35:
            return "quarterly"
        if first_charge <= 65:
            return "annual"
    if code == "eur":
        if first_charge <= 24:
            return "monthly"
        if first_charge <= 40:
            return "quarterly"
        if first_charge <= 65:
            return "annual"
    if code == "gbp":
        if first_charge <= 18:
            return "monthly"
        if first_charge <= 30:
            return "quarterly"
        if first_charge <= 50:
            return "annual"
    if code in {"aud", "cad", "sgd", "myr", "nok", "sek", "chf", "dkk"}:
        if first_charge <= 22:
            return "monthly"
        if first_charge <= 40:
            return "quarterly"
        if first_charge <= 70:
            return "annual"
    return None


def plan_full_price_usd(plan_id: str, month_key: str) -> float | None:
    if not plan_id:
        return None
    meta = load_price_catalog().get(plan_id)
    if not meta:
        return None
    if not is_core_subscription_plan(plan_id):
        return None
    return convert_to_usd(
        parse_amount(meta.get("Amount", "0")),
        meta.get("Currency", ""),
        month_key,
    )


def plan_full_price_eur(plan_id: str, month_key: str) -> float | None:
    if not plan_id:
        return None
    meta = load_price_catalog().get(plan_id)
    if not meta:
        return None
    if not is_core_subscription_plan(plan_id):
        return None
    return convert_to_eur(
        parse_amount(meta.get("Amount", "0")),
        meta.get("Currency", ""),
        month_key,
    )


def load_subscription_creation_amounts() -> dict[str, float]:
    cached = getattr(load_subscription_creation_amounts, "_cache", None)
    if cached is not None:
        return cached

    first_amounts: dict[str, tuple[date, float]] = {}
    for payment in load_payments_rows():
        if payment.get("Status") != "Paid":
            continue
        customer_id = (payment.get("Customer ID") or "").strip()
        if not customer_id:
            continue
        description = (payment.get("Description") or "").strip()
        if description != "Subscription creation":
            continue
        created_raw = (payment.get("Created date (UTC)") or "")[:10]
        if not created_raw:
            continue
        payment_date = datetime.strptime(created_raw, "%Y-%m-%d").date()
        amount_eur = parse_amount(payment.get("Converted Amount", "0")) - parse_amount(payment.get("Converted Amount Refunded", "0"))
        if amount_eur <= 0:
            continue
        existing = first_amounts.get(customer_id)
        if existing is None or payment_date < existing[0]:
            first_amounts[customer_id] = (payment_date, amount_eur)

    result = {customer_id: amount for customer_id, (_, amount) in first_amounts.items()}
    load_subscription_creation_amounts._cache = result
    return result


def infer_blank_usd_projection(total_spend: float) -> tuple[str, float] | None:
    candidates = [
        ("monthly", 34.27, 16.80, 34.27),
        ("monthly", 34.27, 13.37, 34.27),
        ("quarterly", 57.92, 27.42, 57.92),
        ("quarterly", 57.92, 20.57, 57.92),
        ("annual", 102.85, 50.37, 0.0),
        ("annual", 102.85, 40.09, 0.0),
    ]
    best: tuple[float, str, float] | None = None
    for family, full_price, intro, renewal in candidates:
        max_renewals = 3 if family == "monthly" else 1
        for renewals in range(max_renewals + 1):
            for upsells in range(3):
                estimate = intro + renewal * renewals + 8.56 * upsells
                gap = abs(total_spend - estimate)
                if best is None or gap < best[0]:
                    best = (gap, family, full_price)
    if best and best[0] <= 0.75:
        return best[1], best[2]
    return None


def infer_blank_spend_pattern(total_spend: float, currency: str) -> dict[str, float] | None:
    code = ((currency or "").strip() or "blank").lower()
    patterns = BLANK_SPEND_PATTERNS.get(code, [])
    if not patterns:
        return None

    best: tuple[float, dict[str, float]] | None = None
    for pattern in patterns:
        family = pattern["family"]
        intro = pattern["intro"]
        renewal = pattern["renewal"]
        max_count = 4 if family == "monthly" else 2
        for count in range(1, max_count + 1):
            estimate = intro + renewal * max(0, count - 1)
            gap = abs(total_spend - estimate)
            if best is None or gap < best[0]:
                best = (gap, {**pattern, "count": count, "estimate": estimate})

    if best and best[0] <= 1.25:
        return best[1]
    return None


def cumulative_from_blank_pattern(pattern: dict[str, float], billing_count: int) -> float:
    family = pattern["family"]
    intro = pattern["intro"]
    renewal = pattern["renewal"]
    if family == "monthly":
        capped = min(max(billing_count, 1), 4)
        return intro + renewal * max(0, capped - 1)
    if family == "quarterly":
        capped = min(max(billing_count, 1), 2)
        return intro + renewal * max(0, capped - 1)
    return intro


def projection_spec_for_row(row: dict[str, str], month_key: str, first_subscription_amount_eur: float | None = None) -> tuple[str, float] | None:
    plan_id = (row.get("Plan") or "").strip()
    currency = ((row.get("Currency") or "").strip() or "").lower()
    total_spend = parse_amount(row.get("Total Spend", "0"))

    if plan_id:
        family = classify_plan_family(plan_id, 0.0, currency)
        full_price_eur = plan_full_price_eur(plan_id, month_key)
        if family and full_price_eur:
            return family, full_price_eur
        return None

    if currency == "usd":
        return infer_blank_usd_projection(total_spend)

    return None


def projected_family_revenue(family: str, full_price_eur: float, milestone_months: int) -> float:
    return sum(
        full_price_eur * retention
        for month_number, retention in RETENTION_RATE_BENCHMARKS[family].items()
        if month_number <= milestone_months
    )


def required_payment_count(family: str, month_number: int) -> int:
    if family == "monthly":
        return month_number + 1
    if family == "quarterly":
        return month_number // 3 + 1
    if family == "annual":
        return 2
    return 1


def projected_benchmark_revenue(
    family: str,
    full_price_eur: float,
    milestone_months: int,
) -> float:
    return sum(
        full_price_eur * retention
        for month_number, retention in RETENTION_RATE_BENCHMARKS[family].items()
        if month_number <= milestone_months
    )


def format_month(month_key: str) -> str:
    year, month = month_key.split("-")
    names = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }
    return f"{names[month]} {year}"


def format_week_label(week_start: date, week_end: date) -> str:
    iso = week_start.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def format_week_chart_label(week_start: date) -> str:
    anchor = date(2026, 1, 1)
    week_number = ((week_start - anchor).days // 7) + 1
    return f"W{week_number:02d}"


def format_short_date(d: date) -> str:
    return d.strftime("%b %d").replace(" 0", " ")


def anchored_week_range(d: date) -> tuple[date, date]:
    anchor = date(2026, 1, 1)
    delta_days = (d - anchor).days
    week_index = max(0, delta_days // 7)
    week_start = anchor.fromordinal(anchor.toordinal() + week_index * 7)
    week_end = week_start.fromordinal(week_start.toordinal() + 6)
    return week_start, week_end


def load_spend_rows() -> list[dict[str, str]]:
    cached = getattr(load_spend_rows, "_cache", None)
    if cached is not None:
        return cached

    with SPEND_CSV.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    load_spend_rows._cache = rows
    return rows


def parse_compact_eur_amount(value: str) -> float:
    raw = (value or "").strip().replace("€", "").replace(" ", "")
    if not raw:
        return 0.0
    if "." in raw:
        whole, frac = raw.split(".", 1)
        if len(frac) == 3:
            return float(whole + frac)
    return parse_amount(raw)


def spend_override_from_tsv() -> dict[str, float]:
    result: dict[str, float] = {}
    for line in SPEND_OVERRIDE_TSV.splitlines():
        if not line.strip():
            continue
        day_raw, amount_raw = line.split("\t", 1)
        result[day_raw.strip()] = parse_compact_eur_amount(amount_raw)
    return result


def spend_maps_usd() -> tuple[dict[str, float], dict[str, float]]:
    cached = getattr(spend_maps_usd, "_cache", None)
    if cached is not None:
        return cached

    monthly: defaultdict[str, float] = defaultdict(float)
    weekly: defaultdict[str, float] = defaultdict(float)
    daily_eur: dict[str, float] = {}
    for row in load_spend_rows():
        day_raw = (row.get("Day") or "").strip()
        if not day_raw:
            continue
        daily_eur[day_raw] = parse_amount(row.get("Amount spent (EUR)", "0"))

    daily_eur.update(DAILY_SPEND_OVERRIDE_EUR)
    daily_eur.update(spend_override_from_tsv())

    for day_raw, amount_eur in daily_eur.items():
        day = datetime.strptime(day_raw, "%Y-%m-%d").date()
        month_key = day.strftime("%Y-%m")
        if month_key not in TARGET_MONTHS:
            continue
        monthly[month_key] += amount_eur
        week_start, week_end = anchored_week_range(day)
        week_key = format_week_label(week_start, week_end)
        weekly[week_key] += amount_eur

    result = (dict(monthly), dict(weekly))
    for month_key, amount in MONTHLY_SPEND_OVERRIDE_USD.items():
        result[0][month_key] = amount
    spend_maps_usd._cache = result
    return result


def format_currency_bucket(currency: str, amount: float) -> str:
    rounded = round(amount)
    symbol_map = {
        "USD": "$",
        "EUR": "€",
        "GBP": "£",
        "AUD": "A$",
        "CAD": "C$",
        "SEK": "SEK ",
        "NOK": "NOK ",
        "CHF": "CHF ",
        "DKK": "DKK ",
        "SGD": "S$",
        "MYR": "MYR ",
        "UNKNOWN": "",
    }
    prefix = symbol_map.get(currency, f"{currency} ")
    if currency == "UNKNOWN":
        return f"Unknown {rounded:,.0f}"
    return f"{prefix}{rounded:,.0f}"


def convert_to_usd(amount: float, currency: str, month_key: str) -> float:
    code = (currency or "UNKNOWN").upper()
    if amount == 0:
        return 0.0
    if code in {"USD", "UNKNOWN"}:
        return amount
    usd_per_eur = ECB_USD_PER_EUR[month_key]
    if code == "EUR":
        return amount * usd_per_eur
    cur_per_eur = ECB_CUR_PER_EUR[month_key].get(code)
    if not cur_per_eur:
        return 0.0
    return amount / cur_per_eur * usd_per_eur


def convert_to_eur(amount: float, currency: str, month_key: str) -> float:
    code = (currency or "UNKNOWN").upper()
    if amount == 0:
        return 0.0
    if code in {"EUR", "UNKNOWN"}:
        return amount
    cur_per_eur = ECB_CUR_PER_EUR[month_key].get(code)
    if not cur_per_eur:
        return 0.0
    return amount / cur_per_eur


def summarize_revenue_usd(rows: list[dict[str, str]]) -> float:
    total = 0.0
    for row in rows:
        total += parse_amount(row.get("Total Spend", "0"))
    return total


def summarize_net_revenue_usd(rows: list[dict[str, str]], month_key: str) -> float:
    total = 0.0
    for row in rows:
        gross = parse_amount(row.get("Total Spend", "0"))
        refunds = parse_amount(row.get("Refunded Volume", "0"))
        dispute_losses = parse_amount(row.get("Dispute Losses", "0"))
        total += convert_to_usd(gross - refunds - dispute_losses, row.get("Currency", ""), month_key)
    return total


def cohort_dispute_losses(rows: list[dict[str, str]]) -> float:
    return sum(parse_amount(row.get("Dispute Losses", "0")) for row in rows)


def cohort_refunded_volume(rows: list[dict[str, str]]) -> float:
    return sum(parse_amount(row.get("Refunded Volume", "0")) for row in rows)


def net_actual_revenue_through_rows(rows: list[dict[str, str]], cutoff_date: date) -> float:
    return actual_revenue_through_rows(rows, cutoff_date) - cohort_dispute_losses(rows)


def format_usd(amount: float) -> str:
    return f"€{amount:,.0f}"


def format_cpa(amount: float) -> str:
    return f"€{amount:,.0f}"


def format_money_2(amount: float) -> str:
    return "—" if amount == 0 else f"€{amount:,.2f}"


def describe_top_plan(rows: list[dict[str, str]]) -> str:
    plans = Counter((row.get("Plan") or "(blank)").strip() or "(blank)" for row in rows)
    top_plan, top_count = plans.most_common(1)[0]
    if top_plan == "(blank)":
        return f"Blank plan ids ({top_count})"
    return f"{top_plan} ({top_count})"


def build_metrics(
    rows: list[dict[str, str]],
    cohort_key: str,
    cohort_label: str,
    chart_label: str,
    cohort_end_date: date,
    spend_usd: float,
) -> CohortMetrics:
    payment_counts = [parse_int(row.get("Payment Count", "")) for row in rows]
    spends = [parse_amount(row.get("Total Spend", "0")) for row in rows]
    customers = len(rows)

    paid_customers = sum(spend > 0 for spend in spends)
    customers_with_payment = sum(count >= 1 for count in payment_counts)
    first_charge_lookup = load_first_charge_lookup()
    cumulative_charge_lookup = load_cumulative_charge_lookup()
    subscription_creation_amounts = load_subscription_creation_amounts()
    m1_subscription_counts = subscription_billing_counts_through(rows, add_months(cohort_end_date, 1))
    m2_subscription_counts = subscription_billing_counts_through(rows, add_months(cohort_end_date, 2))
    m3_subscription_counts = subscription_billing_counts_through(rows, add_months(cohort_end_date, 3))
    d0_revenue_base = 0.0
    m1_revenue_actual = 0.0
    m2_revenue_actual = 0.0
    m3_revenue_actual = 0.0
    m6_revenue_actual = 0.0
    m12_revenue_actual = 0.0
    projected = {"m1": 0.0, "m2": 0.0, "m3": 0.0, "m6": 0.0, "m12": 0.0}
    prepared_rows = []
    for row in rows:
        payment_count = parse_int(row.get("Payment Count", ""))
        if payment_count < 1:
            continue
        key = (
            (row.get("Plan") or "(blank)").strip() or "(blank)",
            (row.get("Currency") or "").strip() or "blank",
        )
        plan_id = (row.get("Plan") or "").strip()
        customer_id = (row.get("id") or "").strip()
        first_charge = first_charge_lookup.get(key)
        if first_charge is None:
            first_charge = parse_amount(row.get("Total Spend", "0")) if parse_amount(row.get("Total Spend", "0")) > 0 else 0.0
        created_date = datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        age_days = (CURRENT_DATE - created_date).days
        fx_month = (row.get("Created (UTC)") or "")[:7]
        first_subscription_amount_eur = subscription_creation_amounts.get(customer_id)
        projection_spec = projection_spec_for_row(row, fx_month, first_subscription_amount_eur)
        blank_pattern = None
        if not plan_id:
            blank_pattern = infer_blank_spend_pattern(parse_amount(row.get("Total Spend", "0")), key[1])
        first_charge_eur = first_subscription_amount_eur if first_subscription_amount_eur is not None else first_charge
        prepared_rows.append((row, payment_count, key, plan_id, first_charge, first_charge_eur, age_days, projection_spec, fx_month, blank_pattern))

    for row, payment_count, key, plan_id, first_charge, first_charge_eur, age_days, projection_spec, fx_month, blank_pattern in prepared_rows:
        d0_revenue_base += first_charge_eur
        if projection_spec:
            family, full_price_eur = projection_spec
            projected["m1"] += projected_benchmark_revenue(family, full_price_eur, 1)
            projected["m2"] += projected_benchmark_revenue(family, full_price_eur, 2)
            projected["m3"] += projected_benchmark_revenue(family, full_price_eur, 3)
            projected["m6"] += projected_benchmark_revenue(family, full_price_eur, 6)
            projected["m12"] += projected_benchmark_revenue(family, full_price_eur, 12)

        customer_id = (row.get("id") or "").strip()

        target_payment_count = m1_subscription_counts.get(customer_id, 0) or 1
        if target_payment_count == 1 and age_days >= 30 and payment_count >= 2 and plan_is_monthly((row.get("Plan") or "").strip()):
            target_payment_count = 2

        if blank_pattern:
            m1_cumulative = cumulative_from_blank_pattern(blank_pattern, target_payment_count)
        else:
            m1_cumulative = cumulative_charge_lookup.get((key[0], key[1], target_payment_count), first_charge)
        m1_revenue_actual += m1_cumulative

        m2_target_payment_count = m2_subscription_counts.get(customer_id, 0) or 1
        if m2_target_payment_count == 1 and plan_is_monthly(plan_id):
            if age_days >= 60 and payment_count >= 3:
                m2_target_payment_count = 3
            elif age_days >= 30 and payment_count >= 2:
                m2_target_payment_count = 2

        if blank_pattern:
            m2_cumulative = cumulative_from_blank_pattern(blank_pattern, m2_target_payment_count)
        else:
            m2_cumulative = cumulative_charge_lookup.get((key[0], key[1], m2_target_payment_count), first_charge)
        m2_revenue_actual += m2_cumulative

        m3_target_payment_count = m3_subscription_counts.get(customer_id, 0) or 1
        if m3_target_payment_count == 1:
            if plan_is_monthly(plan_id):
                if age_days >= 90 and payment_count >= 4:
                    m3_target_payment_count = 4
                elif age_days >= 60 and payment_count >= 3:
                    m3_target_payment_count = 3
                elif age_days >= 30 and payment_count >= 2:
                    m3_target_payment_count = 2
            elif plan_is_quarterly(plan_id):
                if age_days >= 90 and payment_count >= 2:
                    m3_target_payment_count = 2

        if blank_pattern:
            m3_cumulative = cumulative_from_blank_pattern(blank_pattern, m3_target_payment_count)
        else:
            m3_cumulative = cumulative_charge_lookup.get((key[0], key[1], m3_target_payment_count), first_charge)
        m3_revenue_actual += m3_cumulative

        m6_target_payment_count = 1
        if plan_is_monthly(plan_id):
            if age_days >= 180 and payment_count >= 7:
                m6_target_payment_count = 7
            elif age_days >= 150 and payment_count >= 6:
                m6_target_payment_count = 6
            elif age_days >= 120 and payment_count >= 5:
                m6_target_payment_count = 5
            elif age_days >= 90 and payment_count >= 4:
                m6_target_payment_count = 4
            elif age_days >= 60 and payment_count >= 3:
                m6_target_payment_count = 3
            elif age_days >= 30 and payment_count >= 2:
                m6_target_payment_count = 2
        elif plan_is_quarterly(plan_id):
            if age_days >= 180 and payment_count >= 3:
                m6_target_payment_count = 3
            elif age_days >= 90 and payment_count >= 2:
                m6_target_payment_count = 2

        m6_cumulative = cumulative_charge_lookup.get((key[0], key[1], m6_target_payment_count), first_charge)
        m6_revenue_actual += m6_cumulative

        m12_target_payment_count = 1
        if plan_is_monthly(plan_id):
            for threshold_months in range(12, 0, -1):
                required_count = threshold_months + 1
                if age_days >= threshold_months * 30 and payment_count >= required_count:
                    m12_target_payment_count = required_count
                    break
        elif plan_is_quarterly(plan_id):
            if age_days >= 360 and payment_count >= 5:
                m12_target_payment_count = 5
            elif age_days >= 270 and payment_count >= 4:
                m12_target_payment_count = 4
            elif age_days >= 180 and payment_count >= 3:
                m12_target_payment_count = 3
            elif age_days >= 90 and payment_count >= 2:
                m12_target_payment_count = 2
        elif plan_is_annual(plan_id):
            if age_days >= 360 and payment_count >= 2:
                m12_target_payment_count = 2

        m12_cumulative = cumulative_charge_lookup.get((key[0], key[1], m12_target_payment_count), first_charge)
        m12_revenue_actual += m12_cumulative

    transaction_upsell = compute_transaction_upsells_for_rows(rows)
    upsell_count = int(transaction_upsell["count"])
    upsell_billing_count = int(transaction_upsell["billing_count"])
    upsell_revenue = float(transaction_upsell["revenue"])

    active_customers = sum((row.get("Status") or "") == "active" for row in rows)
    past_due_customers = sum((row.get("Status") or "") == "past_due" for row in rows)
    refunded_customers = sum(parse_amount(row.get("Refunded Volume", "0")) > 0 for row in rows)
    repeat_customers = sum(count >= 2 for count in payment_counts)
    three_plus_customers = sum(count >= 3 for count in payment_counts)
    four_plus_customers = sum(count >= 4 for count in payment_counts)

    d0_revenue = actual_revenue_through_rows(rows, cohort_end_date)
    m1_cutoff = add_months(cohort_end_date, 1)
    m2_cutoff = add_months(cohort_end_date, 2)
    m3_cutoff = add_months(cohort_end_date, 3)
    m1_revenue = net_actual_revenue_through_rows(rows, m1_cutoff) if milestone_is_closed_from_end(cohort_end_date, 1) else (d0_revenue + projected["m1"])
    m2_revenue = net_actual_revenue_through_rows(rows, m2_cutoff) if milestone_is_closed_from_end(cohort_end_date, 2) else (d0_revenue + projected["m2"])
    m3_revenue = net_actual_revenue_through_rows(rows, m3_cutoff) if milestone_is_closed_from_end(cohort_end_date, 3) else (d0_revenue + projected["m3"])
    m6_revenue = d0_revenue + projected["m6"]
    m12_revenue = d0_revenue + projected["m12"]
    refunded_volume = cohort_refunded_volume(rows)
    dispute_losses = cohort_dispute_losses(rows)
    rev_to_date_revenue = actual_revenue_through_rows(rows, CURRENT_DATE) - dispute_losses

    return CohortMetrics(
        month_key=cohort_key,
        cohort=cohort_label,
        chart_label=chart_label,
        cohort_end_date=cohort_end_date,
        spend_usd=spend_usd,
        customers=customers,
        paid_customers=paid_customers,
        customers_with_payment=customers_with_payment,
        d0_revenue=d0_revenue,
        m1_revenue=m1_revenue,
        m2_revenue=m2_revenue,
        m3_revenue=m3_revenue,
        m6_revenue=m6_revenue,
        m12_revenue=m12_revenue,
        net_revenue=rev_to_date_revenue,
        refunded_volume=refunded_volume,
        dispute_losses=dispute_losses,
        upsell_count=upsell_count,
        upsell_billing_count=upsell_billing_count,
        upsell_avg=(upsell_revenue / upsell_count) if upsell_count else 0.0,
        upsell_revenue=upsell_revenue,
        active_customers=active_customers,
        past_due_customers=past_due_customers,
        refunded_customers=refunded_customers,
        repeat_customers=repeat_customers,
        three_plus_customers=three_plus_customers,
        four_plus_customers=four_plus_customers,
        avg_payment_count=(sum(payment_counts) / customers) if customers else 0.0,
        median_payment_count=median(payment_counts) if payment_counts else 0.0,
        usd_revenue=rev_to_date_revenue,
        top_plan=describe_top_plan(rows),
        paid_rate=(paid_customers / customers * 100) if customers else 0.0,
        active_rate=(active_customers / customers * 100) if customers else 0.0,
        repeat_rate=(repeat_customers / customers * 100) if customers else 0.0,
        three_plus_rate=(three_plus_customers / customers * 100) if customers else 0.0,
        four_plus_rate=(four_plus_customers / customers * 100) if customers else 0.0,
        refunded_rate=(refunded_customers / customers * 100) if customers else 0.0,
    )


def load_rows() -> list[dict[str, str]]:
    with INPUT_CSV.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_payments_rows() -> list[dict[str, str]]:
    cached = getattr(load_payments_rows, "_cache", None)
    if cached is not None:
        return cached

    with PAYMENTS_CSV.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    load_payments_rows._cache = rows
    return rows


def compute_transaction_upsells() -> dict[str, dict[str, float]]:
    cached = getattr(compute_transaction_upsells, "_cache", None)
    if cached is not None:
        return cached

    cohort_map: dict[str, tuple[str, str]] = {}
    for row in load_rows():
        month_key = (row.get("Created (UTC)") or "")[:7]
        if month_key in TARGET_MONTHS:
            cohort_map[row["id"]] = (month_key, (row.get("Created (UTC)") or "")[:10])

    paid_payments = [
        row
        for row in load_payments_rows()
        if row.get("Status") == "Paid" and (row.get("Customer ID") or "") in cohort_map
    ]

    by_customer: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for row in paid_payments:
        by_customer[row["Customer ID"]].append(row)

    results = {
        month_key: {"count": 0, "billing_count": 0, "revenue": 0.0}
        for month_key in TARGET_MONTHS
    }

    for customer_id, items in by_customer.items():
        month_key, cohort_day = cohort_map[customer_id]
        first_day_items = [
            row
            for row in sorted(items, key=lambda value: value["Created date (UTC)"])
            if (row.get("Created date (UTC)") or "")[:10] == cohort_day
        ]
        invoice_items = [row for row in first_day_items if row.get("Description") == "Payment for Invoice"]
        if not invoice_items:
            continue

        results[month_key]["count"] += 1
        results[month_key]["billing_count"] += len(invoice_items)
        for row in invoice_items:
            amount = parse_amount(row.get("Amount", "0")) - parse_amount(row.get("Amount Refunded", "0"))
            results[month_key]["revenue"] += convert_to_usd(amount, row.get("Currency", ""), month_key)

    compute_transaction_upsells._cache = results
    return results


def compute_transaction_upsells_for_rows(rows: list[dict[str, str]]) -> dict[str, float]:
    cohort_days = {
        (row.get("id") or "").strip(): (row.get("Created (UTC)") or "")[:10]
        for row in rows
        if (row.get("id") or "").strip()
    }
    if not cohort_days:
        return {"count": 0, "billing_count": 0, "revenue": 0.0}

    items_by_customer: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for payment in load_payments_rows():
        customer_id = (payment.get("Customer ID") or "").strip()
        if customer_id in cohort_days and payment.get("Status") == "Paid":
            items_by_customer[customer_id].append(payment)

    count = 0
    billing_count = 0
    revenue = 0.0
    for customer_id, items in items_by_customer.items():
        cohort_day = cohort_days[customer_id]
        first_day_items = [
            row for row in sorted(items, key=lambda value: value["Created date (UTC)"])
            if (row.get("Created date (UTC)") or "")[:10] == cohort_day
        ]
        invoice_items = [row for row in first_day_items if row.get("Description") == "Payment for Invoice"]
        if not invoice_items:
            continue
        count += 1
        billing_count += len(invoice_items)
        for row in invoice_items:
            amount = parse_amount(row.get("Converted Amount", "0")) - parse_amount(row.get("Converted Amount Refunded", "0"))
            revenue += amount

    return {"count": count, "billing_count": billing_count, "revenue": revenue}


def subscription_billing_counts_through(rows: list[dict[str, str]], cutoff_date: date) -> dict[str, int]:
    cohort_starts = {
        (row.get("id") or "").strip(): datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        for row in rows
        if (row.get("id") or "").strip() and (row.get("Created (UTC)") or "")[:10]
    }
    counts = {customer_id: 0 for customer_id in cohort_starts}
    if not cohort_starts:
        return counts

    for payment in load_payments_rows():
        customer_id = (payment.get("Customer ID") or "").strip()
        if customer_id not in cohort_starts or payment.get("Status") != "Paid":
            continue
        created_raw = (payment.get("Created date (UTC)") or "")[:10]
        if not created_raw:
            continue
        payment_date = datetime.strptime(created_raw, "%Y-%m-%d").date()
        if not (cohort_starts[customer_id] <= payment_date <= cutoff_date):
            continue
        description = (payment.get("Description") or "").strip()
        if description in {"Subscription creation", "Subscription update"}:
            counts[customer_id] += 1

    return counts


def actual_revenue_through_rows(rows: list[dict[str, str]], cutoff_date: date) -> float:
    cohort_starts = {
        (row.get("id") or "").strip(): datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        for row in rows
        if (row.get("id") or "").strip() and (row.get("Created (UTC)") or "")[:10]
    }
    if not cohort_starts:
        return 0.0

    payments_by_customer: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for payment in load_payments_rows():
        customer_id = (payment.get("Customer ID") or "").strip()
        if customer_id in cohort_starts and payment.get("Status") == "Paid":
            payments_by_customer[customer_id].append(payment)

    revenue = 0.0
    customers_with_payment_rows: set[str] = set()
    for customer_id, items in payments_by_customer.items():
        cohort_start = cohort_starts[customer_id]
        for payment in items:
            created_raw = (payment.get("Created date (UTC)") or "")[:10]
            if not created_raw:
                continue
            payment_date = datetime.strptime(created_raw, "%Y-%m-%d").date()
            if cohort_start <= payment_date <= cutoff_date:
                amount = parse_amount(payment.get("Converted Amount", "0")) - parse_amount(payment.get("Converted Amount Refunded", "0"))
                revenue += amount
                customers_with_payment_rows.add(customer_id)

    if len(customers_with_payment_rows) == len(cohort_starts):
        return revenue

    fallback_rows = [row for row in rows if (row.get("id") or "").strip() not in customers_with_payment_rows]
    if not fallback_rows:
        return revenue

    first_charge_lookup = load_first_charge_lookup()
    cumulative_charge_lookup = load_cumulative_charge_lookup()
    fallback_total = 0.0
    for row in fallback_rows:
        payment_count = parse_int(row.get("Payment Count", ""))
        if payment_count < 1:
            continue
        key = (
            (row.get("Plan") or "(blank)").strip() or "(blank)",
            (row.get("Currency") or "").strip() or "blank",
        )
        created_date = datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        age_days = min((cutoff_date - created_date).days, (CURRENT_DATE - created_date).days)
        if age_days < 0:
            continue
        first_charge = first_charge_lookup.get(key)
        if first_charge is None:
            first_charge = parse_amount(row.get("Total Spend", "0")) if parse_amount(row.get("Total Spend", "0")) > 0 else 0.0
        target_payment_count = 1
        plan_id = (row.get("Plan") or "").strip()
        if plan_is_monthly(plan_id):
            if age_days >= 60 and payment_count >= 3:
                target_payment_count = 3
            elif age_days >= 30 and payment_count >= 2:
                target_payment_count = 2
        elif plan_is_quarterly(plan_id):
            if age_days >= 90 and payment_count >= 2:
                target_payment_count = 2
        cumulative = cumulative_charge_lookup.get((key[0], key[1], target_payment_count), first_charge)
        fallback_total += cumulative

    return revenue + fallback_total


def build_view_payload(metrics: list[CohortMetrics]) -> dict[str, object]:
    chart_data = [
        {
            "cohort": item.chart_label,
            "paidRate": round(item.paid_rate, 1),
            "activeRate": round(item.active_rate, 1),
            "repeatRate": round(item.repeat_rate, 1),
            "threePlusRate": round(item.three_plus_rate, 1),
        }
        for item in metrics
    ]

    table_rows = [
        {
            "spendUsd": item.spend_usd,
            "cohort": item.cohort,
            "spend": format_usd(item.spend_usd),
            "newCustomers": item.customers_with_payment,
            "cpa": format_cpa(item.spend_usd / item.customers_with_payment) if item.customers_with_payment else "—",
            "upsell": format_money_2(item.upsell_revenue),
            "upsellCount": item.upsell_count if item.upsell_count else "—",
            "avg": f"€{item.upsell_avg:.2f}" if item.upsell_count else "—",
            "d0Rev": format_usd(item.d0_revenue),
            "revToDate": format_usd(item.net_revenue),
            "refunds": format_usd(item.refunded_volume),
            "disputeLosses": format_usd(item.dispute_losses),
            "d0Roas": f"{(item.d0_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "roasToDate": f"{(item.net_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m1": f"{(item.m1_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m2": f"{(item.m2_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m3": f"{(item.m3_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m6": f"{(item.m6_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m12": f"{(item.m12_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m1Projected": not milestone_is_closed_from_end(item.cohort_end_date, 1),
            "m2Projected": not milestone_is_closed_from_end(item.cohort_end_date, 2),
            "m3Projected": not milestone_is_closed_from_end(item.cohort_end_date, 3),
            "m6Projected": True,
            "m12Projected": True,
        }
        for item in metrics
    ]

    return {"chart": chart_data, "rows": table_rows}


def render_html(monthly_metrics: list[CohortMetrics], weekly_metrics: list[CohortMetrics]) -> str:
    totals = {
        "customers": sum(item.customers for item in monthly_metrics),
        "paid_customers": sum(item.paid_customers for item in monthly_metrics),
        "active_customers": sum(item.active_customers for item in monthly_metrics),
        "repeat_customers": sum(item.repeat_customers for item in monthly_metrics),
    }

    payload = json.dumps(
        {
            "monthly": build_view_payload(monthly_metrics),
            "weekly": build_view_payload(weekly_metrics),
            "totals": totals,
        },
        indent=2,
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>January-April Cohort Dashboard</title>
  <style>
    :root {{
      --bg: #0d1730;
      --panel: #1d2940;
      --panel-alt: #24324b;
      --stroke: rgba(166, 183, 214, 0.18);
      --text: #e7eefc;
      --muted: #9cadca;
      --cyan: #18c7f3;
      --blue: #4f8bff;
      --violet: #a45dff;
      --amber: #ffb02e;
      --green: #40d37d;
      --red: #ff7d8f;
      --shadow: 0 18px 50px rgba(3, 10, 28, 0.45);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(24, 199, 243, 0.12), transparent 30%),
        radial-gradient(circle at top right, rgba(164, 93, 255, 0.12), transparent 30%),
        linear-gradient(180deg, #0f1a35 0%, #0b1428 100%);
      color: var(--text);
      min-height: 100vh;
      padding: 40px 28px 56px;
    }}

    .shell {{
      max-width: 1500px;
      margin: 0 auto;
    }}

    .hero {{
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 20px;
      margin-bottom: 22px;
    }}

    .card {{
      background: linear-gradient(180deg, rgba(41, 56, 83, 0.95), rgba(27, 39, 61, 0.95));
      border: 1px solid var(--stroke);
      border-radius: 22px;
      box-shadow: var(--shadow);
    }}

    .hero-copy {{
      padding: 28px;
    }}

    .eyebrow {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      color: var(--cyan);
      margin-bottom: 10px;
    }}

    h1 {{
      margin: 0 0 10px;
      font-size: clamp(2rem, 4vw, 3.5rem);
      line-height: 1;
    }}

    .subtitle {{
      margin: 0;
      color: var(--muted);
      max-width: 52ch;
      line-height: 1.6;
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      padding: 18px;
    }}

    .summary-pill {{
      background: rgba(12, 20, 38, 0.45);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 18px;
      padding: 18px;
    }}

    .summary-label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}

    .summary-value {{
      font-size: 1.9rem;
      font-weight: 700;
      letter-spacing: -0.04em;
    }}

    .chart-card {{
      padding: 22px 22px 12px;
      margin-bottom: 22px;
    }}

    .tabs {{
      display: inline-flex;
      gap: 10px;
      margin-bottom: 18px;
    }}

    .tab-button {{
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(12, 20, 38, 0.45);
      color: var(--muted);
      border-radius: 999px;
      padding: 10px 16px;
      font-size: 0.95rem;
      font-weight: 600;
      cursor: pointer;
      transition: 0.2s ease;
    }}

    .tab-button.active {{
      color: var(--text);
      background: rgba(79, 139, 255, 0.22);
      border-color: rgba(79, 139, 255, 0.45);
      box-shadow: inset 0 0 0 1px rgba(79, 139, 255, 0.18);
    }}

    .section-head {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      margin-bottom: 18px;
    }}

    .section-title {{
      margin: 0;
      font-size: 1.1rem;
    }}

    .section-note {{
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.94rem;
    }}

    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .legend span {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}

    .legend i {{
      width: 14px;
      height: 14px;
      border-radius: 4px;
      display: inline-block;
    }}

    .chart-wrap {{
      width: 100%;
      overflow-x: auto;
    }}

    svg {{
      width: 100%;
      height: auto;
      display: block;
    }}

    .table-card {{
      padding: 0;
      overflow: hidden;
    }}

    .table-scroll {{
      width: 100%;
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      scrollbar-gutter: stable both-edges;
    }}

    table {{
      width: 100%;
      min-width: 1540px;
      border-collapse: collapse;
    }}

    thead {{
      background: rgba(255, 255, 255, 0.03);
    }}

    th, td {{
      padding: 18px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
      text-align: left;
      vertical-align: top;
    }}

    th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      white-space: nowrap;
    }}

    td {{
      font-size: 0.97rem;
    }}

    th:first-child,
    td:first-child {{
      position: sticky;
      left: 0;
      z-index: 1;
      background: linear-gradient(180deg, rgba(41, 56, 83, 0.98), rgba(27, 39, 61, 0.98));
      min-width: 210px;
      width: 210px;
    }}

    th:first-child {{
      z-index: 2;
    }}

    tbody tr:hover {{
      background: rgba(255, 255, 255, 0.025);
    }}

    .cohort {{
      font-weight: 700;
      font-size: 1.05rem;
      line-height: 1.2;
    }}

    .cohort-code {{
      display: block;
      white-space: nowrap;
    }}

    .cohort-range {{
      display: block;
      margin-top: 4px;
      color: var(--text);
    }}

    .money {{
      max-width: 220px;
      color: #dce7ff;
      line-height: 1.45;
    }}

    .muted {{
      color: var(--muted);
    }}

    .metric-cyan {{ color: var(--cyan); }}
    .metric-blue {{ color: var(--blue); }}
    .metric-violet {{ color: var(--violet); }}
    .metric-amber {{ color: var(--amber); }}
    .metric-green {{ color: var(--green); }}
    .metric-red {{ color: var(--red); }}
    .actual {{
      font-weight: 700;
      color: #dfe8fb;
    }}
    .projected {{
      font-style: italic;
      color: #8fa0bf;
    }}

    .footer-note {{
      margin-top: 18px;
      padding: 18px 22px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.7;
    }}

    @media (max-width: 1024px) {{
      .hero {{
        grid-template-columns: 1fr;
      }}
    }}

    @media (max-width: 720px) {{
      body {{
        padding: 20px 14px 34px;
      }}

      th, td {{
        padding: 14px 12px;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="card hero-copy">
        <div class="eyebrow">Customer Export Dashboard</div>
        <h1>Jan-Apr 2026 Cohorts</h1>
        <p class="subtitle">
          A cohort dashboard built directly from the customer and transactions exports. It compares January,
          February, March, and April acquisition cohorts.
        </p>
      </div>
      <div class="card summary-grid">
        <div class="summary-pill">
          <div class="summary-label">Customers</div>
          <div class="summary-value">{totals["customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Paid Customers</div>
          <div class="summary-value">{totals["paid_customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Active Customers</div>
          <div class="summary-value">{totals["active_customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Repeat Customers</div>
          <div class="summary-value">{totals["repeat_customers"]:,}</div>
        </div>
      </div>
    </section>

    <section class="card chart-card">
      <div class="tabs" role="tablist" aria-label="Cohort views">
        <button class="tab-button active" type="button" data-view="monthly">Monthly Cohorts</button>
        <button class="tab-button" type="button" data-view="weekly">Weekly Cohorts</button>
      </div>
      <div class="section-head">
        <div>
          <h2 class="section-title">Cohort Rate Comparison</h2>
          <p class="section-note" id="section-note">Grouped bar chart for the four key rates available in the export.</p>
        </div>
        <div class="legend">
          <span><i style="background: var(--cyan);"></i>Paid Rate</span>
          <span><i style="background: var(--blue);"></i>Active Rate</span>
          <span><i style="background: var(--violet);"></i>2+ Payments</span>
          <span><i style="background: var(--amber);"></i>3+ Payments</span>
        </div>
      </div>
      <div class="chart-wrap">
        <svg id="chart" viewBox="0 0 1320 380" preserveAspectRatio="xMidYMid meet" aria-label="Cohort chart"></svg>
      </div>
    </section>

    <section class="card table-card">
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Spend</th>
              <th>New</th>
              <th>CPA</th>
              <th>Upsell</th>
              <th>#</th>
              <th>Avg</th>
              <th>D0 Rev</th>
              <th>Net Rev To Date</th>
              <th>Refunds</th>
              <th>Dispute Losses</th>
              <th>D0 ROAS</th>
              <th>ROAS To Date</th>
              <th>M1</th>
              <th>M2</th>
              <th>M3</th>
              <th>M6</th>
              <th>M12</th>
            </tr>
          </thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card footer-note">
      Revenue is shown in EUR and uses Stripe converted transaction amounts. Cohort membership and new-customer counts
      come from the customer export. Closed M1/M2/M3 values use transaction revenue through the milestone cutoff,
      while projected milestones use benchmark retention schedules. Upsell comes from first-day paid invoice
      transactions in the payments export. Spend comes from the daily spend export in EUR, with January overridden
      from your screenshot values where the spend export was incomplete.
    </section>
  </div>

  <script>
    const payload = {payload};
    let activeView = "monthly";

    function renderTable() {{
      const tbody = document.getElementById("tbody");
      const view = payload[activeView];
      const milestoneCell = (value, projected) => `<td class="${{projected ? 'projected' : 'actual'}}">${{value}}</td>`;
      const cohortCell = (label) => {{
        if (activeView !== "weekly") {{
          return `<td class="cohort">${{label}}</td>`;
        }}
        const parts = label.split(" · ");
        if (parts.length !== 2) {{
          return `<td class="cohort">${{label}}</td>`;
        }}
        return `<td class="cohort"><span class="cohort-code">${{parts[0]}}</span><span class="cohort-range">${{parts[1]}}</span></td>`;
      }};
      tbody.innerHTML = view.rows.map((row) => `
        <tr>
          ${{cohortCell(row.cohort)}}
          <td class="money">${{row.spend}}</td>
          <td>${{row.newCustomers.toLocaleString()}}</td>
          <td class="muted">${{row.cpa}}</td>
          <td class="metric-amber">${{row.upsell}}</td>
          <td class="muted">${{row.upsellCount}}</td>
          <td class="muted">${{row.avg}}</td>
          <td>${{row.d0Rev}}</td>
          <td>${{row.revToDate}}</td>
          <td class="metric-red">${{row.refunds}}</td>
          <td class="metric-red">${{row.disputeLosses}}</td>
          <td class="metric-amber">${{row.d0Roas}}</td>
          <td class="metric-blue">${{row.roasToDate}}</td>
          ${{milestoneCell(row.m1, row.m1Projected)}}
          ${{milestoneCell(row.m2, row.m2Projected)}}
          ${{milestoneCell(row.m3, row.m3Projected)}}
          ${{milestoneCell(row.m6, row.m6Projected)}}
          ${{milestoneCell(row.m12, row.m12Projected)}}
        </tr>
      `).join("");
    }}

    function renderChart() {{
      const svg = document.getElementById("chart");
      const view = payload[activeView];
      const width = 1320;
      const height = 380;
      const margin = {{ top: 30, right: 20, bottom: 70, left: 72 }};
      const innerWidth = width - margin.left - margin.right;
      const innerHeight = height - margin.top - margin.bottom;
      const maxValue = 100;
      const groups = view.chart.length;
      const groupWidth = innerWidth / groups;
      const barWidth = Math.min(40, groupWidth / 5.5);
      const series = [
        ["paidRate", "#18c7f3"],
        ["activeRate", "#4f8bff"],
        ["repeatRate", "#a45dff"],
        ["threePlusRate", "#ffb02e"],
      ];

      const y = (value) => margin.top + innerHeight - (value / maxValue) * innerHeight;

      let markup = "";

      [0, 25, 50, 75, 100].forEach((tick) => {{
        const yPos = y(tick);
        markup += `<line x1="${{margin.left}}" y1="${{yPos}}" x2="${{width - margin.right}}" y2="${{yPos}}" stroke="rgba(166,183,214,0.16)" stroke-dasharray="4 6" />`;
        markup += `<text x="${{margin.left - 14}}" y="${{yPos + 4}}" fill="#9cadca" text-anchor="end" font-size="12">${{tick}}%</text>`;
      }});

      view.chart.forEach((row, groupIndex) => {{
        const startX = margin.left + groupIndex * groupWidth + (groupWidth - barWidth * 4 - 18) / 2;
        series.forEach(([key, color], seriesIndex) => {{
          const value = row[key];
          const barHeight = (value / maxValue) * innerHeight;
          const x = startX + seriesIndex * (barWidth + 6);
          const yPos = margin.top + innerHeight - barHeight;
          markup += `<rect x="${{x}}" y="${{yPos}}" width="${{barWidth}}" height="${{barHeight}}" rx="7" fill="${{color}}" opacity="0.92" />`;
        }});
        markup += `<text x="${{margin.left + groupIndex * groupWidth + groupWidth / 2}}" y="${{height - 26}}" fill="#9cadca" text-anchor="middle" font-size="15">${{row.cohort}}</text>`;
      }});

      svg.innerHTML = markup;
    }}

    function setView(nextView) {{
      activeView = nextView;
      document.querySelectorAll(".tab-button").forEach((button) => {{
        button.classList.toggle("active", button.dataset.view === nextView);
      }});
      document.getElementById("section-note").textContent =
        nextView === "monthly"
          ? "Grouped bar chart for the four key rates available in the export."
          : "Weekly cohort comparison using the same rate definitions and milestone table.";
      renderTable();
      renderChart();
    }}

    document.querySelectorAll(".tab-button").forEach((button) => {{
      button.addEventListener("click", () => setView(button.dataset.view));
    }});

    setView("monthly");
  </script>
</body>
</html>
"""


def main() -> None:
    rows = load_rows()
    monthly_grouped: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    weekly_grouped: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    weekly_ranges: dict[str, tuple[date, date]] = {}
    for row in rows:
        created_raw = (row.get("Created (UTC)") or "")
        month_key = created_raw[:7]
        if month_key not in TARGET_MONTHS:
            continue
        monthly_grouped[month_key].append(row)
        created_date = datetime.strptime(created_raw[:10], "%Y-%m-%d").date()
        week_start, week_end = anchored_week_range(created_date)
        week_key = format_week_label(week_start, week_end)
        weekly_grouped[week_key].append(row)
        weekly_ranges[week_key] = (week_start, week_end)

    monthly_spend_map, weekly_spend_map = spend_maps_usd()

    monthly_metrics = [
        build_metrics(
            monthly_grouped[month_key],
            month_key,
            format_month(month_key),
            format_month(month_key).replace(" 2026", ""),
            cohort_month_end(month_key),
            monthly_spend_map.get(month_key, 0.0),
        )
        for month_key in TARGET_MONTHS
    ]

    weekly_keys = sorted(weekly_grouped.keys(), key=lambda key: weekly_ranges[key][0])
    weekly_metrics = [
        build_metrics(
            weekly_grouped[week_key],
            week_key,
            f"{format_short_date(weekly_ranges[week_key][0])}-{format_short_date(weekly_ranges[week_key][1])}",
            format_week_chart_label(weekly_ranges[week_key][0]),
            weekly_ranges[week_key][1],
            weekly_spend_map.get(week_key, 0.0),
        )
        for week_key in weekly_keys
    ]

    html = render_html(monthly_metrics, weekly_metrics)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    PAGES_HTML.parent.mkdir(parents=True, exist_ok=True)
    PAGES_HTML.write_text(html, encoding="utf-8")
    NOJEKYLL_FILE.write_text("", encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML}")
    print(f"Wrote {PAGES_HTML}")


if __name__ == "__main__":
    main()
