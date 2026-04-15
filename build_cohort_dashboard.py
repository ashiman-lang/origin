from __future__ import annotations

import csv
import json
import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import median


INPUT_CSV = Path("/Users/annashiman/Downloads/unified_customers (55).csv")
PRICES_CSV = Path("/Users/annashiman/Downloads/prices (3).csv")
PAYMENTS_CSV = Path("/Users/annashiman/Downloads/unified_payments (38).csv")
SPEND_CSV = Path("/Users/annashiman/Downloads/Spend (1).csv")
OUTPUT_HTML = Path("/Users/annashiman/Documents/Playground/cohort_dashboard_jan_apr.html")
PAGES_HTML = Path("/Users/annashiman/Documents/Playground/docs/index.html")
NOJEKYLL_FILE = Path("/Users/annashiman/Documents/Playground/docs/.nojekyll")
TARGET_MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
MONTHLY_SPEND_OVERRIDE_USD = {
    "2026-01": 54845.58,
}
SPEND_OVERRIDE_TSV = """
2026-04-14	€839.2
2026-04-13	€1863.5
2026-04-12	€1873.67
2026-04-11	€1666.92
2026-04-10	€1457.08
2026-04-09	€1555.62
2026-04-08	€1403.18
2026-04-07	€1489.57
2026-04-06	€2308.4
2026-04-05	€2505.58
2026-04-04	€1768.46
2026-04-03	€1738.51
2026-04-02	€1759.24
2026-04-01	€1129.9
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
CURRENT_DATE = date(2026, 4, 14)
PROJECTED_NET_REVENUE_FACTOR = 0.85
RETENTION_RATE_BENCHMARKS = {
    "monthly": {1: 0.55, 2: 0.27, 3: 0.12, 4: 0.09, 5: 0.08, 6: 0.08, 7: 0.07, 8: 0.06, 9: 0.05, 10: 0.03, 11: 0.02, 12: 0.01},
    "quarterly": {3: 0.55, 6: 0.30, 9: 0.15, 12: 0.10},
    "annual": {12: 0.35},
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
    m9_revenue: float
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
        if first_charge <= 21:
            return "monthly"
        if first_charge <= 35:
            return "quarterly"
        if first_charge <= 65:
            return "annual"
    if code == "eur":
        if first_charge <= 18.5:
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


def load_subscription_creation_info() -> dict[str, dict[str, object]]:
    cached = getattr(load_subscription_creation_info, "_cache", None)
    if cached is not None:
        return cached

    first_amounts: dict[str, tuple[date, dict[str, object]]] = {}
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
        info = {
            "date": payment_date,
            "amount_eur": amount_eur,
            "amount_raw": parse_amount(payment.get("Amount", "0")) - parse_amount(payment.get("Amount Refunded", "0")),
            "currency": (payment.get("Currency") or "").strip(),
        }
        if existing is None or payment_date < existing[0]:
            first_amounts[customer_id] = (payment_date, info)

    result = {customer_id: info for customer_id, (_, info) in first_amounts.items()}
    load_subscription_creation_info._cache = result
    return result


def load_subscription_creation_amounts() -> dict[str, float]:
    return {
        customer_id: float(info["amount_eur"])
        for customer_id, info in load_subscription_creation_info().items()
    }


def load_first_subscription_update_amounts() -> dict[str, float]:
    cached = getattr(load_first_subscription_update_amounts, "_cache", None)
    if cached is not None:
        return cached

    first_updates: dict[str, tuple[date, float]] = {}
    for payment in load_payments_rows():
        if payment.get("Status") != "Paid":
            continue
        customer_id = (payment.get("Customer ID") or "").strip()
        if not customer_id:
            continue
        description = (payment.get("Description") or "").strip()
        if description != "Subscription update":
            continue
        created_raw = (payment.get("Created date (UTC)") or "")[:10]
        if not created_raw:
            continue
        payment_date = datetime.strptime(created_raw, "%Y-%m-%d").date()
        amount_eur = parse_amount(payment.get("Converted Amount", "0")) - parse_amount(payment.get("Converted Amount Refunded", "0"))
        if amount_eur <= 0:
            continue
        existing = first_updates.get(customer_id)
        if existing is None or payment_date < existing[0]:
            first_updates[customer_id] = (payment_date, amount_eur)

    result = {customer_id: amount for customer_id, (_, amount) in first_updates.items()}
    load_first_subscription_update_amounts._cache = result
    return result


def infer_full_price_from_intro_eur(family: str, intro_amount_eur: float, month_key: str) -> float | None:
    candidates: list[tuple[float, float]] = []
    for spec in PLAN_REVENUE_SCHEDULES.values():
        if spec["family"] != family:
            continue
        intro_eur = convert_to_eur(spec["intro"], "usd", month_key)
        full_eur = convert_to_eur(spec["full_price"], "usd", month_key)
        candidates.append((intro_eur, full_eur))

    if family == "quarterly":
        candidates.append((32.00, 79.99))
        candidates.append((23.99, 79.99))
    elif family == "annual":
        candidates.append((58.80, 119.99))
        candidates.append((46.80, 119.99))

    best: tuple[float, float] | None = None
    for intro_eur, full_eur in candidates:
        gap = abs(intro_amount_eur - intro_eur)
        if best is None or gap < best[0]:
            best = (gap, full_eur)
    if best and best[0] <= 4.5:
        return best[1]
    return None


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


def starter_family_for_row(
    row: dict[str, str],
    month_key: str,
    first_subscription_info: dict[str, object] | None = None,
) -> str | None:
    customer_id = (row.get("id") or "").strip()
    if customer_id.startswith("gcus_"):
        return None

    plan_id = (row.get("Plan") or "").strip()
    if plan_id:
        return classify_plan_family(plan_id, 0.0, (row.get("Currency") or "").strip())

    if first_subscription_info:
        amount_raw = float(first_subscription_info.get("amount_raw", 0.0) or 0.0)
        raw_currency = str(first_subscription_info.get("currency", "") or "")
        if amount_raw > 0 and raw_currency:
            return classify_plan_family("", amount_raw, raw_currency)
        amount_eur = float(first_subscription_info.get("amount_eur", 0.0) or 0.0)
        if amount_eur > 0:
            return classify_plan_family("", amount_eur, "eur")

    currency = ((row.get("Currency") or "").strip() or "").lower()
    total_spend = parse_amount(row.get("Total Spend", "0"))
    if currency == "usd":
        inferred = infer_blank_usd_projection(total_spend)
        if inferred:
            return inferred[0]
    blank_pattern = infer_blank_spend_pattern(total_spend, currency)
    if blank_pattern:
        return str(blank_pattern["family"])
    return None


def projection_price_for_row(
    row: dict[str, str],
    month_key: str,
    family: str | None,
    first_subscription_info: dict[str, object] | None = None,
    first_update_amount_eur: float | None = None,
) -> float | None:
    customer_id = (row.get("id") or "").strip()
    if customer_id.startswith("gcus_"):
        return None

    if not family:
        return None

    plan_id = (row.get("Plan") or "").strip()
    if plan_id:
        return plan_full_price_eur(plan_id, month_key)

    if first_update_amount_eur and first_update_amount_eur > 0:
        return first_update_amount_eur

    if first_subscription_info:
        amount_eur = float(first_subscription_info.get("amount_eur", 0.0) or 0.0)
        if amount_eur > 0:
            inferred = infer_full_price_from_intro_eur(family, amount_eur, month_key)
            if inferred:
                return inferred

    currency = ((row.get("Currency") or "").strip() or "").lower()
    total_spend = parse_amount(row.get("Total Spend", "0"))
    if currency == "usd":
        inferred = infer_blank_usd_projection(total_spend)
        if inferred and inferred[0] == family:
            return inferred[1]

    blank_pattern = infer_blank_spend_pattern(total_spend, currency)
    if blank_pattern and blank_pattern["family"] == family:
        return float(blank_pattern["renewal"]) if blank_pattern["renewal"] else None

    return None


def projected_family_revenue(family: str, full_price_eur: float, milestone_months: int) -> float:
    return sum(
        full_price_eur * retention
        for month_number, retention in RETENTION_RATE_BENCHMARKS[family].items()
        if month_number <= milestone_months
    )


def benchmark_milestones_for_family(family: str, milestone_months: int | None = None) -> list[int]:
    months = sorted(RETENTION_RATE_BENCHMARKS.get(family, {}).keys())
    if milestone_months is None:
        return months
    return [month for month in months if month <= milestone_months]


def benchmark_rate_for_family_month(family: str, month_number: int) -> float:
    return RETENTION_RATE_BENCHMARKS.get(family, {}).get(month_number, 0.0)


def last_closed_benchmark_month(cohort_end_date: date, family: str, target_months: int) -> int:
    closed = [
        month
        for month in benchmark_milestones_for_family(family, target_months)
        if milestone_is_closed_from_end(cohort_end_date, month)
    ]
    return max(closed) if closed else 0


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
    gross_revenue = sum(
        full_price_eur * retention
        for month_number, retention in RETENTION_RATE_BENCHMARKS[family].items()
        if month_number <= milestone_months
    )
    return gross_revenue * PROJECTED_NET_REVENUE_FACTOR


def projected_pop_tail_revenue(
    family: str,
    full_price_eur: float,
    cohort_end_date: date,
    target_months: int,
    actual_retention_rates: dict[str, dict[int, float]],
) -> float:
    milestone_months = benchmark_milestones_for_family(family, target_months)
    if not milestone_months:
        return 0.0

    last_actual_month = last_closed_benchmark_month(cohort_end_date, family, target_months)
    if last_actual_month <= 0:
        return projected_benchmark_revenue(family, full_price_eur, target_months)

    last_actual_rate = actual_retention_rates.get(family, {}).get(last_actual_month)
    if not last_actual_rate or last_actual_rate <= 0:
        return projected_benchmark_revenue(family, full_price_eur, target_months)

    projected_total = 0.0
    previous_month = last_actual_month
    previous_rate = last_actual_rate
    for month_number in milestone_months:
        if month_number <= last_actual_month:
            continue
        previous_benchmark = benchmark_rate_for_family_month(family, previous_month)
        current_benchmark = benchmark_rate_for_family_month(family, month_number)
        if previous_benchmark <= 0 or current_benchmark <= 0:
            previous_month = month_number
            continue
        previous_rate *= current_benchmark / previous_benchmark
        projected_total += full_price_eur * previous_rate
        previous_month = month_number

    return projected_total * PROJECTED_NET_REVENUE_FACTOR


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


def build_weekly_plan_retention_rows(
    weekly_grouped: dict[str, list[dict[str, str]]],
    weekly_ranges: dict[str, tuple[date, date]],
) -> list[dict[str, object]]:
    subscription_creation_info = load_subscription_creation_info()
    rows_out: list[dict[str, object]] = []

    for week_key in sorted(weekly_grouped.keys()):
        cohort_rows = weekly_grouped[week_key]
        starter_ids: list[str] = []
        for row in cohort_rows:
            customer_id = (row.get("id") or "").strip()
            if not customer_id or customer_id.startswith("gcus_"):
                continue
            if parse_int(row.get("Payment Count", "")) < 1:
                continue
            month_key = (row.get("Created (UTC)") or "")[:7]
            family = starter_family_for_row(row, month_key, subscription_creation_info.get(customer_id))
            if family == "monthly":
                starter_ids.append(customer_id)

        starter_ids = sorted(set(starter_ids))
        denominator = len(starter_ids)
        milestone_counts = subscription_billing_counts_through(cohort_rows, CURRENT_DATE)
        m1 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 2)
        m2 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 3)
        m3 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 4)
        week_start, week_end = weekly_ranges[week_key]
        rows_out.append(
            {
                "cohort": f"{format_short_date(week_start)}-{format_short_date(week_end)}",
                "monthly_starters": denominator,
                "m1_count": m1,
                "m1_rate": (m1 / denominator * 100) if denominator else 0.0,
                "m1_closed": milestone_is_closed_from_end(week_end, 1),
                "m2_count": m2,
                "m2_rate": (m2 / denominator * 100) if denominator else 0.0,
                "m2_closed": milestone_is_closed_from_end(week_end, 2),
                "m3_count": m3,
                "m3_rate": (m3 / denominator * 100) if denominator else 0.0,
                "m3_closed": milestone_is_closed_from_end(week_end, 3),
            }
        )
    return rows_out


def build_weekly_quarterly_retention_rows(
    weekly_grouped: dict[str, list[dict[str, str]]],
    weekly_ranges: dict[str, tuple[date, date]],
) -> list[dict[str, object]]:
    subscription_creation_info = load_subscription_creation_info()
    rows_out: list[dict[str, object]] = []

    for week_key in sorted(weekly_grouped.keys()):
        cohort_rows = weekly_grouped[week_key]
        starter_ids: list[str] = []
        for row in cohort_rows:
            customer_id = (row.get("id") or "").strip()
            if not customer_id or customer_id.startswith("gcus_"):
                continue
            if parse_int(row.get("Payment Count", "")) < 1:
                continue
            month_key = (row.get("Created (UTC)") or "")[:7]
            family = starter_family_for_row(row, month_key, subscription_creation_info.get(customer_id))
            if family == "quarterly":
                starter_ids.append(customer_id)

        starter_ids = sorted(set(starter_ids))
        denominator = len(starter_ids)
        milestone_counts = subscription_billing_counts_through(cohort_rows, CURRENT_DATE)
        m3 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 2)
        m6 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 3)
        m9 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 4)
        m12 = sum(1 for customer_id in starter_ids if milestone_counts.get(customer_id, 0) >= 5)
        week_start, week_end = weekly_ranges[week_key]
        rows_out.append(
            {
                "cohort": f"{format_short_date(week_start)}-{format_short_date(week_end)}",
                "quarterly_starters": denominator,
                "m3_count": m3,
                "m3_rate": (m3 / denominator * 100) if denominator else 0.0,
                "m3_closed": milestone_is_closed_from_end(week_end, 3),
                "m6_count": m6,
                "m6_rate": (m6 / denominator * 100) if denominator else 0.0,
                "m6_closed": milestone_is_closed_from_end(week_end, 6),
                "m9_count": m9,
                "m9_rate": (m9 / denominator * 100) if denominator else 0.0,
                "m9_closed": milestone_is_closed_from_end(week_end, 9),
                "m12_count": m12,
                "m12_rate": (m12 / denominator * 100) if denominator else 0.0,
                "m12_closed": milestone_is_closed_from_end(week_end, 12),
            }
        )
    return rows_out


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
    subscription_creation_info = load_subscription_creation_info()
    first_subscription_updates = load_first_subscription_update_amounts()
    projected = {"m1": 0.0, "m2": 0.0, "m3": 0.0, "m6": 0.0, "m9": 0.0, "m12": 0.0}
    prepared_rows = []
    for row in rows:
        if parse_int(row.get("Payment Count", "")) < 1:
            continue
        plan_id = (row.get("Plan") or "").strip()
        customer_id = (row.get("id") or "").strip()
        fx_month = (row.get("Created (UTC)") or "")[:7]
        first_subscription = subscription_creation_info.get(customer_id)
        family = starter_family_for_row(row, fx_month, first_subscription)
        full_price_eur = projection_price_for_row(row, fx_month, family, first_subscription, first_subscription_updates.get(customer_id))
        if family:
            prepared_rows.append((row, customer_id, family, full_price_eur))

    family_customers: defaultdict[str, list[str]] = defaultdict(list)
    for _row, customer_id, family, _full_price_eur in prepared_rows:
        family_customers[family].append(customer_id)

    milestone_counts = {
        month: subscription_billing_counts_through(rows, CURRENT_DATE)
        for month in {1, 2, 3, 6, 9, 12}
    }
    actual_retention_rates: dict[str, dict[int, float]] = defaultdict(dict)
    for family, customer_ids in family_customers.items():
        denominator = len(customer_ids)
        if denominator == 0:
            continue
        for month in benchmark_milestones_for_family(family):
            if not milestone_is_closed_from_end(cohort_end_date, month):
                continue
            required_count = required_payment_count(family, month)
            numerator = sum(1 for customer_id in customer_ids if milestone_counts[month].get(customer_id, 0) >= required_count)
            actual_retention_rates[family][month] = numerator / denominator

    for _row, _customer_id, family, full_price_eur in prepared_rows:
        if not full_price_eur:
            continue
        projected["m1"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 1, actual_retention_rates)
        projected["m2"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 2, actual_retention_rates)
        projected["m3"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 3, actual_retention_rates)
        projected["m6"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 6, actual_retention_rates)
        projected["m9"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 9, actual_retention_rates)
        projected["m12"] += projected_pop_tail_revenue(family, full_price_eur, cohort_end_date, 12, actual_retention_rates)

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
    refunded_volume = cohort_refunded_volume(rows)
    dispute_losses = cohort_dispute_losses(rows)
    m1_cutoff = add_months(cohort_end_date, 1)
    m2_cutoff = add_months(cohort_end_date, 2)
    m3_cutoff = add_months(cohort_end_date, 3)
    def blended_revenue(target_month: int) -> float:
        if milestone_is_closed_from_end(cohort_end_date, target_month):
            return actual_revenue_by_billing_milestone(rows, target_month) - dispute_losses
        closed_points = [month for month in (1, 2, 3, 6, 9, 12) if month < target_month and milestone_is_closed_from_end(cohort_end_date, month)]
        if closed_points:
            last_actual_month = max(closed_points)
            base_revenue = actual_revenue_by_billing_milestone(rows, last_actual_month) - dispute_losses
        else:
            base_revenue = d0_revenue
        return base_revenue + projected[f"m{target_month}"]

    m1_revenue = blended_revenue(1)
    m2_revenue = blended_revenue(2)
    m3_revenue = blended_revenue(3)
    m6_revenue = blended_revenue(6)
    m9_revenue = blended_revenue(9)
    m12_revenue = blended_revenue(12)
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
        m9_revenue=m9_revenue,
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


def subscription_billing_amounts_through(rows: list[dict[str, str]], cutoff_date: date) -> dict[str, list[float]]:
    cohort_starts = {
        (row.get("id") or "").strip(): datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        for row in rows
        if (row.get("id") or "").strip() and (row.get("Created (UTC)") or "")[:10]
    }
    amounts = {customer_id: [] for customer_id in cohort_starts}
    if not cohort_starts:
        return amounts

    grouped: defaultdict[str, list[tuple[date, float]]] = defaultdict(list)
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
        if description not in {"Subscription creation", "Subscription update"}:
            continue
        amount = parse_amount(payment.get("Converted Amount", "0")) - parse_amount(payment.get("Converted Amount Refunded", "0"))
        grouped[customer_id].append((payment_date, amount))

    for customer_id, items in grouped.items():
        items.sort(key=lambda item: item[0])
        amounts[customer_id] = [amount for _payment_date, amount in items]

    return amounts


def first_day_invoice_revenue(rows: list[dict[str, str]], cutoff_date: date) -> dict[str, float]:
    cohort_meta = {
        (row.get("id") or "").strip(): ((row.get("Created (UTC)") or "")[:10], datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date())
        for row in rows
        if (row.get("id") or "").strip() and (row.get("Created (UTC)") or "")[:10]
    }
    revenue = {customer_id: 0.0 for customer_id in cohort_meta}
    if not cohort_meta:
        return revenue

    for payment in load_payments_rows():
        customer_id = (payment.get("Customer ID") or "").strip()
        if customer_id not in cohort_meta or payment.get("Status") != "Paid":
            continue
        created_raw = (payment.get("Created date (UTC)") or "")[:10]
        if not created_raw:
            continue
        payment_date = datetime.strptime(created_raw, "%Y-%m-%d").date()
        cohort_day_raw, cohort_day = cohort_meta[customer_id]
        if not (cohort_day <= payment_date <= cutoff_date):
            continue
        if created_raw != cohort_day_raw:
            continue
        if (payment.get("Description") or "").strip() != "Payment for Invoice":
            continue
        amount = parse_amount(payment.get("Converted Amount", "0")) - parse_amount(payment.get("Converted Amount Refunded", "0"))
        revenue[customer_id] += amount

    return revenue


def actual_revenue_by_billing_milestone(rows: list[dict[str, str]], milestone_month: int) -> float:
    subscription_creation_info = load_subscription_creation_info()
    billing_amounts = subscription_billing_amounts_through(rows, CURRENT_DATE)
    invoice_amounts = first_day_invoice_revenue(rows, CURRENT_DATE)
    total = 0.0
    for row in rows:
        if parse_int(row.get("Payment Count", "")) < 1:
            continue
        customer_id = (row.get("id") or "").strip()
        month_key = (row.get("Created (UTC)") or "")[:7]
        family = starter_family_for_row(row, month_key, subscription_creation_info.get(customer_id))
        if not family:
            continue
        required_count = required_payment_count(family, milestone_month)
        total += invoice_amounts.get(customer_id, 0.0)
        total += sum(billing_amounts.get(customer_id, [])[:required_count])
    return total


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
            "m9": f"{(item.m9_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m12": f"{(item.m12_revenue / item.spend_usd * 100):.1f}%" if item.spend_usd else "—",
            "m1Projected": not milestone_is_closed_from_end(item.cohort_end_date, 1),
            "m2Projected": not milestone_is_closed_from_end(item.cohort_end_date, 2),
            "m3Projected": not milestone_is_closed_from_end(item.cohort_end_date, 3),
            "m6Projected": True,
            "m9Projected": True,
            "m12Projected": True,
        }
        for item in metrics
    ]

    return {"chart": chart_data, "rows": table_rows}


def build_week_prediction_example() -> dict[str, object]:
    week_start = date(2026, 1, 1)
    week_end = date(2026, 1, 7)
    rows = [
        row for row in load_rows()
        if week_start.isoformat() <= (row.get("Created (UTC)") or "")[:10] <= week_end.isoformat()
    ]
    metrics = build_metrics(
        rows,
        week_start.isoformat(),
        "Jan 1-Jan 7",
        "W01",
        week_end,
        spend_maps_usd()[1]["2026-W01"],
    )

    subscription_creation_info = load_subscription_creation_info()
    first_subscription_updates = load_first_subscription_update_amounts()
    prepared: list[tuple[str, str, float | None]] = []
    for row in rows:
        if parse_int(row.get("Payment Count", "")) < 1:
            continue
        customer_id = (row.get("id") or "").strip()
        month_key = (row.get("Created (UTC)") or "")[:7]
        family = starter_family_for_row(row, month_key, subscription_creation_info.get(customer_id))
        full_price_eur = projection_price_for_row(
            row,
            month_key,
            family,
            subscription_creation_info.get(customer_id),
            first_subscription_updates.get(customer_id),
        )
        if family:
            prepared.append((customer_id, family, full_price_eur))

    family_customers: defaultdict[str, list[str]] = defaultdict(list)
    family_full_price: defaultdict[str, float] = defaultdict(float)
    for customer_id, family, full_price_eur in prepared:
        family_customers[family].append(customer_id)
        if full_price_eur:
            family_full_price[family] += full_price_eur

    milestone_counts = {
        month: subscription_billing_counts_through(rows, CURRENT_DATE)
        for month in (1, 2, 3, 6, 9, 12)
    }
    actual_retention_rates: dict[str, dict[int, float]] = defaultdict(dict)
    for family, customer_ids in family_customers.items():
        for month in benchmark_milestones_for_family(family):
            if not milestone_is_closed_from_end(week_end, month):
                continue
            required_count = required_payment_count(family, month)
            numerator = sum(1 for customer_id in customer_ids if milestone_counts[month].get(customer_id, 0) >= required_count)
            actual_retention_rates[family][month] = numerator / len(customer_ids) if customer_ids else 0.0

    monthly_anchor_m3 = actual_retention_rates.get("monthly", {}).get(3, 0.0)
    quarterly_anchor_m3 = actual_retention_rates.get("quarterly", {}).get(3, 0.0)
    monthly_m4 = monthly_anchor_m3 * benchmark_rate_for_family_month("monthly", 4) / benchmark_rate_for_family_month("monthly", 3) if monthly_anchor_m3 else 0.0
    monthly_m5 = monthly_m4 * benchmark_rate_for_family_month("monthly", 5) / benchmark_rate_for_family_month("monthly", 4) if monthly_m4 else 0.0
    monthly_m6 = monthly_m5 * benchmark_rate_for_family_month("monthly", 6) / benchmark_rate_for_family_month("monthly", 5) if monthly_m5 else 0.0
    quarterly_m6 = quarterly_anchor_m3 * benchmark_rate_for_family_month("quarterly", 6) / benchmark_rate_for_family_month("quarterly", 3) if quarterly_anchor_m3 else 0.0

    monthly_m6_tail = sum(
        projected_pop_tail_revenue("monthly", full_price_eur, week_end, 6, actual_retention_rates)
        for _customer_id, family, full_price_eur in prepared
        if family == "monthly" and full_price_eur
    )
    quarterly_m6_tail = sum(
        projected_pop_tail_revenue("quarterly", full_price_eur, week_end, 6, actual_retention_rates)
        for _customer_id, family, full_price_eur in prepared
        if family == "quarterly" and full_price_eur
    )
    annual_m6_tail = sum(
        projected_pop_tail_revenue("annual", full_price_eur, week_end, 6, actual_retention_rates)
        for _customer_id, family, full_price_eur in prepared
        if family == "annual" and full_price_eur
    )

    return {
        "cohort": "Jan 1-Jan 7",
        "spend": metrics.spend_usd,
        "d0": metrics.d0_revenue,
        "m1": metrics.m1_revenue,
        "m2": metrics.m2_revenue,
        "m3": metrics.m3_revenue,
        "m6": metrics.m6_revenue,
        "family_counts": {family: len(customer_ids) for family, customer_ids in family_customers.items()},
        "family_full_price": dict(family_full_price),
        "actual_retention": actual_retention_rates,
        "monthly_m4": monthly_m4,
        "monthly_m5": monthly_m5,
        "monthly_m6": monthly_m6,
        "quarterly_m6": quarterly_m6,
        "monthly_m6_tail": monthly_m6_tail,
        "quarterly_m6_tail": quarterly_m6_tail,
        "annual_m6_tail": annual_m6_tail,
    }


def render_table_rows(rows: list[dict[str, object]], weekly: bool) -> str:
    parts: list[str] = []
    for row in rows:
        cohort = str(row["cohort"])
        if weekly:
            cohort_html = f'<td class="cohort"><span class="cohort-range">{cohort}</span></td>'
        else:
            cohort_html = f'<td class="cohort">{cohort}</td>'

        def milestone_cell(value_key: str, projected_key: str) -> str:
            klass = "projected" if row[projected_key] else "actual"
            return f'<td class="{klass}">{row[value_key]}</td>'

        parts.append(
            f"""
        <tr>
          {cohort_html}
          <td class="money">{row["spend"]}</td>
          <td>{int(row["newCustomers"]):,}</td>
          <td class="muted">{row["cpa"]}</td>
          <td class="metric-amber">{row["upsell"]}</td>
          <td class="muted">{row["upsellCount"]}</td>
          <td class="muted">{row["avg"]}</td>
          <td>{row["d0Rev"]}</td>
          <td>{row["revToDate"]}</td>
          <td class="metric-red">{row["refunds"]}</td>
          <td class="metric-red">{row["disputeLosses"]}</td>
          <td class="metric-amber">{row["d0Roas"]}</td>
          <td class="metric-blue">{row["roasToDate"]}</td>
          {milestone_cell("m1", "m1Projected")}
          {milestone_cell("m2", "m2Projected")}
          {milestone_cell("m3", "m3Projected")}
          {milestone_cell("m6", "m6Projected")}
          {milestone_cell("m9", "m9Projected")}
          {milestone_cell("m12", "m12Projected")}
        </tr>
        """
        )
    return "".join(parts)


def render_chart_markup(chart_rows: list[dict[str, object]]) -> str:
    width = 1320
    height = 380
    margin_top = 30
    margin_right = 20
    margin_bottom = 70
    margin_left = 72
    inner_width = width - margin_left - margin_right
    inner_height = height - margin_top - margin_bottom
    max_value = 100
    groups = max(len(chart_rows), 1)
    group_width = inner_width / groups
    bar_width = min(40, group_width / 5.5)
    series = [
        ("paidRate", "#18c7f3"),
        ("activeRate", "#4f8bff"),
        ("repeatRate", "#a45dff"),
        ("threePlusRate", "#ffb02e"),
    ]

    def y(value: float) -> float:
        return margin_top + inner_height - (value / max_value) * inner_height

    parts: list[str] = []
    for tick in [0, 25, 50, 75, 100]:
        y_pos = y(tick)
        parts.append(
            f'<line x1="{margin_left}" y1="{y_pos}" x2="{width - margin_right}" y2="{y_pos}" stroke="rgba(166,183,214,0.16)" stroke-dasharray="4 6" />'
        )
        parts.append(
            f'<text x="{margin_left - 14}" y="{y_pos + 4}" fill="#9cadca" text-anchor="end" font-size="12">{tick}%</text>'
        )

    for group_index, row in enumerate(chart_rows):
        start_x = margin_left + group_index * group_width + (group_width - bar_width * 4 - 18) / 2
        for series_index, (key, color) in enumerate(series):
            value = float(row[key])
            bar_height = (value / max_value) * inner_height
            x = start_x + series_index * (bar_width + 6)
            y_pos = margin_top + inner_height - bar_height
            parts.append(
                f'<rect x="{x}" y="{y_pos}" width="{bar_width}" height="{bar_height}" rx="7" fill="{color}" opacity="0.92" />'
            )
        parts.append(
            f'<text x="{margin_left + group_index * group_width + group_width / 2}" y="{height - 26}" fill="#9cadca" text-anchor="middle" font-size="15">{row["cohort"]}</text>'
        )

    return "".join(parts)


def render_html(
    monthly_metrics: list[CohortMetrics],
    weekly_metrics: list[CohortMetrics],
    weekly_retention_rows: list[dict[str, object]],
    weekly_quarterly_retention_rows: list[dict[str, object]],
) -> str:
    totals = {
        "customers": sum(item.customers for item in monthly_metrics),
        "paid_customers": sum(item.paid_customers for item in monthly_metrics),
        "active_customers": sum(item.active_customers for item in monthly_metrics),
        "repeat_customers": sum(item.repeat_customers for item in monthly_metrics),
    }
    monthly_spend_map, weekly_spend_map = spend_maps_usd()
    weekly_spend_upload_map = {
        (item.cohort_end_date.fromordinal(item.cohort_end_date.toordinal() - 6)).isoformat(): item.spend_usd
        for item in weekly_metrics
    }
    catalog_payload = {
        price_id: {
            "amount": parse_amount(meta.get("Amount", "0")),
            "currency": meta.get("Currency", ""),
            "interval": meta.get("Interval", ""),
            "interval_count": meta.get("Interval Count", ""),
            "product_name": meta.get("Product Name", ""),
        }
        for price_id, meta in load_price_catalog().items()
    }

    payload = json.dumps(
        {
            "monthly": build_view_payload(monthly_metrics),
            "weekly": build_view_payload(weekly_metrics),
            "weeklyRetention": weekly_retention_rows,
            "quarterlyRetention": weekly_quarterly_retention_rows,
            "totals": totals,
        },
        indent=2,
    )
    initial_view = build_view_payload(monthly_metrics)
    initial_table_rows = render_table_rows(initial_view["rows"], weekly=False)
    initial_chart_markup = render_chart_markup(initial_view["chart"])
    week_example = build_week_prediction_example()
    week_example_markup = f"""
      <details class="forecast-details">
        <summary>Worked Example: {week_example["cohort"]}</summary>
        <div class="forecast-copy">
          <p><strong>Actual closed milestones</strong></p>
          <ul>
            <li>Spend: {format_usd(week_example["spend"])}. D0 actual: {format_usd(week_example["d0"])}, M1 actual: {format_usd(week_example["m1"])}, M2 actual: {format_usd(week_example["m2"])}, M3 actual: {format_usd(week_example["m3"])}.</li>
            <li>Because this weekly cohort is already closed for M3, the displayed M3 is actual, not projected: {format_usd(week_example["m3"])} / {format_usd(week_example["spend"])} = {(week_example["m3"] / week_example["spend"] * 100):.1f}%.</li>
          </ul>
          <p><strong>Starter-family denominators</strong></p>
          <ul>
            <li>Monthly starters: {week_example["family_counts"].get("monthly", 0)} with actual retentions M1 {(week_example["actual_retention"].get("monthly", {}).get(1, 0.0) * 100):.1f}%, M2 {(week_example["actual_retention"].get("monthly", {}).get(2, 0.0) * 100):.1f}%, M3 {(week_example["actual_retention"].get("monthly", {}).get(3, 0.0) * 100):.1f}%.</li>
            <li>Quarterly starters: {week_example["family_counts"].get("quarterly", 0)} with actual M3 retention {(week_example["actual_retention"].get("quarterly", {}).get(3, 0.0) * 100):.1f}%.</li>
            <li>Annual starters: {week_example["family_counts"].get("annual", 0)}.</li>
          </ul>
          <p><strong>How M6 is projected from the last actual month</strong></p>
          <ul>
            <li>Monthly anchor is actual M3 retention {(week_example["actual_retention"].get("monthly", {}).get(3, 0.0) * 100):.1f}%. Then M4 = M3 × 9%/12% = {(week_example["monthly_m4"] * 100):.1f}%, M5 = M4 × 8%/9% = {(week_example["monthly_m5"] * 100):.1f}%, M6 = M5 × 8%/8% = {(week_example["monthly_m6"] * 100):.1f}%.</li>
            <li>Quarterly anchor is actual M3 retention {(week_example["actual_retention"].get("quarterly", {}).get(3, 0.0) * 100):.1f}%. Then M6 = M3 × 30%/55% = {(week_example["quarterly_m6"] * 100):.1f}%.</li>
            <li>The projected M6 tail uses the real renewal-price pools and then applies the 15% haircut: monthly tail {format_usd(week_example["monthly_m6_tail"])}, quarterly tail {format_usd(week_example["quarterly_m6_tail"])}, annual tail {format_usd(week_example["annual_m6_tail"])}.</li>
            <li>So projected M6 = actual M3 base {format_usd(week_example["m3"])} + future tail {format_usd(week_example["monthly_m6_tail"] + week_example["quarterly_m6_tail"] + week_example["annual_m6_tail"])} = {format_usd(week_example["m6"])}, which is {(week_example["m6"] / week_example["spend"] * 100):.1f}% ROAS.</li>
          </ul>
        </div>
      </details>
    """
    client_config = json.dumps(
        {
            "targetMonths": TARGET_MONTHS,
            "currentDate": CURRENT_DATE.isoformat(),
            "projectedNetRevenueFactor": PROJECTED_NET_REVENUE_FACTOR,
            "retentionRates": RETENTION_RATE_BENCHMARKS,
            "planRevenueSchedules": PLAN_REVENUE_SCHEDULES,
            "priceCatalog": catalog_payload,
            "ecbCurPerEur": ECB_CUR_PER_EUR,
            "defaultSpendMaps": {
                "monthly": monthly_spend_map,
                "weekly": weekly_spend_upload_map,
            },
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

    .tabs-card {{
      padding: 18px 22px;
      margin-bottom: 18px;
    }}

    .chart-card {{
      padding: 22px 22px 12px;
      margin-bottom: 22px;
    }}

    .tabs {{
      display: inline-flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 0;
    }}

    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      margin-bottom: 18px;
    }}

    .tabs-card .toolbar {{
      margin-bottom: 0;
    }}

    .toolbar-actions {{
      display: inline-flex;
      flex-wrap: wrap;
      gap: 10px;
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

    .action-button {{
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(12, 20, 38, 0.45);
      color: var(--text);
      border-radius: 999px;
      padding: 10px 16px;
      font-size: 0.92rem;
      font-weight: 600;
      cursor: pointer;
      transition: 0.2s ease;
    }}

    .action-button:hover {{
      border-color: rgba(24, 199, 243, 0.4);
    }}

    .action-button.secondary {{
      color: var(--muted);
    }}

    .action-button:disabled {{
      cursor: not-allowed;
      opacity: 0.5;
    }}

    .upload-card {{
      padding: 18px 20px;
      margin-bottom: 22px;
    }}

    .upload-card > summary {{
      list-style: none;
      cursor: pointer;
    }}

    .upload-card > summary::-webkit-details-marker {{
      display: none;
    }}

    .upload-summary {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 14px;
    }}

    .upload-summary-copy {{
      min-width: 0;
    }}

    .upload-panel {{
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 14px;
      margin-top: 16px;
    }}

    .upload-field {{
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}

    .upload-field label {{
      font-size: 0.85rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }}

    .upload-field input[type="file"] {{
      width: 100%;
      color: var(--muted);
      background: rgba(12, 20, 38, 0.45);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 14px;
      padding: 10px 12px;
      font-size: 0.88rem;
    }}

    .upload-status {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.94rem;
      line-height: 1.5;
    }}

    .upload-status strong {{
      color: var(--text);
    }}

    .upload-card:not([open]) .upload-panel,
    .upload-card:not([open]) .toolbar {{
      display: none;
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
      min-width: 1660px;
      border-collapse: separate;
      border-spacing: 0;
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
      white-space: normal;
      line-height: 1.25;
      min-width: 86px;
      position: sticky;
      top: 0;
      z-index: 2;
      background: linear-gradient(180deg, rgba(48, 64, 94, 0.98), rgba(34, 47, 71, 0.98));
    }}

    .th-two-line {{
      display: inline-block;
      white-space: nowrap;
      line-height: 1.2;
    }}

    th:nth-child(8),
    td:nth-child(8),
    th:nth-child(9),
    td:nth-child(9),
    th:nth-child(10),
    td:nth-child(10),
    th:nth-child(11),
    td:nth-child(11),
    th:nth-child(12),
    td:nth-child(12),
    th:nth-child(13),
    td:nth-child(13) {{
      min-width: 118px;
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
      min-width: 170px;
      width: 170px;
    }}

    th:first-child {{
      z-index: 6;
    }}

    #main-table-card th:first-child,
    #main-table-card td:first-child {{
      left: 0;
      min-width: 170px;
      width: 170px;
      z-index: 4;
    }}

    #main-table-card th:nth-child(2),
    #main-table-card td:nth-child(2),
    #main-table-card th:nth-child(3),
    #main-table-card td:nth-child(3),
    #main-table-card th:nth-child(4),
    #main-table-card td:nth-child(4) {{
      position: sticky;
      z-index: 3;
      background: linear-gradient(180deg, rgba(41, 56, 83, 0.98), rgba(27, 39, 61, 0.98));
    }}

    #main-table-card th:nth-child(2),
    #main-table-card td:nth-child(2) {{
      left: 170px;
      min-width: 110px;
      width: 110px;
    }}

    #main-table-card th:nth-child(3),
    #main-table-card td:nth-child(3) {{
      left: 280px;
      min-width: 90px;
      width: 90px;
    }}

    #main-table-card th:nth-child(4),
    #main-table-card td:nth-child(4) {{
      left: 370px;
      min-width: 90px;
      width: 90px;
    }}

    #main-table-card thead th:nth-child(2),
    #main-table-card thead th:nth-child(3),
    #main-table-card thead th:nth-child(4) {{
      z-index: 6;
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

    .retention-metric {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      line-height: 1.1;
    }}

    .retention-metric .rate {{
      font-weight: 700;
      color: #dfe8fb;
      font-size: 1rem;
    }}

    .retention-metric .count {{
      color: var(--muted);
      font-size: 0.92rem;
    }}

    .retention-metric.closed .rate,
    .retention-metric.closed .count {{
      color: #dfe8fb;
      font-weight: 700;
    }}

    .retention-metric.open .rate,
    .retention-metric.open .count {{
      color: #8fa0bf;
      font-weight: 400;
    }}

    #retention-card table {{
      min-width: 980px;
    }}

    #retention-card th,
    #retention-card td {{
      padding: 14px 12px;
    }}

    #retention-card th {{
      min-width: 72px;
    }}

    #retention-card th:first-child,
    #retention-card td:first-child {{
      min-width: 150px;
      width: 150px;
    }}

    #retention-card th:nth-child(2),
    #retention-card td:nth-child(2) {{
      min-width: 92px;
      width: 92px;
    }}

    #retention-card th:nth-child(n+3),
    #retention-card td:nth-child(n+3) {{
      min-width: 96px;
      width: 96px;
    }}

    .footer-note {{
      margin-top: 18px;
      padding: 18px 22px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.7;
    }}

    .footer-note ul {{
      margin: 0;
      padding-left: 20px;
    }}

    .footer-note li + li {{
      margin-top: 8px;
    }}

    .explanation-card {{
      margin-top: 18px;
      padding: 22px 24px;
    }}

    .explanation-card h2 {{
      margin: 0 0 8px;
      font-size: 1.05rem;
    }}

    .explanation-card p {{
      margin: 0;
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.65;
    }}

    .forecast-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
    }}

    .forecast-block {{
      padding: 16px 18px;
      border-radius: 18px;
      background: rgba(12, 20, 38, 0.28);
      border: 1px solid rgba(255, 255, 255, 0.06);
    }}

    .forecast-block h3 {{
      margin: 0 0 10px;
      font-size: 0.96rem;
    }}

    .forecast-block ul {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.65;
    }}

    .forecast-block li + li {{
      margin-top: 7px;
    }}

    .forecast-details {{
      margin-top: 18px;
      border-radius: 18px;
      background: rgba(12, 20, 38, 0.32);
      border: 1px solid rgba(255, 255, 255, 0.06);
      overflow: hidden;
    }}

    .forecast-details summary {{
      cursor: pointer;
      padding: 16px 18px;
      font-weight: 700;
      color: var(--text);
      list-style: none;
    }}

    .forecast-details summary::-webkit-details-marker {{
      display: none;
    }}

    .forecast-details[open] summary {{
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }}

    .forecast-copy {{
      padding: 16px 18px 18px;
    }}

    .forecast-copy p {{
      margin: 0 0 8px;
      color: var(--text);
      font-size: 0.94rem;
    }}

    .forecast-copy ul {{
      margin: 0 0 14px;
      padding-left: 18px;
      color: var(--muted);
      font-size: 0.91rem;
      line-height: 1.65;
    }}

    .forecast-copy li + li {{
      margin-top: 7px;
    }}

    @media (max-width: 1024px) {{
      .hero {{
        grid-template-columns: 1fr;
      }}

      .upload-panel {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}

      .forecast-grid {{
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

      .toolbar {{
        align-items: stretch;
      }}

      .upload-panel {{
        grid-template-columns: 1fr;
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
          <div class="summary-value" id="summary-customers">{totals["customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Paid Customers</div>
          <div class="summary-value" id="summary-paid">{totals["paid_customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Active Customers</div>
          <div class="summary-value" id="summary-active">{totals["active_customers"]:,}</div>
        </div>
        <div class="summary-pill">
          <div class="summary-label">Repeat Customers</div>
          <div class="summary-value" id="summary-repeat">{totals["repeat_customers"]:,}</div>
        </div>
      </div>
    </section>

    <details class="card upload-card" id="upload-card">
      <summary class="upload-summary" id="toggle-upload">
        <div class="upload-summary-copy">
          <h2 class="section-title">Upload Exports</h2>
          <p class="section-note">Load fresh CSV exports in-browser to recalculate the dashboard without rebuilding the page.</p>
        </div>
        <span class="action-button">Upload Exports</span>
      </summary>
      <div class="upload-panel" id="upload-panel">
        <div class="upload-field">
          <label for="customers-upload">Customers CSV</label>
          <input id="customers-upload" type="file" accept=".csv,text/csv">
        </div>
        <div class="upload-field">
          <label for="payments-upload">Payments CSV</label>
          <input id="payments-upload" type="file" accept=".csv,text/csv">
        </div>
        <div class="upload-field">
          <label for="spend-upload">Spend CSV</label>
          <input id="spend-upload" type="file" accept=".csv,text/csv">
        </div>
        <div class="upload-field">
          <label for="prices-upload">Prices CSV (Optional)</label>
          <input id="prices-upload" type="file" accept=".csv,text/csv">
        </div>
      </div>
      <div class="toolbar">
        <div class="upload-status" id="upload-status"><strong>Using embedded exports.</strong> Upload customers and payments CSVs to recalculate monthly and weekly cohorts.</div>
        <div class="toolbar-actions">
          <button class="action-button secondary" type="button" id="reset-uploads" hidden>Use Embedded Data</button>
          <button class="action-button secondary" type="button" id="publish-uploads" disabled>Publish Imported Data</button>
          <button class="action-button" type="button" id="apply-uploads" disabled>Recalculate Dashboard</button>
        </div>
      </div>
    </details>

    <section class="card tabs-card">
      <div class="toolbar">
        <div class="tabs" role="tablist" aria-label="Cohort views">
          <button class="tab-button active" type="button" data-view="monthly">Monthly Cohorts</button>
          <button class="tab-button" type="button" data-view="weekly">Weekly Cohorts</button>
          <button class="tab-button" type="button" data-view="weekly-retention">Monthly Plan Retention</button>
          <button class="tab-button" type="button" data-view="quarterly-retention">Quarterly Plan Retention</button>
        </div>
      </div>
    </section>

    <section class="card chart-card">
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
        <svg id="chart" viewBox="0 0 1320 380" preserveAspectRatio="xMidYMid meet" aria-label="Cohort chart">{initial_chart_markup}</svg>
      </div>
    </section>

    <section class="card table-card" id="main-table-card">
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
              <th><span class="th-two-line">D0<br>Rev</span></th>
              <th><span class="th-two-line">Net Rev<br>To Date</span></th>
              <th>Refunds</th>
              <th><span class="th-two-line">Dispute<br>Losses</span></th>
              <th><span class="th-two-line">D0<br>ROAS</span></th>
              <th><span class="th-two-line">ROAS<br>To Date</span></th>
              <th>M1</th>
              <th>M2</th>
              <th>M3</th>
              <th>M6</th>
              <th>M9</th>
              <th>M12</th>
            </tr>
          </thead>
          <tbody id="tbody">{initial_table_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="card table-card" id="retention-card" style="display:none;">
      <div class="table-scroll">
        <table id="retention-table-monthly">
          <thead>
            <tr>
              <th>Cohort</th>
              <th><span class="th-two-line">Monthly<br>Starters</span></th>
              <th><span class="th-two-line">M1<br>% / #</span></th>
              <th><span class="th-two-line">M2<br>% / #</span></th>
              <th><span class="th-two-line">M3<br>% / #</span></th>
            </tr>
          </thead>
          <tbody id="retention-tbody"></tbody>
        </table>
        <table id="retention-table-quarterly" style="display:none;">
          <thead>
            <tr>
              <th>Cohort</th>
              <th><span class="th-two-line">Quarterly<br>Starters</span></th>
              <th><span class="th-two-line">M3<br>% / #</span></th>
              <th><span class="th-two-line">M6<br>% / #</span></th>
              <th><span class="th-two-line">M9<br>% / #</span></th>
              <th><span class="th-two-line">M12<br>% / #</span></th>
            </tr>
          </thead>
          <tbody id="quarterly-retention-tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card explanation-card">
      <h2>How Milestones And Forecasts Work</h2>
      <p>
        Milestones are cumulative revenue points. <strong>M1</strong> means revenue through cohort end + 1 month,
        <strong>M2</strong> through cohort end + 2 months, <strong>M3</strong> through cohort end + 3 months, and so on.
        If a milestone cutoff has already passed, the dashboard shows the actual net value. If the cutoff has not passed yet,
        the dashboard forecasts only the missing future tail.
      </p>

      <div class="forecast-grid">
        <div class="forecast-block">
          <h3>What Counts As Actual</h3>
          <ul>
            <li>Closed M1/M2/M3 values use actual renewal progress per customer: M1 means first renewal, M2 means second renewal, M3 means third renewal.</li>
            <li><strong>Net</strong> means paid transaction revenue minus refunds, with dispute losses subtracted as a cohort-level proxy.</li>
            <li>This avoids dropping real renewals just because Stripe retried the billing a few days later than the calendar milestone date.</li>
            <li>Cohort membership and <strong>New</strong> counts still come from the customer export.</li>
          </ul>
        </div>

        <div class="forecast-block">
          <h3>Benchmark Rates Used</h3>
          <ul>
            <li>Monthly: M1 55%, M2 27%, M3 12%, M4 9%, M5 8%, M6 8%, M7 7%, M8 6%, M9 5%, M10 3%, M11 2%, M12 1%.</li>
            <li>Quarterly: M3 55%, M6 30%, M9 15%, M12 10%.</li>
            <li>Annual: M12 35%.</li>
          </ul>
        </div>

        <div class="forecast-block">
          <h3>How Prediction Starts</h3>
          <ul>
            <li>Each customer is assigned to a starter family from their first paid <strong>Subscription creation</strong> transaction: monthly, quarterly, or annual.</li>
            <li>Guest customers (`gcus_*`) and one-time guest purchases are excluded from subscription-family prediction.</li>
            <li>If the current <strong>Plan</strong> later goes blank, that customer still stays in the original starter-family denominator, so canceled starters remain included.</li>
            <li>The last closed actual milestone for that family becomes the anchor retention point.</li>
          </ul>
        </div>

        <div class="forecast-block">
          <h3>How Future Revenue Is Added</h3>
          <ul>
            <li>Later milestone retention is projected by applying the benchmark month-over-month ratio to the last actual retention (for example: if actual M1 retention = 46% and the monthly benchmark goes from M1 55% to M2 27%, then predicted M2 retention = 46% × 27% / 55% = 22.6%).</li>
            <li>The projected future tail uses each priced customer’s real full renewal price, not a generic bucket price.</li>
            <li>First-day upsell revenue stays inside cumulative predicted ROAS, and projected future revenue gets a 15% haircut for refunds and fees.</li>
          </ul>
        </div>
      </div>

{week_example_markup}
    </section>

  </div>

  <script>
    const defaultPayload = {payload};
    const clientConfig = {client_config};
    const STORAGE_KEY = "cohort_dashboard_published_payload_v2";
    const state = {{
      payload: defaultPayload,
      uploaded: false,
    }};
    let activeView = "monthly";
    const uploadFiles = {{
      customers: null,
      payments: null,
      spend: null,
      prices: null,
    }};

    function serializeForInlineScript(value) {{
      return JSON.stringify(value).replace(/<\\//g, "<\\\\/");
    }}

    function loadPublishedPayload() {{
      try {{
        const raw = window.localStorage.getItem(STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed || !parsed.monthly || !parsed.weekly || !parsed.weeklyRetention) {{
          return null;
        }}
        return parsed;
      }} catch (_error) {{
        return null;
      }}
    }}

    function savePublishedPayload(payload) {{
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    }}

    function clearPublishedPayload() {{
      window.localStorage.removeItem(STORAGE_KEY);
    }}

    function parseDelimited(text) {{
      const sampleLine = text.split(/\\\\r?\\\\n/).find((line) => line.trim().length);
      const delimiter = sampleLine && sampleLine.includes("\t") && !sampleLine.includes(",") ? "\t" : ",";
      const rows = [];
      let row = [];
      let field = "";
      let inQuotes = false;
      for (let i = 0; i < text.length; i += 1) {{
        const char = text[i];
        const next = text[i + 1];
        if (char === '"') {{
          if (inQuotes && next === '"') {{
            field += '"';
            i += 1;
          }} else {{
            inQuotes = !inQuotes;
          }}
          continue;
        }}
        if (!inQuotes && char === delimiter) {{
          row.push(field);
          field = "";
          continue;
        }}
        if (!inQuotes && (char === "\\n" || char === "\\r")) {{
          if (char === "\\r" && next === "\\n") {{
            i += 1;
          }}
          row.push(field);
          if (row.some((value) => value.trim() !== "")) {{
            rows.push(row);
          }}
          row = [];
          field = "";
          continue;
        }}
        field += char;
      }}
      row.push(field);
      if (row.some((value) => value.trim() !== "")) {{
        rows.push(row);
      }}
      if (!rows.length) {{
        return [];
      }}
      const headers = rows[0].map((value) => value.trim());
      return rows.slice(1).map((values) => {{
        const record = {{}};
        headers.forEach((header, index) => {{
          record[header] = (values[index] || "").trim();
        }});
        return record;
      }});
    }}

    function parseAmount(value) {{
      const raw = String(value || "").trim().replace(/€/g, "").replace(/\\$/g, "");
      if (!raw) {{
        return 0;
      }}
      if (raw.includes(",") && raw.includes(".")) {{
        return raw.lastIndexOf(",") > raw.lastIndexOf(".")
          ? Number(raw.replace(/\./g, "").replace(",", "."))
          : Number(raw.replace(/,/g, ""));
      }}
      if (raw.includes(",")) {{
        return Number(raw.replace(",", "."));
      }}
      return Number(raw);
    }}

    function parseIntValue(value) {{
      const raw = String(value || "").trim();
      return raw ? Number.parseInt(raw, 10) : 0;
    }}

    function parseCompactEuroAmount(value) {{
      const raw = String(value || "").trim().replace(/€/g, "").replace(/\s+/g, "");
      if (!raw) {{
        return 0;
      }}
      const parts = raw.split(".");
      if (parts.length === 2 && parts[1].length === 3) {{
        return Number(parts[0] + parts[1]);
      }}
      return parseAmount(raw);
    }}

    function monthEnd(monthKey) {{
      const [year, month] = monthKey.split("-").map(Number);
      return new Date(Date.UTC(year, month, 0));
    }}

    function toIsoDate(dateObj) {{
      return dateObj.toISOString().slice(0, 10);
    }}

    function addMonths(dateObj, months) {{
      const year = dateObj.getUTCFullYear();
      const month = dateObj.getUTCMonth();
      const day = dateObj.getUTCDate();
      const shifted = new Date(Date.UTC(year, month + months, 1));
      const lastDay = new Date(Date.UTC(shifted.getUTCFullYear(), shifted.getUTCMonth() + 1, 0)).getUTCDate();
      shifted.setUTCDate(Math.min(day, lastDay));
      return shifted;
    }}

    function milestoneClosedFromEnd(cohortEndDate, milestoneMonths) {{
      return toIsoDate(new Date(clientConfig.currentDate + "T00:00:00Z")) >= toIsoDate(addMonths(cohortEndDate, milestoneMonths));
    }}

    function anchoredWeekRange(dateString) {{
      const anchor = new Date(Date.UTC(2026, 0, 1));
      const current = new Date(dateString + "T00:00:00Z");
      const diffDays = Math.max(0, Math.floor((current - anchor) / 86400000));
      const weekIndex = Math.floor(diffDays / 7);
      const weekStart = new Date(anchor.getTime() + weekIndex * 7 * 86400000);
      const weekEnd = new Date(weekStart.getTime() + 6 * 86400000);
      return [weekStart, weekEnd];
    }}

    function formatShortDate(dateObj) {{
      return dateObj.toLocaleDateString("en-US", {{
        month: "short",
        day: "numeric",
        timeZone: "UTC",
      }}).replace(",", "");
    }}

    function formatMonth(monthKey) {{
      return new Date(monthKey + "-01T00:00:00Z").toLocaleDateString("en-US", {{
        month: "long",
        year: "numeric",
        timeZone: "UTC",
      }});
    }}

    function formatWeekChartLabel(weekStart) {{
      const anchor = new Date(Date.UTC(2026, 0, 1));
      const weekNumber = Math.floor((weekStart - anchor) / (7 * 86400000)) + 1;
      return `W${{String(weekNumber).padStart(2, "0")}}`;
    }}

    function modeValue(values) {{
      const counts = new Map();
      values.forEach((value) => {{
        const rounded = Number(value.toFixed(2));
        counts.set(rounded, (counts.get(rounded) || 0) + 1);
      }});
      let bestValue = 0;
      let bestCount = -1;
      Array.from(counts.entries()).forEach(([value, count]) => {{
        if (count > bestCount || (count === bestCount && value < bestValue)) {{
          bestValue = value;
          bestCount = count;
        }}
      }});
      return bestValue;
    }}

    function loadFirstChargeLookup(rows) {{
      const groups = new Map();
      rows.forEach((row) => {{
        if (parseIntValue(row["Payment Count"]) !== 1) {{
          return;
        }}
        const key = `${{(row.Plan || "(blank)").trim() || "(blank)"}}|||${{(row.Currency || "").trim() || "blank"}}`;
        const value = Number(parseAmount(row["Total Spend"]).toFixed(2));
        if (value <= 0) {{
          return;
        }}
        if (!groups.has(key)) {{
          groups.set(key, []);
        }}
        groups.get(key).push(value);
      }});
      const lookup = new Map();
      groups.forEach((values, key) => lookup.set(key, modeValue(values)));
      return lookup;
    }}

    function loadCumulativeChargeLookup(rows) {{
      const groups = new Map();
      rows.forEach((row) => {{
        const paymentCount = parseIntValue(row["Payment Count"]);
        if (paymentCount < 1) {{
          return;
        }}
        const key = `${{(row.Plan || "(blank)").trim() || "(blank)"}}|||${{(row.Currency || "").trim() || "blank"}}|||${{paymentCount}}`;
        const value = Number(parseAmount(row["Total Spend"]).toFixed(2));
        if (value <= 0) {{
          return;
        }}
        if (!groups.has(key)) {{
          groups.set(key, []);
        }}
        groups.get(key).push(value);
      }});
      const lookup = new Map();
      groups.forEach((values, key) => lookup.set(key, modeValue(values)));
      return lookup;
    }}

    function isCoreSubscriptionPlan(meta) {{
      if (!meta) {{
        return true;
      }}
      const name = String(meta.product_name || "").toLowerCase();
      return name.includes("subscription") && !name.includes("add-on") && !name.includes("upgrade") && !name.includes("one-time");
    }}

    function planIsMonthly(planId, catalog) {{
      const meta = catalog[planId];
      return Boolean(meta && meta.interval === "month" && String(meta.interval_count) === "1");
    }}

    function planIsQuarterly(planId, catalog) {{
      const meta = catalog[planId];
      return Boolean(meta && meta.interval === "month" && String(meta.interval_count) === "3");
    }}

    function planIsAnnual(planId, catalog) {{
      const meta = catalog[planId];
      return Boolean(meta && meta.interval === "year" && String(meta.interval_count) === "1");
    }}

    function convertToEur(amount, currency, monthKey) {{
      const code = String(currency || "EUR").toUpperCase();
      if (!amount) {{
        return 0;
      }}
      if (code === "EUR" || code === "UNKNOWN") {{
        return amount;
      }}
      const rate = clientConfig.ecbCurPerEur[monthKey]?.[code];
      return rate ? amount / rate : amount;
    }}

    function planFullPriceEur(planId, monthKey, catalog) {{
      const meta = catalog[planId];
      if (!meta || !isCoreSubscriptionPlan(meta)) {{
        return null;
      }}
      return convertToEur(parseAmount(meta.amount), meta.currency, monthKey);
    }}

    function classifyPlanFamily(planId, firstCharge, currency, catalog) {{
      if (planIsMonthly(planId, catalog)) return "monthly";
      if (planIsQuarterly(planId, catalog)) return "quarterly";
      if (planIsAnnual(planId, catalog)) return "annual";
      const code = String(currency || "").toLowerCase();
      if (code === "usd" || code === "blank" || code === "") {{
        if (firstCharge <= 19) return "monthly";
        if (firstCharge <= 35) return "quarterly";
        if (firstCharge <= 65) return "annual";
      }}
      if (code === "eur") {{
        if (firstCharge <= 24) return "monthly";
        if (firstCharge <= 40) return "quarterly";
        if (firstCharge <= 65) return "annual";
      }}
      return null;
    }}

    function inferBlankUsdProjection(totalSpend) {{
      const candidates = [
        ["monthly", 34.27, 16.80, 34.27],
        ["monthly", 34.27, 13.37, 34.27],
        ["quarterly", 57.92, 27.42, 57.92],
        ["quarterly", 57.92, 20.57, 57.92],
        ["annual", 102.85, 50.37, 0.0],
        ["annual", 102.85, 40.09, 0.0],
      ];
      let best = null;
      candidates.forEach(([family, fullPrice, intro, renewal]) => {{
        const maxRenewals = family === "monthly" ? 3 : 1;
        for (let renewals = 0; renewals <= maxRenewals; renewals += 1) {{
          for (let upsells = 0; upsells < 3; upsells += 1) {{
            const estimate = intro + renewal * renewals + 8.56 * upsells;
            const gap = Math.abs(totalSpend - estimate);
            if (!best || gap < best.gap) {{
              best = {{ gap, family, fullPrice }};
            }}
          }}
        }}
      }});
      return best && best.gap <= 0.75 ? [best.family, best.fullPrice] : null;
    }}

    function projectionSpecForRow(row, monthKey, firstSubscriptionAmountEur, catalog) {{
      const planId = String(row.Plan || "").trim();
      const currency = String(row.Currency || "").trim().toLowerCase();
      const totalSpend = parseAmount(row["Total Spend"]);
      if (planId) {{
        const family = classifyPlanFamily(planId, firstSubscriptionAmountEur || 0, currency, catalog);
        const fullPriceEur = planFullPriceEur(planId, monthKey, catalog);
        if (family && fullPriceEur) {{
          return [family, fullPriceEur];
        }}
        return null;
      }}
      if (currency === "usd") {{
        return inferBlankUsdProjection(totalSpend);
      }}
      return null;
    }}

    function starterFamilyForRow(row, monthKey, firstSubscriptionInfo, catalog) {{
      const customerId = String(row.id || "").trim();
      if (customerId.startsWith("gcus_")) return null;
      const planId = String(row.Plan || "").trim();
      if (planId) {{
        return classifyPlanFamily(planId, 0, String(row.Currency || "").trim(), catalog);
      }}
      if (firstSubscriptionInfo && Number(firstSubscriptionInfo.amountEur || 0) > 0) {{
        return classifyPlanFamily("", Number(firstSubscriptionInfo.amountEur || 0), "eur", catalog);
      }}
      const currency = String(row.Currency || "").trim().toLowerCase();
      const totalSpend = parseAmount(row["Total Spend"]);
      if (currency === "usd") {{
        const inferred = inferBlankUsdProjection(totalSpend);
        if (inferred) return inferred[0];
      }}
      return null;
    }}

    function inferFullPriceFromIntroEur(family, introAmountEur, monthKey) {{
      const candidates = [];
      Object.values(clientConfig.planRevenueSchedules || {{}}).forEach((spec) => {{
        if (spec.family !== family) return;
        const introEur = convertToEur(spec.intro, "usd", monthKey);
        const fullEur = convertToEur(spec.full_price, "usd", monthKey);
        candidates.push([introEur, fullEur]);
      }});
      if (family === "quarterly") {{
        candidates.push([32.0, 79.99], [23.99, 79.99]);
      }} else if (family === "annual") {{
        candidates.push([58.8, 119.99], [46.8, 119.99]);
      }}
      let best = null;
      candidates.forEach(([introEur, fullEur]) => {{
        const gap = Math.abs(introAmountEur - introEur);
        if (!best || gap < best.gap) {{
          best = {{ gap, fullEur }};
        }}
      }});
      return best && best.gap <= 4.5 ? best.fullEur : null;
    }}

    function projectionPriceForRow(row, monthKey, family, firstSubscriptionInfo, firstUpdateAmountEur, catalog) {{
      const customerId = String(row.id || "").trim();
      if (customerId.startsWith("gcus_")) return null;
      if (!family) return null;
      const planId = String(row.Plan || "").trim();
      if (planId) {{
        return planFullPriceEur(planId, monthKey, catalog);
      }}
      if (firstUpdateAmountEur && firstUpdateAmountEur > 0) {{
        return firstUpdateAmountEur;
      }}
      if (firstSubscriptionInfo && Number(firstSubscriptionInfo.amountEur || 0) > 0) {{
        const inferred = inferFullPriceFromIntroEur(family, Number(firstSubscriptionInfo.amountEur || 0), monthKey);
        if (inferred) return inferred;
      }}
      const currency = String(row.Currency || "").trim().toLowerCase();
      const totalSpend = parseAmount(row["Total Spend"]);
      if (currency === "usd") {{
        const inferred = inferBlankUsdProjection(totalSpend);
        if (inferred && inferred[0] === family) return inferred[1];
      }}
      return null;
    }}

    function projectedBenchmarkRevenue(family, fullPriceEur, milestoneMonths) {{
      const rates = clientConfig.retentionRates[family] || {{}};
      let total = 0;
      Object.entries(rates).forEach(([monthNumber, retention]) => {{
        if (Number(monthNumber) <= milestoneMonths) {{
          total += fullPriceEur * retention;
        }}
      }});
      return total * clientConfig.projectedNetRevenueFactor;
    }}

    function benchmarkMilestonesForFamily(family, milestoneMonths = null) {{
      const months = Object.keys(clientConfig.retentionRates[family] || {{}})
        .map((value) => Number(value))
        .sort((a, b) => a - b);
      if (milestoneMonths == null) {{
        return months;
      }}
      return months.filter((month) => month <= milestoneMonths);
    }}

    function benchmarkRateForFamilyMonth(family, monthNumber) {{
      const rates = clientConfig.retentionRates[family] || {{}};
      return Number(rates[monthNumber] || 0);
    }}

    function lastClosedBenchmarkMonth(cohortEndDate, family, targetMonths) {{
      const closed = benchmarkMilestonesForFamily(family, targetMonths)
        .filter((month) => milestoneClosedFromEnd(cohortEndDate, month));
      return closed.length ? Math.max(...closed) : 0;
    }}

    function requiredPaymentCount(family, monthNumber) {{
      if (family === "monthly") return monthNumber + 1;
      if (family === "quarterly") return Math.floor(monthNumber / 3) + 1;
      if (family === "annual") return 2;
      return 1;
    }}

    function projectedPopTailRevenue(family, fullPriceEur, cohortEndDate, targetMonths, actualRetentionRates) {{
      const milestoneMonths = benchmarkMilestonesForFamily(family, targetMonths);
      if (!milestoneMonths.length) return 0;

      const lastActualMonth = lastClosedBenchmarkMonth(cohortEndDate, family, targetMonths);
      if (!lastActualMonth) {{
        return projectedBenchmarkRevenue(family, fullPriceEur, targetMonths);
      }}

      const familyRetention = actualRetentionRates[family] || {{}};
      let previousRate = Number(familyRetention[lastActualMonth] || 0);
      if (!previousRate) {{
        return projectedBenchmarkRevenue(family, fullPriceEur, targetMonths);
      }}

      let previousMonth = lastActualMonth;
      let total = 0;
      milestoneMonths.forEach((monthNumber) => {{
        if (monthNumber <= lastActualMonth) return;
        const previousBenchmark = benchmarkRateForFamilyMonth(family, previousMonth);
        const currentBenchmark = benchmarkRateForFamilyMonth(family, monthNumber);
        if (!previousBenchmark || !currentBenchmark) {{
          previousMonth = monthNumber;
          return;
        }}
        previousRate *= currentBenchmark / previousBenchmark;
        total += fullPriceEur * previousRate;
        previousMonth = monthNumber;
      }});
      return total * clientConfig.projectedNetRevenueFactor;
    }}

    function loadSubscriptionCreationInfo(paymentsRows) {{
      const first = new Map();
      paymentsRows.forEach((payment) => {{
        if (payment.Status !== "Paid") return;
        const customerId = String(payment["Customer ID"] || "").trim();
        if (!customerId) return;
        if (String(payment.Description || "").trim() !== "Subscription creation") return;
        const created = String(payment["Created date (UTC)"] || "").slice(0, 10);
        if (!created) return;
        const amount = parseAmount(payment["Converted Amount"]) - parseAmount(payment["Converted Amount Refunded"]);
        if (amount <= 0) return;
        const existing = first.get(customerId);
        if (!existing || created < existing.date) {{
          first.set(customerId, {{
            date: created,
            amountEur: amount,
            amountRaw: parseAmount(payment.Amount) - parseAmount(payment["Amount Refunded"]),
            currency: String(payment.Currency || "").trim(),
          }});
        }}
      }});
      return first;
    }}

    function loadFirstSubscriptionUpdateAmounts(paymentsRows) {{
      const first = new Map();
      paymentsRows.forEach((payment) => {{
        if (payment.Status !== "Paid") return;
        const customerId = String(payment["Customer ID"] || "").trim();
        if (!customerId) return;
        if (String(payment.Description || "").trim() !== "Subscription update") return;
        const created = String(payment["Created date (UTC)"] || "").slice(0, 10);
        if (!created) return;
        const amount = parseAmount(payment["Converted Amount"]) - parseAmount(payment["Converted Amount Refunded"]);
        if (amount <= 0) return;
        const existing = first.get(customerId);
        if (!existing || created < existing.date) {{
          first.set(customerId, {{ date: created, amount }});
        }}
      }});
      const result = new Map();
      first.forEach((value, key) => result.set(key, value.amount));
      return result;
    }}

    function subscriptionBillingCountsThrough(rows, paymentsRows, cutoffDate) {{
      const cohortStarts = new Map();
      rows.forEach((row) => {{
        const id = String(row.id || "").trim();
        const created = String(row["Created (UTC)"] || "").slice(0, 10);
        if (id && created) {{
          cohortStarts.set(id, created);
        }}
      }});
      const counts = new Map(Array.from(cohortStarts.keys(), (id) => [id, 0]));
      paymentsRows.forEach((payment) => {{
        const customerId = String(payment["Customer ID"] || "").trim();
        const created = String(payment["Created date (UTC)"] || "").slice(0, 10);
        if (!cohortStarts.has(customerId) || payment.Status !== "Paid" || !created) return;
        if (created < cohortStarts.get(customerId) || created > cutoffDate) return;
        const description = String(payment.Description || "").trim();
        if (description === "Subscription creation" || description === "Subscription update") {{
          counts.set(customerId, (counts.get(customerId) || 0) + 1);
        }}
      }});
      return counts;
    }}

    function actualRevenueThroughRows(rows, paymentsRows, cutoffDate, firstChargeLookup, cumulativeChargeLookup, catalog) {{
      const cohortStarts = new Map();
      rows.forEach((row) => {{
        const id = String(row.id || "").trim();
        const created = String(row["Created (UTC)"] || "").slice(0, 10);
        if (id && created) {{
          cohortStarts.set(id, created);
        }}
      }});
      if (!cohortStarts.size) {{
        return 0;
      }}
      const paymentsByCustomer = new Map();
      paymentsRows.forEach((payment) => {{
        const customerId = String(payment["Customer ID"] || "").trim();
        if (!cohortStarts.has(customerId) || payment.Status !== "Paid") return;
        if (!paymentsByCustomer.has(customerId)) {{
          paymentsByCustomer.set(customerId, []);
        }}
        paymentsByCustomer.get(customerId).push(payment);
      }});
      let revenue = 0;
      const customersWithPaymentRows = new Set();
      paymentsByCustomer.forEach((items, customerId) => {{
        const cohortStart = cohortStarts.get(customerId);
        items.forEach((payment) => {{
          const created = String(payment["Created date (UTC)"] || "").slice(0, 10);
          if (!created) return;
          if (created >= cohortStart && created <= cutoffDate) {{
            revenue += parseAmount(payment["Converted Amount"]) - parseAmount(payment["Converted Amount Refunded"]);
            customersWithPaymentRows.add(customerId);
          }}
        }});
      }});
      if (customersWithPaymentRows.size === cohortStarts.size) {{
        return revenue;
      }}
      rows
        .filter((row) => !customersWithPaymentRows.has(String(row.id || "").trim()))
        .forEach((row) => {{
          const paymentCount = parseIntValue(row["Payment Count"]);
          if (paymentCount < 1) return;
          const key = `${{(row.Plan || "(blank)").trim() || "(blank)"}}|||${{(row.Currency || "").trim() || "blank"}}`;
          const createdDate = String(row["Created (UTC)"] || "").slice(0, 10);
          if (!createdDate) return;
          const currentCutoff = cutoffDate < clientConfig.currentDate ? cutoffDate : clientConfig.currentDate;
          const ageDays = Math.floor((new Date(currentCutoff + "T00:00:00Z") - new Date(createdDate + "T00:00:00Z")) / 86400000);
          if (ageDays < 0) return;
          let firstCharge = firstChargeLookup.get(key);
          if (firstCharge == null) {{
            firstCharge = parseAmount(row["Total Spend"]) > 0 ? parseAmount(row["Total Spend"]) : 0;
          }}
          let targetPaymentCount = 1;
          const planId = String(row.Plan || "").trim();
          if (planIsMonthly(planId, catalog)) {{
            if (ageDays >= 60 && paymentCount >= 3) targetPaymentCount = 3;
            else if (ageDays >= 30 && paymentCount >= 2) targetPaymentCount = 2;
          }} else if (planIsQuarterly(planId, catalog)) {{
            if (ageDays >= 90 && paymentCount >= 2) targetPaymentCount = 2;
          }}
          revenue += cumulativeChargeLookup.get(`${{key}}|||${{targetPaymentCount}}`) ?? firstCharge;
        }});
      return revenue;
    }}

    function cohortDisputeLosses(rows) {{
      return rows.reduce((sum, row) => sum + parseAmount(row["Dispute Losses"]), 0);
    }}

    function cohortRefundedVolume(rows) {{
      return rows.reduce((sum, row) => sum + parseAmount(row["Refunded Volume"]), 0);
    }}

    function netActualRevenueThroughRows(rows, paymentsRows, cutoffDate, firstChargeLookup, cumulativeChargeLookup, catalog) {{
      return actualRevenueThroughRows(rows, paymentsRows, cutoffDate, firstChargeLookup, cumulativeChargeLookup, catalog) - cohortDisputeLosses(rows);
    }}

    function computeTransactionUpsellsForRows(rows, paymentsRows) {{
      const cohortDays = new Map();
      rows.forEach((row) => {{
        const id = String(row.id || "").trim();
        const created = String(row["Created (UTC)"] || "").slice(0, 10);
        if (id && created) cohortDays.set(id, created);
      }});
      const byCustomer = new Map();
      paymentsRows.forEach((payment) => {{
        const customerId = String(payment["Customer ID"] || "").trim();
        if (!cohortDays.has(customerId) || payment.Status !== "Paid") return;
        if (!byCustomer.has(customerId)) byCustomer.set(customerId, []);
        byCustomer.get(customerId).push(payment);
      }});
      let count = 0;
      let billingCount = 0;
      let revenue = 0;
      byCustomer.forEach((items, customerId) => {{
        const cohortDay = cohortDays.get(customerId);
        const firstDayItems = items
          .slice()
          .sort((a, b) => String(a["Created date (UTC)"]).localeCompare(String(b["Created date (UTC)"])))
          .filter((row) => String(row["Created date (UTC)"] || "").slice(0, 10) === cohortDay);
        const invoiceItems = firstDayItems.filter((row) => String(row.Description || "").trim() === "Payment for Invoice");
        if (!invoiceItems.length) return;
        count += 1;
        billingCount += invoiceItems.length;
        invoiceItems.forEach((row) => {{
          revenue += parseAmount(row["Converted Amount"]) - parseAmount(row["Converted Amount Refunded"]);
        }});
      }});
      return {{ count, billingCount, revenue }};
    }}

    function buildSpendMaps(spendRows) {{
      if (!spendRows || !spendRows.length) {{
        return clientConfig.defaultSpendMaps;
      }}
      const monthly = {{}};
      const weekly = {{}};
      spendRows.forEach((row) => {{
        const values = Object.values(row);
        const dayRaw = String(row.Day || values[0] || "").trim().slice(0, 10);
        if (!dayRaw) return;
        const amountRaw = row["Amount spent (EUR)"] ?? values[1];
        const amount = parseCompactEuroAmount(amountRaw);
        const monthKey = dayRaw.slice(0, 7);
        if (!clientConfig.targetMonths.includes(monthKey)) return;
        monthly[monthKey] = (monthly[monthKey] || 0) + amount;
        const [weekStart] = anchoredWeekRange(dayRaw);
        const weekKey = toIsoDate(weekStart);
        weekly[weekKey] = (weekly[weekKey] || 0) + amount;
      }});
      return {{ monthly, weekly }};
    }}

    function formatMoney(amount) {{
      return `€${{Math.round(amount).toLocaleString("en-US")}}`;
    }}

    function formatMoney2(amount) {{
      return amount ? `€${{amount.toFixed(2)}}` : "—";
    }}

    function buildViewPayload(metrics) {{
      return {{
        chart: metrics.map((item) => ({{
          cohort: item.chartLabel,
          paidRate: Number(item.paidRate.toFixed(1)),
          activeRate: Number(item.activeRate.toFixed(1)),
          repeatRate: Number(item.repeatRate.toFixed(1)),
          threePlusRate: Number(item.threePlusRate.toFixed(1)),
        }})),
        rows: metrics.map((item) => ({{
          cohort: item.cohort,
          spend: formatMoney(item.spend),
          newCustomers: item.customersWithPayment,
          cpa: item.customersWithPayment ? formatMoney(item.spend / item.customersWithPayment) : "—",
          upsell: formatMoney2(item.upsellRevenue),
          upsellCount: item.upsellCount || "—",
          avg: item.upsellCount ? `€${{item.upsellAvg.toFixed(2)}}` : "—",
          d0Rev: formatMoney(item.d0Revenue),
          revToDate: formatMoney(item.netRevenue),
          refunds: formatMoney(item.refundedVolume),
          disputeLosses: formatMoney(item.disputeLosses),
          d0Roas: item.spend ? `${{(item.d0Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          roasToDate: item.spend ? `${{(item.netRevenue / item.spend * 100).toFixed(1)}}%` : "—",
          m1: item.spend ? `${{(item.m1Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m2: item.spend ? `${{(item.m2Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m3: item.spend ? `${{(item.m3Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m6: item.spend ? `${{(item.m6Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m9: item.spend ? `${{(item.m9Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m12: item.spend ? `${{(item.m12Revenue / item.spend * 100).toFixed(1)}}%` : "—",
          m1Projected: !milestoneClosedFromEnd(item.cohortEndDate, 1),
          m2Projected: !milestoneClosedFromEnd(item.cohortEndDate, 2),
          m3Projected: !milestoneClosedFromEnd(item.cohortEndDate, 3),
          m6Projected: true,
          m9Projected: true,
          m12Projected: true,
        }})),
      }};
    }}

    function buildMetrics(rows, cohortKey, cohortLabel, chartLabel, cohortEndDate, spend, paymentsRows, catalog, lookups, subscriptionCreationInfo, firstSubscriptionUpdateAmounts) {{
      const paymentCounts = rows.map((row) => parseIntValue(row["Payment Count"]));
      const customers = rows.length;
      const paidCustomers = rows.filter((row) => parseAmount(row["Total Spend"]) > 0).length;
      const customersWithPayment = rows.filter((row) => parseIntValue(row["Payment Count"]) >= 1).length;
      const projected = {{ m1: 0, m2: 0, m3: 0, m6: 0, m9: 0, m12: 0 }};
      const preparedRows = [];

      rows.forEach((row) => {{
        if (parseIntValue(row["Payment Count"]) < 1) return;
        const customerId = String(row.id || "").trim();
        const monthKey = String(row["Created (UTC)"] || "").slice(0, 7);
        const firstSubscription = subscriptionCreationInfo.get(customerId);
        const family = starterFamilyForRow(row, monthKey, firstSubscription, catalog);
        const fullPriceEur = projectionPriceForRow(row, monthKey, family, firstSubscription, firstSubscriptionUpdateAmounts.get(customerId), catalog);
        if (!family) return;
        preparedRows.push({{ row, customerId, family, fullPriceEur }});
      }});

      const familyCustomers = {{}};
      preparedRows.forEach((item) => {{
        if (!familyCustomers[item.family]) familyCustomers[item.family] = [];
        familyCustomers[item.family].push(item.customerId);
      }});

      const milestoneCounts = {{}};
      [1, 2, 3, 6, 9, 12].forEach((monthNumber) => {{
        milestoneCounts[monthNumber] = subscriptionBillingCountsThrough(rows, paymentsRows, toIsoDate(addMonths(cohortEndDate, monthNumber)));
      }});

      const actualRetentionRates = {{}};
      Object.entries(familyCustomers).forEach(([family, customerIds]) => {{
        if (!customerIds.length) return;
        actualRetentionRates[family] = {{}};
        benchmarkMilestonesForFamily(family).forEach((monthNumber) => {{
          if (!milestoneClosedFromEnd(cohortEndDate, monthNumber)) return;
          const requiredCount = requiredPaymentCount(family, monthNumber);
          const numerator = customerIds.filter((customerId) => (milestoneCounts[monthNumber].get(customerId) || 0) >= requiredCount).length;
          actualRetentionRates[family][monthNumber] = numerator / customerIds.length;
        }});
      }});

      preparedRows.forEach((item) => {{
        if (!item.fullPriceEur) return;
        projected.m1 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 1, actualRetentionRates);
        projected.m2 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 2, actualRetentionRates);
        projected.m3 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 3, actualRetentionRates);
        projected.m6 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 6, actualRetentionRates);
        projected.m9 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 9, actualRetentionRates);
        projected.m12 += projectedPopTailRevenue(item.family, item.fullPriceEur, cohortEndDate, 12, actualRetentionRates);
      }});

      const d0Revenue = actualRevenueThroughRows(rows, paymentsRows, toIsoDate(cohortEndDate), lookups.firstCharge, lookups.cumulative, catalog);
      const blendedRevenue = (targetMonth) => {{
        if (milestoneClosedFromEnd(cohortEndDate, targetMonth)) {{
          return netActualRevenueThroughRows(rows, paymentsRows, toIsoDate(addMonths(cohortEndDate, targetMonth)), lookups.firstCharge, lookups.cumulative, catalog);
        }}
        const closedPoints = [1, 2, 3, 6, 9, 12].filter((month) => month < targetMonth && milestoneClosedFromEnd(cohortEndDate, month));
        const baseRevenue = closedPoints.length
          ? netActualRevenueThroughRows(rows, paymentsRows, toIsoDate(addMonths(cohortEndDate, Math.max(...closedPoints))), lookups.firstCharge, lookups.cumulative, catalog)
          : d0Revenue;
        return baseRevenue + projected[`m${{targetMonth}}`];
      }};
      const m1Revenue = blendedRevenue(1);
      const m2Revenue = blendedRevenue(2);
      const m3Revenue = blendedRevenue(3);
      const m6Revenue = blendedRevenue(6);
      const m9Revenue = blendedRevenue(9);
      const m12Revenue = blendedRevenue(12);
      const refundedVolume = cohortRefundedVolume(rows);
      const disputeLosses = cohortDisputeLosses(rows);
      const netRevenue = actualRevenueThroughRows(rows, paymentsRows, clientConfig.currentDate, lookups.firstCharge, lookups.cumulative, catalog) - disputeLosses;
      const upsell = computeTransactionUpsellsForRows(rows, paymentsRows);
      const activeCustomers = rows.filter((row) => String(row.Status || "") === "active").length;
      const repeatCustomers = paymentCounts.filter((count) => count >= 2).length;
      const threePlusCustomers = paymentCounts.filter((count) => count >= 3).length;

      return {{
        cohort: cohortLabel,
        chartLabel,
        cohortEndDate,
        spend,
        customersWithPayment,
        d0Revenue,
        m1Revenue,
        m2Revenue,
        m3Revenue,
        m6Revenue,
        m9Revenue,
        m12Revenue,
        netRevenue,
        refundedVolume,
        disputeLosses,
        upsellCount: upsell.count,
        upsellAvg: upsell.count ? upsell.revenue / upsell.count : 0,
        upsellRevenue: upsell.revenue,
        paidRate: customers ? (paidCustomers / customers) * 100 : 0,
        activeRate: customers ? (activeCustomers / customers) * 100 : 0,
        repeatRate: customers ? (repeatCustomers / customers) * 100 : 0,
        threePlusRate: customers ? (threePlusCustomers / customers) * 100 : 0,
      }};
    }}

    function buildDashboardPayload(customersRows, paymentsRows, spendRows, catalog) {{
      const groupedMonthly = new Map();
      const groupedWeekly = new Map();
      const weeklyRanges = new Map();
      customersRows.forEach((row) => {{
        const created = String(row["Created (UTC)"] || "");
        const monthKey = created.slice(0, 7);
        if (!clientConfig.targetMonths.includes(monthKey)) return;
        if (!groupedMonthly.has(monthKey)) groupedMonthly.set(monthKey, []);
        groupedMonthly.get(monthKey).push(row);
        const day = created.slice(0, 10);
        const [weekStart, weekEnd] = anchoredWeekRange(day);
        const weekKey = toIsoDate(weekStart);
        if (!groupedWeekly.has(weekKey)) groupedWeekly.set(weekKey, []);
        groupedWeekly.get(weekKey).push(row);
        weeklyRanges.set(weekKey, [weekStart, weekEnd]);
      }});

      const spendMaps = buildSpendMaps(spendRows);
      const firstChargeLookup = loadFirstChargeLookup(customersRows);
      const cumulativeChargeLookup = loadCumulativeChargeLookup(customersRows);
      const subscriptionCreationInfo = loadSubscriptionCreationInfo(paymentsRows);
      const firstSubscriptionUpdateAmounts = loadFirstSubscriptionUpdateAmounts(paymentsRows);
      const lookups = {{ firstCharge: firstChargeLookup, cumulative: cumulativeChargeLookup }};

      const monthlyMetrics = clientConfig.targetMonths.map((monthKey) => buildMetrics(
        groupedMonthly.get(monthKey) || [],
        monthKey,
        formatMonth(monthKey),
        formatMonth(monthKey).replace(" 2026", ""),
        monthEnd(monthKey),
        spendMaps.monthly[monthKey] || 0,
        paymentsRows,
        catalog,
        lookups,
        subscriptionCreationInfo,
        firstSubscriptionUpdateAmounts,
      ));

      const weeklyMetrics = Array.from(groupedWeekly.keys())
        .sort((a, b) => a.localeCompare(b))
        .map((weekKey) => {{
          const [weekStart, weekEnd] = weeklyRanges.get(weekKey);
          return buildMetrics(
            groupedWeekly.get(weekKey) || [],
            weekKey,
            `${{formatShortDate(weekStart)}}-${{formatShortDate(weekEnd)}}`,
            formatWeekChartLabel(weekStart),
            weekEnd,
            spendMaps.weekly[weekKey] || 0,
            paymentsRows,
            catalog,
            lookups,
            subscriptionCreationInfo,
            firstSubscriptionUpdateAmounts,
          );
        }});

      const weeklyRetention = Array.from(groupedWeekly.keys())
        .sort((a, b) => a.localeCompare(b))
        .map((weekKey) => {{
          const cohortRows = groupedWeekly.get(weekKey) || [];
          const starterIds = [];
          cohortRows.forEach((row) => {{
            const customerId = String(row.id || "").trim();
            if (!customerId || customerId.startsWith("gcus_")) return;
            if (parseIntValue(row["Payment Count"]) < 1) return;
            const monthKey = String(row["Created (UTC)"] || "").slice(0, 7);
            const firstSubscription = subscriptionCreationInfo.get(customerId);
            const family = starterFamilyForRow(row, monthKey, firstSubscription, catalog);
            if (family === "monthly") {{
              starterIds.push(customerId);
            }}
          }});
          const uniqueStarterIds = Array.from(new Set(starterIds)).sort();
          const denominator = uniqueStarterIds.length;
          const milestoneCounts = subscriptionBillingCountsThrough(cohortRows, paymentsRows, clientConfig.currentDate);
          const m1 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 2).length;
          const m2 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 3).length;
          const m3 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 4).length;
          const [weekStart, weekEnd] = weeklyRanges.get(weekKey);
          return {{
            cohort: `${{formatShortDate(weekStart)}}-${{formatShortDate(weekEnd)}}`,
            monthlyStarters: denominator,
            m1Count: m1,
            m1Rate: denominator ? (m1 / denominator) * 100 : 0,
            m1Closed: milestoneClosedFromEnd(weekEnd, 1),
            m2Count: m2,
            m2Rate: denominator ? (m2 / denominator) * 100 : 0,
            m2Closed: milestoneClosedFromEnd(weekEnd, 2),
            m3Count: m3,
            m3Rate: denominator ? (m3 / denominator) * 100 : 0,
            m3Closed: milestoneClosedFromEnd(weekEnd, 3),
          }};
        }});

      const quarterlyRetention = Array.from(groupedWeekly.keys())
        .sort((a, b) => a.localeCompare(b))
        .map((weekKey) => {{
          const cohortRows = groupedWeekly.get(weekKey) || [];
          const starterIds = [];
          cohortRows.forEach((row) => {{
            const customerId = String(row.id || "").trim();
            if (!customerId || customerId.startsWith("gcus_")) return;
            if (parseIntValue(row["Payment Count"]) < 1) return;
            const monthKey = String(row["Created (UTC)"] || "").slice(0, 7);
            const firstSubscription = subscriptionCreationInfo.get(customerId);
            const family = starterFamilyForRow(row, monthKey, firstSubscription, catalog);
            if (family === "quarterly") {{
              starterIds.push(customerId);
            }}
          }});
          const uniqueStarterIds = Array.from(new Set(starterIds)).sort();
          const denominator = uniqueStarterIds.length;
          const milestoneCounts = subscriptionBillingCountsThrough(cohortRows, paymentsRows, clientConfig.currentDate);
          const m3 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 2).length;
          const m6 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 3).length;
          const m9 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 4).length;
          const m12 = uniqueStarterIds.filter((customerId) => (milestoneCounts.get(customerId) || 0) >= 5).length;
          const [weekStart, weekEnd] = weeklyRanges.get(weekKey);
          return {{
            cohort: `${{formatShortDate(weekStart)}}-${{formatShortDate(weekEnd)}}`,
            quarterlyStarters: denominator,
            m3Count: m3,
            m3Rate: denominator ? (m3 / denominator) * 100 : 0,
            m3Closed: milestoneClosedFromEnd(weekEnd, 3),
            m6Count: m6,
            m6Rate: denominator ? (m6 / denominator) * 100 : 0,
            m6Closed: milestoneClosedFromEnd(weekEnd, 6),
            m9Count: m9,
            m9Rate: denominator ? (m9 / denominator) * 100 : 0,
            m9Closed: milestoneClosedFromEnd(weekEnd, 9),
            m12Count: m12,
            m12Rate: denominator ? (m12 / denominator) * 100 : 0,
            m12Closed: milestoneClosedFromEnd(weekEnd, 12),
          }};
        }});

      const cohortRows = customersRows.filter((row) => clientConfig.targetMonths.includes(String(row["Created (UTC)"] || "").slice(0, 7)));
      const totals = {{
        customers: cohortRows.length,
        paid_customers: cohortRows.filter((row) => parseAmount(row["Total Spend"]) > 0).length,
        active_customers: cohortRows.filter((row) => String(row.Status || "") === "active").length,
        repeat_customers: cohortRows.filter((row) => parseIntValue(row["Payment Count"]) >= 2).length,
      }};

      return {{
        monthly: buildViewPayload(monthlyMetrics),
        weekly: buildViewPayload(weeklyMetrics),
        weeklyRetention,
        quarterlyRetention,
        totals,
      }};
    }}

    function renderSummary() {{
      const totals = state.payload.totals || {{}};
      document.getElementById("summary-customers").textContent = Number(totals.customers || 0).toLocaleString("en-US");
      document.getElementById("summary-paid").textContent = Number(totals.paid_customers || 0).toLocaleString("en-US");
      document.getElementById("summary-active").textContent = Number(totals.active_customers || 0).toLocaleString("en-US");
      document.getElementById("summary-repeat").textContent = Number(totals.repeat_customers || 0).toLocaleString("en-US");
    }}

    function renderTable() {{
      const tbody = document.getElementById("tbody");
      const view = state.payload[activeView];
      const milestoneCell = (value, projected) => `<td class="${{projected ? 'projected' : 'actual'}}">${{value}}</td>`;
      const cohortCell = (label) => {{
        if (activeView !== "weekly") {{
          return `<td class="cohort">${{label}}</td>`;
        }}
        return `<td class="cohort"><span class="cohort-range">${{label}}</span></td>`;
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
          ${{milestoneCell(row.m9, row.m9Projected)}}
          ${{milestoneCell(row.m12, row.m12Projected)}}
        </tr>
      `).join("");
    }}

    function renderRetentionTable() {{
      const tbody = document.getElementById("retention-tbody");
      const rows = state.payload.weeklyRetention || [];
      tbody.innerHTML = rows.map((row) => `
        <tr>
          <td class="cohort"><span class="cohort-range">${{row.cohort}}</span></td>
          <td>${{Number(row.monthlyStarters ?? row.monthly_starters ?? 0).toLocaleString()}}</td>
          <td>
            <div class="retention-metric ${{(row.m1Closed ?? row.m1_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m1Rate ?? row.m1_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m1Count ?? row.m1_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
          <td>
            <div class="retention-metric ${{(row.m2Closed ?? row.m2_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m2Rate ?? row.m2_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m2Count ?? row.m2_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
          <td>
            <div class="retention-metric ${{(row.m3Closed ?? row.m3_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m3Rate ?? row.m3_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m3Count ?? row.m3_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
        </tr>
      `).join("");
    }}

    function renderQuarterlyRetentionTable() {{
      const tbody = document.getElementById("quarterly-retention-tbody");
      const rows = state.payload.quarterlyRetention || [];
      tbody.innerHTML = rows.map((row) => `
        <tr>
          <td class="cohort"><span class="cohort-range">${{row.cohort}}</span></td>
          <td>${{Number(row.quarterlyStarters ?? row.quarterly_starters ?? 0).toLocaleString()}}</td>
          <td>
            <div class="retention-metric ${{(row.m3Closed ?? row.m3_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m3Rate ?? row.m3_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m3Count ?? row.m3_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
          <td>
            <div class="retention-metric ${{(row.m6Closed ?? row.m6_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m6Rate ?? row.m6_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m6Count ?? row.m6_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
          <td>
            <div class="retention-metric ${{(row.m9Closed ?? row.m9_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m9Rate ?? row.m9_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m9Count ?? row.m9_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
          <td>
            <div class="retention-metric ${{(row.m12Closed ?? row.m12_closed) ? 'closed' : 'open'}}">
              <span class="rate">${{Number(row.m12Rate ?? row.m12_rate ?? 0).toFixed(1)}}%</span>
              <span class="count">${{Number(row.m12Count ?? row.m12_count ?? 0).toLocaleString()}}</span>
            </div>
          </td>
        </tr>
      `).join("");
    }}

    function renderChart() {{
      const svg = document.getElementById("chart");
      if (activeView === "weekly-retention" || activeView === "quarterly-retention") {{
        svg.innerHTML = "";
        return;
      }}
      const view = state.payload[activeView];
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
      const isRetentionView = nextView === "weekly-retention" || nextView === "quarterly-retention";
      document.getElementById("retention-card").style.display = isRetentionView ? "block" : "none";
      document.getElementById("retention-table-monthly").style.display = nextView === "weekly-retention" ? "table" : "none";
      document.getElementById("retention-table-quarterly").style.display = nextView === "quarterly-retention" ? "table" : "none";
      document.querySelector(".chart-card").style.display = isRetentionView ? "none" : "block";
      document.getElementById("main-table-card").style.display = isRetentionView ? "none" : "block";
      document.getElementById("section-note").textContent =
        nextView === "monthly"
          ? "Grouped bar chart for the four key rates available in the export."
          : nextView === "weekly"
            ? "Weekly cohort comparison using the same rate definitions and milestone table."
            : nextView === "weekly-retention"
              ? "Monthly-plan starter retention by weekly cohort, shown as renewals and retention rates."
              : "Quarterly-plan starter retention by weekly cohort, shown as renewals and retention rates.";
      if (nextView === "weekly-retention") {{
        renderRetentionTable();
      }} else if (nextView === "quarterly-retention") {{
        renderQuarterlyRetentionTable();
      }} else {{
        renderTable();
        renderChart();
      }}
    }}

    function updateUploadControls() {{
      const ready = Boolean(uploadFiles.customers?.text && uploadFiles.payments?.text);
      document.getElementById("apply-uploads").disabled = !ready;
      document.getElementById("reset-uploads").hidden = !state.uploaded;
      document.getElementById("publish-uploads").disabled = !state.uploaded;
    }}

    async function publishImportedData() {{
      if (!state.uploaded) {{
        return;
      }}
      const status = document.getElementById("upload-status");
      if (uploadFiles.customers?.text && uploadFiles.payments?.text) {{
        await recalculateFromUploads();
      }}
      savePublishedPayload(state.payload);
      status.innerHTML = "<strong>Published imported data.</strong> This imported dashboard is now saved in your browser and will be restored after reload.";
    }}

    async function recalculateFromUploads() {{
      if (!uploadFiles.customers?.text || !uploadFiles.payments?.text) {{
        return;
      }}
      const status = document.getElementById("upload-status");
      status.innerHTML = "<strong>Recalculating…</strong> Parsing uploaded exports and rebuilding cohort views in your browser.";
      const [customersText, paymentsText, spendText, pricesText] = await Promise.all([
        Promise.resolve(uploadFiles.customers.text),
        Promise.resolve(uploadFiles.payments.text),
        Promise.resolve(uploadFiles.spend?.text || ""),
        Promise.resolve(uploadFiles.prices?.text || ""),
      ]);
      const customersRows = parseDelimited(customersText);
      const paymentsRows = parseDelimited(paymentsText);
      const spendRows = spendText ? parseDelimited(spendText) : [];
      const uploadedCatalogRows = pricesText ? parseDelimited(pricesText) : [];
      const catalog = uploadedCatalogRows.length
        ? Object.fromEntries(uploadedCatalogRows.map((row) => [row["Price ID"], {{
            amount: parseAmount(row.Amount),
            currency: row.Currency || "",
            interval: row.Interval || "",
            interval_count: row["Interval Count"] || "",
            product_name: row["Product Name"] || "",
          }}]))
        : clientConfig.priceCatalog;
      state.payload = buildDashboardPayload(customersRows, paymentsRows, spendRows, catalog);
      state.uploaded = true;
      status.innerHTML = `<strong>Loaded uploaded exports.</strong> Customers: ${{customersRows.length.toLocaleString()}} rows. Payments: ${{paymentsRows.length.toLocaleString()}} rows${{spendRows.length ? `. Spend: ${{spendRows.length.toLocaleString()}} rows.` : "."}}`;
      updateUploadControls();
      renderSummary();
      setView(activeView);
    }}

    document.querySelectorAll(".tab-button").forEach((button) => {{
      button.addEventListener("click", () => setView(button.dataset.view));
    }});

    document.getElementById("reset-uploads").addEventListener("click", () => {{
      state.payload = defaultPayload;
      state.uploaded = false;
      clearPublishedPayload();
      Object.keys(uploadFiles).forEach((key) => {{
        uploadFiles[key] = null;
      }});
      ["customers-upload", "payments-upload", "spend-upload", "prices-upload"].forEach((id) => {{
        document.getElementById(id).value = "";
      }});
      document.getElementById("upload-status").innerHTML = "<strong>Using embedded exports.</strong> Upload customers and payments CSVs to recalculate monthly and weekly cohorts.";
      updateUploadControls();
      renderSummary();
      renderTable();
      renderChart();
    }});

    [["customers-upload", "customers"], ["payments-upload", "payments"], ["spend-upload", "spend"], ["prices-upload", "prices"]].forEach(([id, key]) => {{
      document.getElementById(id).addEventListener("change", async (event) => {{
        const file = event.target.files && event.target.files[0] ? event.target.files[0] : null;
        if (!file) {{
          uploadFiles[key] = null;
          updateUploadControls();
          return;
        }}
        try {{
          const text = await file.text();
          uploadFiles[key] = {{
            name: file.name,
            text,
          }};
          document.getElementById("upload-status").innerHTML = `<strong>Loaded ${{file.name}}.</strong> Ready to recalculate with uploaded exports.`;
        }} catch (error) {{
          uploadFiles[key] = null;
          document.getElementById("upload-status").innerHTML = `<strong>Upload failed.</strong> ${{error.message}}`;
        }}
        updateUploadControls();
      }});
    }});

    document.getElementById("apply-uploads").addEventListener("click", () => {{
      recalculateFromUploads().catch((error) => {{
        document.getElementById("upload-status").innerHTML = `<strong>Upload failed.</strong> ${{error.message}}`;
      }});
    }});

    document.getElementById("publish-uploads").addEventListener("click", () => {{
      publishImportedData().catch((error) => {{
        document.getElementById("upload-status").innerHTML = `<strong>Publish failed.</strong> ${{error.message}}`;
      }});
    }});

    const persistedPayload = loadPublishedPayload();
    if (persistedPayload) {{
      state.payload = persistedPayload;
      state.uploaded = true;
      document.getElementById("upload-status").innerHTML = "<strong>Using published imported data.</strong> The last published imported dashboard was restored from this browser.";
    }}

    updateUploadControls();
    renderSummary();
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
    weekly_retention_rows = build_weekly_plan_retention_rows(dict(weekly_grouped), weekly_ranges)
    weekly_quarterly_retention_rows = build_weekly_quarterly_retention_rows(dict(weekly_grouped), weekly_ranges)

    html = render_html(monthly_metrics, weekly_metrics, weekly_retention_rows, weekly_quarterly_retention_rows)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    PAGES_HTML.parent.mkdir(parents=True, exist_ok=True)
    PAGES_HTML.write_text(html, encoding="utf-8")
    NOJEKYLL_FILE.write_text("", encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML}")
    print(f"Wrote {PAGES_HTML}")


if __name__ == "__main__":
    main()
