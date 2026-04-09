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
OUTPUT_HTML = Path("/Users/annashiman/Documents/Playground/cohort_dashboard_jan_apr.html")
PAGES_HTML = Path("/Users/annashiman/Documents/Playground/docs/index.html")
NOJEKYLL_FILE = Path("/Users/annashiman/Documents/Playground/docs/.nojekyll")
TARGET_MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
SCREENSHOT_SPEND_USD = {
    "2026-01": 64378,
    "2026-02": 64867,
    "2026-03": 67835,
    "2026-04": 14726,
}
CURRENT_DATE = date(2026, 4, 8)
RETENTION_RATE_BENCHMARKS = {
    "monthly": {1: 0.55, 2: 0.38, 3: 0.20, 4: 0.14, 5: 0.07, 6: 0.05, 7: 0.03},
    "quarterly": {3: 0.50, 6: 0.28, 9: 0.18, 12: 0.12},
    "annual": {12: 0.30},
}
PLAN_REVENUE_SCHEDULES = {
    "monthly_19_60": {"family": "monthly", "intro": 19.60, "months": {1: 21.80, 2: 15.07, 3: 8.13, 4: 5.60, 5: 2.80, 6: 2.00, 7: 1.20}},
    "monthly_15_60": {"family": "monthly", "intro": 15.60, "months": {1: 21.80, 2: 15.07, 3: 8.13, 4: 5.60, 5: 2.80, 6: 2.00, 7: 1.20}},
    "quarterly_32_00": {"family": "quarterly", "intro": 32.00, "months": {3: 40.00, 6: 22.40, 9: 14.40, 12: 9.60}},
    "quarterly_23_99": {"family": "quarterly", "intro": 23.99, "months": {3: 40.00, 4: 0.00, 5: 0.00, 6: 22.40, 7: 0.00, 8: 0.00, 9: 14.40, 10: 0.00, 11: 0.00, 12: 9.60}},
    "annual_58_80": {"family": "annual", "intro": 58.80, "months": {12: 36.00}},
    "annual_46_80": {"family": "annual", "intro": 46.80, "months": {12: 36.00}},
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
    customers: int
    paid_customers: int
    customers_with_payment: int
    d0_revenue: float
    m1_revenue: float
    m3_revenue: float
    m6_revenue: float
    m12_revenue: float
    net_revenue: float
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


def milestone_is_closed(month_key: str, milestone_months: int) -> bool:
    return CURRENT_DATE >= add_months(cohort_month_end(month_key), milestone_months)


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


def infer_blank_usd_projection(total_spend: float) -> tuple[str, float] | None:
    candidates = [
        ("monthly", 39.99, 16.80, 34.27),
        ("monthly", 39.99, 13.37, 34.27),
        ("quarterly", 79.99, 27.42, 57.92),
        ("quarterly", 79.99, 20.57, 57.92),
        ("annual", 119.99, 50.37, 0.0),
        ("annual", 119.99, 40.09, 0.0),
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


def projection_spec_for_row(row: dict[str, str], month_key: str) -> tuple[str, float] | None:
    plan_id = (row.get("Plan") or "").strip()
    currency = ((row.get("Currency") or "").strip() or "").lower()
    total_spend = parse_amount(row.get("Total Spend", "0"))

    if plan_id:
        family = classify_plan_family(plan_id, 0.0, currency)
        full_price_usd = plan_full_price_usd(plan_id, month_key)
        if family and full_price_usd:
            return family, full_price_usd
        return None

    if currency == "usd":
        return infer_blank_usd_projection(total_spend)

    return None


def projected_family_revenue(family: str, full_price_usd: float, milestone_months: int) -> float:
    return sum(
        full_price_usd * retention
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


def choose_plan_schedule(family: str, first_charge_usd: float) -> str | None:
    options = [key for key, meta in PLAN_REVENUE_SCHEDULES.items() if meta["family"] == family]
    if not options:
        return None
    best_key = min(options, key=lambda key: abs(first_charge_usd - PLAN_REVENUE_SCHEDULES[key]["intro"]))
    if abs(first_charge_usd - PLAN_REVENUE_SCHEDULES[best_key]["intro"]) <= 4.0:
        return best_key
    return None


def projected_benchmark_revenue(
    family: str,
    full_price_usd: float,
    schedule_key: str | None,
    milestone_months: int,
) -> float:
    if schedule_key:
        schedule = PLAN_REVENUE_SCHEDULES[schedule_key]
        return sum(
            amount
            for month_number, amount in schedule["months"].items()
            if month_number <= milestone_months
        )
    return sum(
        full_price_usd * retention
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


def summarize_revenue_usd(rows: list[dict[str, str]], month_key: str) -> float:
    total = 0.0
    for row in rows:
        total += convert_to_usd(
            parse_amount(row.get("Total Spend", "0")),
            row.get("Currency", ""),
            month_key,
        )
    return total


def summarize_net_revenue_usd(rows: list[dict[str, str]], month_key: str) -> float:
    total = 0.0
    for row in rows:
        gross = parse_amount(row.get("Total Spend", "0"))
        refunds = parse_amount(row.get("Refunded Volume", "0"))
        dispute_losses = parse_amount(row.get("Dispute Losses", "0"))
        total += convert_to_usd(gross - refunds - dispute_losses, row.get("Currency", ""), month_key)
    return total


def format_usd(amount: float) -> str:
    return f"${amount:,.0f}"


def format_cpa(amount: float) -> str:
    return f"${amount:,.0f}"


def format_money_2(amount: float) -> str:
    return "—" if amount == 0 else f"${amount:,.2f}"


def describe_top_plan(rows: list[dict[str, str]]) -> str:
    plans = Counter((row.get("Plan") or "(blank)").strip() or "(blank)" for row in rows)
    top_plan, top_count = plans.most_common(1)[0]
    if top_plan == "(blank)":
        return f"Blank plan ids ({top_count})"
    return f"{top_plan} ({top_count})"


def build_metrics(rows: list[dict[str, str]], month_key: str) -> CohortMetrics:
    payment_counts = [parse_int(row.get("Payment Count", "")) for row in rows]
    spends = [parse_amount(row.get("Total Spend", "0")) for row in rows]
    customers = len(rows)

    paid_customers = sum(spend > 0 for spend in spends)
    customers_with_payment = sum(count >= 1 for count in payment_counts)
    first_charge_lookup = load_first_charge_lookup()
    cumulative_charge_lookup = load_cumulative_charge_lookup()
    d0_revenue_base = 0.0
    m1_revenue_actual = 0.0
    m3_revenue_actual = 0.0
    m6_revenue_actual = 0.0
    m12_revenue_actual = 0.0
    projected = {"m1": 0.0, "m3": 0.0, "m6": 0.0, "m12": 0.0}
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
        first_charge = first_charge_lookup.get(key)
        if first_charge is None:
            first_charge = parse_amount(row.get("Total Spend", "0")) if parse_amount(row.get("Total Spend", "0")) > 0 else 0.0
        created_date = datetime.strptime((row.get("Created (UTC)") or "")[:10], "%Y-%m-%d").date()
        age_days = (CURRENT_DATE - created_date).days
        projection_spec = projection_spec_for_row(row, month_key)
        first_charge_usd = convert_to_usd(first_charge, key[1], month_key)
        schedule_key = None
        if projection_spec:
            family, _ = projection_spec
            schedule_key = choose_plan_schedule(family, first_charge_usd)
        prepared_rows.append((row, payment_count, key, plan_id, first_charge, first_charge_usd, age_days, projection_spec, schedule_key))

    for row, payment_count, key, plan_id, first_charge, first_charge_usd, age_days, projection_spec, schedule_key in prepared_rows:
        d0_revenue_base += first_charge_usd
        if projection_spec:
            family, full_price_usd = projection_spec
            projected["m1"] += projected_benchmark_revenue(family, full_price_usd, schedule_key, 1)
            projected["m3"] += projected_benchmark_revenue(family, full_price_usd, schedule_key, 3)
            projected["m6"] += projected_benchmark_revenue(family, full_price_usd, schedule_key, 6)
            projected["m12"] += projected_benchmark_revenue(family, full_price_usd, schedule_key, 12)

        target_payment_count = 1
        if age_days >= 30 and payment_count >= 2 and plan_is_monthly((row.get("Plan") or "").strip()):
            target_payment_count = 2

        m1_cumulative = cumulative_charge_lookup.get((key[0], key[1], target_payment_count), first_charge)
        m1_revenue_actual += convert_to_usd(m1_cumulative, key[1], month_key)

        m3_target_payment_count = 1
        if plan_is_monthly(plan_id):
            if age_days >= 60 and payment_count >= 3:
                m3_target_payment_count = 3
            elif age_days >= 30 and payment_count >= 2:
                m3_target_payment_count = 2
        elif plan_is_quarterly(plan_id):
            if age_days >= 90 and payment_count >= 2:
                m3_target_payment_count = 2

        m3_cumulative = cumulative_charge_lookup.get((key[0], key[1], m3_target_payment_count), first_charge)
        m3_revenue_actual += convert_to_usd(m3_cumulative, key[1], month_key)

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
        m6_revenue_actual += convert_to_usd(m6_cumulative, key[1], month_key)

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
        m12_revenue_actual += convert_to_usd(m12_cumulative, key[1], month_key)

    transaction_upsell = compute_transaction_upsells().get(
        month_key,
        {"count": 0, "billing_count": 0, "revenue": 0.0},
    )
    upsell_count = int(transaction_upsell["count"])
    upsell_billing_count = int(transaction_upsell["billing_count"])
    upsell_revenue = float(transaction_upsell["revenue"])

    active_customers = sum((row.get("Status") or "") == "active" for row in rows)
    past_due_customers = sum((row.get("Status") or "") == "past_due" for row in rows)
    refunded_customers = sum(parse_amount(row.get("Refunded Volume", "0")) > 0 for row in rows)
    repeat_customers = sum(count >= 2 for count in payment_counts)
    three_plus_customers = sum(count >= 3 for count in payment_counts)
    four_plus_customers = sum(count >= 4 for count in payment_counts)

    d0_revenue = d0_revenue_base + upsell_revenue
    m1_revenue = (m1_revenue_actual + upsell_revenue) if milestone_is_closed(month_key, 1) else (d0_revenue + projected["m1"])
    m3_revenue = (m3_revenue_actual + upsell_revenue) if milestone_is_closed(month_key, 3) else (d0_revenue + projected["m3"])
    m6_revenue = d0_revenue + projected["m6"]
    m12_revenue = d0_revenue + projected["m12"]

    return CohortMetrics(
        month_key=month_key,
        cohort=format_month(month_key),
        customers=customers,
        paid_customers=paid_customers,
        customers_with_payment=customers_with_payment,
        d0_revenue=d0_revenue,
        m1_revenue=m1_revenue,
        m3_revenue=m3_revenue,
        m6_revenue=m6_revenue,
        m12_revenue=m12_revenue,
        net_revenue=summarize_revenue_usd(rows, month_key),
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
        usd_revenue=summarize_revenue_usd(rows, month_key),
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


def render_html(metrics: list[CohortMetrics]) -> str:
    totals = {
        "customers": sum(item.customers for item in metrics),
        "paid_customers": sum(item.paid_customers for item in metrics),
        "active_customers": sum(item.active_customers for item in metrics),
        "repeat_customers": sum(item.repeat_customers for item in metrics),
    }

    chart_data = [
        {
            "cohort": item.cohort.replace(" 2026", ""),
            "paidRate": round(item.paid_rate, 1),
            "activeRate": round(item.active_rate, 1),
            "repeatRate": round(item.repeat_rate, 1),
            "threePlusRate": round(item.three_plus_rate, 1),
        }
        for item in metrics
    ]

    table_rows = [
        {
            "spendUsd": SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue),
            "cohort": item.cohort,
            "spend": format_usd(SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue)),
            "newCustomers": item.customers_with_payment,
            "cpa": format_cpa(
                SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) / item.customers_with_payment
            ) if item.customers_with_payment else "—",
            "subD0": f"{item.paid_customers:,}",
            "upsell": format_money_2(item.upsell_revenue),
            "upsellCount": item.upsell_count if item.upsell_count else "—",
            "avg": f"${item.upsell_avg:.2f}" if item.upsell_count else "—",
            "d0Rev": format_usd(item.d0_revenue),
            "renewals": "—",
            "revToDate": format_usd(item.net_revenue),
            "d0Roas": f"{(item.d0_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "roasToDate": f"{(item.net_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "m1": f"{(item.m1_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "m3": f"{(item.m3_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "m6": f"{(item.m6_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "m12": f"{(item.m12_revenue / SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) * 100):.1f}%" if SCREENSHOT_SPEND_USD.get(item.month_key, item.usd_revenue) else "—",
            "m1Projected": not milestone_is_closed(item.month_key, 1),
            "m3Projected": not milestone_is_closed(item.month_key, 3),
            "m6Projected": True,
            "m12Projected": True,
        }
        for item in metrics
    ]

    payload = json.dumps({"chart": chart_data, "rows": table_rows, "totals": totals}, indent=2)

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
          A screenshot-style cohort dashboard built directly from the unified customer export. It compares January,
          February, March, and April acquisition cohorts across payment depth, active status, and refund incidence.
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
      <div class="section-head">
        <div>
          <h2 class="section-title">Cohort Rate Comparison</h2>
          <p class="section-note">Grouped bar chart for the four key rates available in the export.</p>
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
              <th>Sub D0</th>
              <th>Upsell</th>
              <th>#</th>
              <th>Avg</th>
              <th>D0 Rev</th>
              <th>Renewals</th>
              <th>Rev To Date</th>
              <th>D0 ROAS</th>
              <th>ROAS To Date</th>
              <th>M1</th>
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
      Revenue is normalized to USD using official ECB reference rates. January to March use ECB monthly average rates,
      while April uses the ECB daily average for April 1-8, 2026 as a month-to-date proxy. Rows with blank currency
      codes are treated as USD so they remain included. Upsell follows the earlier heuristic: we infer hidden upsells
      from spend-pattern gaps above each plan/currency baseline, using the recurring add-on gap signatures from the
      price sheet. To keep the signal stable, the hidden-upsell inference is limited to the core non-localized
      subscription signatures.
    </section>
  </div>

  <script>
    const payload = {payload};

    function renderTable() {{
      const tbody = document.getElementById("tbody");
      const milestoneCell = (value, projected) => `<td class="${{projected ? 'projected' : 'actual'}}">${{value}}</td>`;
      tbody.innerHTML = payload.rows.map((row) => `
        <tr>
          <td class="cohort">${{row.cohort}}</td>
          <td class="money">${{row.spend}}</td>
          <td>${{row.newCustomers.toLocaleString()}}</td>
          <td class="muted">${{row.cpa}}</td>
          <td class="metric-cyan">${{row.subD0}}</td>
          <td class="metric-amber">${{row.upsell}}</td>
          <td class="muted">${{row.upsellCount}}</td>
          <td class="muted">${{row.avg}}</td>
          <td>${{row.d0Rev}}</td>
          <td class="metric-violet">${{row.renewals}}</td>
          <td>${{row.revToDate}}</td>
          <td class="metric-amber">${{row.d0Roas}}</td>
          <td class="metric-blue">${{row.roasToDate}}</td>
          ${{milestoneCell(row.m1, row.m1Projected)}}
          ${{milestoneCell(row.m3, row.m3Projected)}}
          ${{milestoneCell(row.m6, row.m6Projected)}}
          ${{milestoneCell(row.m12, row.m12Projected)}}
        </tr>
      `).join("");
    }}

    function renderChart() {{
      const svg = document.getElementById("chart");
      const width = 1320;
      const height = 380;
      const margin = {{ top: 30, right: 20, bottom: 70, left: 72 }};
      const innerWidth = width - margin.left - margin.right;
      const innerHeight = height - margin.top - margin.bottom;
      const maxValue = 100;
      const groups = payload.chart.length;
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

      payload.chart.forEach((row, groupIndex) => {{
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

    renderTable();
    renderChart();
  </script>
</body>
</html>
"""


def main() -> None:
    rows = load_rows()
    grouped: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
      month_key = (row.get("Created (UTC)") or "")[:7]
      if month_key in TARGET_MONTHS:
          grouped[month_key].append(row)

    metrics = [build_metrics(grouped[month_key], month_key) for month_key in TARGET_MONTHS]
    html = render_html(metrics)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    PAGES_HTML.parent.mkdir(parents=True, exist_ok=True)
    PAGES_HTML.write_text(html, encoding="utf-8")
    NOJEKYLL_FILE.write_text("", encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML}")
    print(f"Wrote {PAGES_HTML}")


if __name__ == "__main__":
    main()
