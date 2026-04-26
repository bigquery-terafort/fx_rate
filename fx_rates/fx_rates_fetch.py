"""
Daily FX Rates → BigQuery Pipeline
Fetches USD exchange rates from TWO sources:
  1. frankfurter.app (ECB data, 30 currencies, historical dates)
  2. open.er-api.com (160+ currencies, latest rates, fallback)
Loads into terafort.cross_platform.daily_fx_rates
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
GCP_PROJECT          = os.environ["GCP_PROJECT"].strip()
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
BQ_DATASET           = os.environ.get("BQ_DATASET", "cross_platform")
BQ_TABLE             = os.environ.get("BQ_TABLE", "daily_fx_rates")
LOOKBACK_DAYS        = int(os.environ.get("FX_LOOKBACK_DAYS", "7"))

# All currencies seen across GP Sales, Apple, and other platforms
CURRENCIES = [
    "INR", "IDR", "EUR", "TRY", "MYR", "SAR", "BRL", "PHP",
    "ILS", "LKR", "PLN", "CAD", "PKR", "AED", "MXN", "NGN",
    "GBP", "ZAR", "UAH", "AUD", "THB", "NZD", "COP", "SGD",
    "HUF", "PEN", "KES", "CRC", "MAD", "CLP", "NOK", "VND",
    "RON", "KZT", "CHF", "KRW", "TWD", "JPY", "SEK", "MMK",
    "IQD", "CZK", "TZS", "BDT", "EGP", "PYG", "RSD", "HKD",
    "GEL", "DZD", "QAR", "BGN", "DKK", "MNT", "HNL", "XAF",
    "XOF", "JOD", "OMR", "BHD", "KWD", "GTQ", "BOB", "UYU",
    "ISK", "RWF", "UGX", "GHS", "ETB", "NPR", "LBP", "MZN",
    "CNY", "RUB", "ARS", "AMD", "BAM", "BWP", "DOP", "FJD",
    "GIP", "HTG", "JMD", "KGS", "LAK", "MDL", "MKD", "MVR",
    "MWK", "NAD", "NIO", "PAB", "SOS", "SRD", "TTD", "UZS",
    "XCD", "YER", "ZMW",
]

FRANKFURTER_URL = "https://api.frankfurter.app"
FALLBACK_URL    = "https://open.er-api.com/v6/latest/USD"

# ─── SCHEMA ──────────────────────────────────────────────────────────────────
S = bigquery.SchemaField
SCHEMA = [
    S("rate_date", "DATE"),
    S("currency_code", "STRING"),
    S("usd_rate", "FLOAT64"),
    S("_ingested_at", "TIMESTAMP"),
]

# ─── FALLBACK RATES ─────────────────────────────────────────────────────────
_fallback_cache = None

def fetch_fallback_rates():
    """Fetch latest rates from open.er-api.com (160+ currencies, free, no key)."""
    global _fallback_cache
    if _fallback_cache is not None:
        return _fallback_cache

    log.info("Fetching fallback rates from open.er-api.com...")
    try:
        resp = requests.get(FALLBACK_URL, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            rates = {}
            for currency, rate_per_usd in data.get("rates", {}).items():
                if rate_per_usd and rate_per_usd > 0:
                    # Invert: 1 foreign = X USD
                    rates[currency] = round(1.0 / rate_per_usd, 8)
            log.info(f"  Fallback: {len(rates)} currencies loaded")
            _fallback_cache = rates
            return rates
        else:
            log.warning(f"  Fallback API HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"  Fallback API error: {e}")

    _fallback_cache = {}
    return {}


# ─── FX FETCH ────────────────────────────────────────────────────────────────
def fetch_rates_for_date(rate_date, fallback_rates):
    """
    Fetch rates for a single date:
    1. Try frankfurter.app (ECB, ~30 currencies)
    2. Fill missing currencies from fallback (open.er-api.com latest rates)
    """
    date_str = rate_date.strftime("%Y-%m-%d")
    rows = []
    fetched_currencies = set()

    # Always add USD = 1.0
    rows.append({
        "rate_date": date_str,
        "currency_code": "USD",
        "usd_rate": 1.0,
        "_ingested_at": datetime.utcnow().isoformat(),
    })
    fetched_currencies.add("USD")

    # ── SOURCE 1: Frankfurter (ECB, 30 currencies, historical) ──
    batch_size = 30
    for i in range(0, len(CURRENCIES), batch_size):
        batch = CURRENCIES[i:i + batch_size]
        symbols = ",".join(batch)

        try:
            resp = requests.get(
                f"{FRANKFURTER_URL}/{date_str}",
                params={"from": "USD", "to": symbols},
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                rates = data.get("rates", {})

                for currency, rate_per_usd in rates.items():
                    if rate_per_usd and rate_per_usd > 0:
                        usd_rate = round(1.0 / rate_per_usd, 8)
                        rows.append({
                            "rate_date": date_str,
                            "currency_code": currency,
                            "usd_rate": usd_rate,
                            "_ingested_at": datetime.utcnow().isoformat(),
                        })
                        fetched_currencies.add(currency)
            elif resp.status_code == 404:
                pass  # Weekend/holiday
            else:
                log.warning(f"  {date_str}: HTTP {resp.status_code}")

        except Exception as e:
            log.warning(f"  {date_str} batch error: {e}")

    # ── SOURCE 2: Fill missing from fallback ──
    missing = [c for c in CURRENCIES if c not in fetched_currencies]
    if missing and fallback_rates:
        filled = 0
        for currency in missing:
            if currency in fallback_rates:
                rows.append({
                    "rate_date": date_str,
                    "currency_code": currency,
                    "usd_rate": fallback_rates[currency],
                    "_ingested_at": datetime.utcnow().isoformat(),
                })
                fetched_currencies.add(currency)
                filled += 1
        if filled:
            log.debug(f"  {date_str}: {filled} currencies filled from fallback")

    return rows


def fetch_all_rates():
    """Fetch rates for lookback period, forward-filling weekends."""
    log.info(f"Fetching FX rates for last {LOOKBACK_DAYS} days...")

    # Pre-fetch fallback rates (called once, cached)
    fallback_rates = fetch_fallback_rates()

    all_rows = []
    last_known_rates = {}

    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    current = start_date

    while current <= end_date:
        day_rows = fetch_rates_for_date(current, fallback_rates)

        if len(day_rows) > 1:  # More than just USD
            # Update last known rates
            for r in day_rows:
                last_known_rates[r["currency_code"]] = r["usd_rate"]
            all_rows.extend(day_rows)
            log.info(f"  {current}: {len(day_rows)} rates fetched")
        else:
            # Weekend/holiday — forward-fill from last known
            if last_known_rates:
                date_str = current.strftime("%Y-%m-%d")
                for currency, rate in last_known_rates.items():
                    all_rows.append({
                        "rate_date": date_str,
                        "currency_code": currency,
                        "usd_rate": rate,
                        "_ingested_at": datetime.utcnow().isoformat(),
                    })
                log.info(f"  {current}: Forward-filled {len(last_known_rates)} rates (weekend/holiday)")

        current += timedelta(days=1)

    log.info(f"  ✓ Total: {len(all_rows)} rate rows")
    return all_rows


# ─── BIGQUERY ────────────────────────────────────────────────────────────────
def get_bq():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)


def ensure_table(bq):
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(table_ref)
    except Exception:
        log.info(f"Creating table {BQ_DATASET}.{BQ_TABLE}")
        bq.create_table(bigquery.Table(table_ref, schema=SCHEMA))


def load_to_bq(bq, rows):
    if not rows:
        log.info("No rows to load")
        return

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Find date range to delete
    dates = [r["rate_date"] for r in rows]
    min_d, max_d = min(dates), max(dates)

    # Delete existing data for this date range
    try:
        bq.query(
            f"DELETE FROM `{table_ref}` WHERE rate_date BETWEEN '{min_d}' AND '{max_d}'"
        ).result()
        log.info(f"  Cleared {BQ_TABLE} for {min_d} to {max_d}")
    except Exception as e:
        log.warning(f"  Could not clear {BQ_TABLE}: {e}")

    # Load new data
    try:
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        log.info(f"  ✅ {len(rows):,} rows → {BQ_TABLE}")
    except Exception as e:
        log.error(f"  Load failed: {e}")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    log.info("FX Rates BigQuery Pipeline (Dual Source)")
    log.info(f"   Dataset: {BQ_DATASET}.{BQ_TABLE}")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")
    log.info(f"   Primary: frankfurter.app (ECB, 30 currencies)")
    log.info(f"   Fallback: open.er-api.com (160+ currencies)")

    bq = get_bq()
    ensure_table(bq)

    rows = fetch_all_rates()
    load_to_bq(bq, rows)

    log.info("FX rates sync complete!")


if __name__ == "__main__":
    main()
