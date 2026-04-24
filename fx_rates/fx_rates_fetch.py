"""
Daily FX Rates → BigQuery Pipeline
Fetches USD exchange rates from frankfurter.app (ECB data, free, no API key)
Loads into terafort.cross_platform.daily_fx_rates

Hybrid approach:
  - GP Earnings table: 100% accurate USD (3-4 weeks delayed)
  - GP Sales table × daily_fx_rates: ~99.5% accurate (real-time)
  - Staging query auto-picks the best source per date
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

# All currencies seen in your GP Sales data
CURRENCIES = [
    "INR", "IDR", "EUR", "TRY", "MYR", "SAR", "BRL", "PHP",
    "ILS", "LKR", "PLN", "CAD", "PKR", "AED", "MXN", "NGN",
    "GBP", "ZAR", "UAH", "AUD", "THB", "NZD", "COP", "SGD",
    "HUF", "PEN", "KES", "CRC", "MAD", "CLP", "NOK", "VND",
    "RON", "KZT", "CHF", "KRW", "TWD", "JPY", "SEK", "MMK",
    "IQD", "CZK", "TZS", "BDT", "EGP", "PYG", "RSD", "HKD",
    "GEL", "DZD", "QAR", "BGN", "DKK", "MNT", "HNL", "XAF",
    "XOF", "JOD", "OMR", "BHD", "KWD", "GTQ", "BOB", "UYU",
    "ISK", "RWF", "UGX", "GHS", "ETB", "NPR", "LBP", "MZN"
]

FRANKFURTER_URL = "https://api.frankfurter.app"

# ─── SCHEMA ──────────────────────────────────────────────────────────────────
S = bigquery.SchemaField
SCHEMA = [
    S("rate_date", "DATE"),
    S("currency_code", "STRING"),
    S("usd_rate", "FLOAT64"),
    S("_ingested_at", "TIMESTAMP"),
]

# ─── FX FETCH ────────────────────────────────────────────────────────────────
def fetch_rates_for_date(rate_date):
    """Fetch 1 USD = X foreign currency rates, then invert to get 1 FOREIGN = X USD."""
    date_str = rate_date.strftime("%Y-%m-%d")
    rows = []

    # Always add USD = 1.0
    rows.append({
        "rate_date": date_str,
        "currency_code": "USD",
        "usd_rate": 1.0,
        "_ingested_at": datetime.utcnow().isoformat(),
    })

    # Frankfurter supports max ~40 currencies per call
    # Split into batches
    batch_size = 30
    for i in range(0, len(CURRENCIES), batch_size):
        batch = CURRENCIES[i:i + batch_size]
        symbols = ",".join(batch)

        try:
            # Get: 1 USD = X foreign currency
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
                        # Invert: 1 foreign = X USD
                        usd_rate = round(1.0 / rate_per_usd, 8)
                        rows.append({
                            "rate_date": date_str,
                            "currency_code": currency,
                            "usd_rate": usd_rate,
                            "_ingested_at": datetime.utcnow().isoformat(),
                        })
            elif resp.status_code == 404:
                # Weekend or holiday — no rates available
                log.info(f"  {date_str}: No rates (weekend/holiday)")
            else:
                log.warning(f"  {date_str}: HTTP {resp.status_code}")

        except Exception as e:
            log.warning(f"  {date_str} batch error: {e}")

    return rows


def fetch_all_rates():
    """Fetch rates for lookback period, forward-filling weekends."""
    log.info(f"Fetching FX rates for last {LOOKBACK_DAYS} days...")
    all_rows = []
    last_known_rates = {}

    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    current = start_date

    while current <= end_date:
        day_rows = fetch_rates_for_date(current)

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
    log.info("💱 FX Rates → BigQuery Pipeline")
    log.info(f"   Dataset: {BQ_DATASET}.{BQ_TABLE}")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")

    bq = get_bq()
    ensure_table(bq)

    rows = fetch_all_rates()
    load_to_bq(bq, rows)

    log.info("✅ FX rates sync complete!")


if __name__ == "__main__":
    main()
