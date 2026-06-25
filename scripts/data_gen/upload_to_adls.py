"""
Prime Capital Bank — ADLS Gen2 Upload Script
============================================
Uploads all generated CSV files from the local data directory to Azure Data
Lake Storage Gen2, organised into the raw zone folder structure below:

  raw/
    customers/
      customers_<YYYYMMDD>.csv
    accounts/
      accounts_<YYYYMMDD>.csv
    transactions/
      YYYY/MM/DD/
        transactions_<YYYYMMDD_HHMMSS>_part<N>.csv
    loans/
      loans_<YYYYMMDD>.csv
    card_transactions/
      YYYY/MM/DD/
        card_transactions_<YYYYMMDD>.csv
    payments/
      payments_<YYYYMMDD>.csv
    fraud_alerts/
      fraud_alerts_<YYYYMMDD>.csv

Prerequisites
-------------
  pip install azure-storage-blob azure-identity

Authentication options (set ONE):
  Option A — Account Key (fastest for dev):
    Set env var:  ADLS_ACCOUNT_KEY=<your-key>
  Option B — SAS token:
    Set env var:  ADLS_SAS_TOKEN=<sas-token>
  Option C — Azure AD (DefaultAzureCredential, for prod):
    Log in with: az login
    Leave ADLS_ACCOUNT_KEY and ADLS_SAS_TOKEN unset.

Usage
-----
  python upload_to_adls.py [--date YYYY-MM-DD] [--dry-run]

  --date      Partition date for transactions (defaults to today)
  --dry-run   List files that would be uploaded without uploading

Author  : Anthony Apollis
Created : 2026-06-24
"""

import os
import sys
import argparse
import pathlib
import datetime
import time
import math

# ---------------------------------------------------------------------------
# Configuration — edit these or override with environment variables
# ---------------------------------------------------------------------------

STORAGE_ACCOUNT_NAME = os.environ.get("ADLS_ACCOUNT_NAME", "primecapitaladls")
CONTAINER_NAME = os.environ.get("ADLS_CONTAINER", "raw")
LOCAL_DATA_DIR = pathlib.Path(
    r"C:\Users\Anthony.DESKTOP-ES5HL78\Documents\Prime Capital Bank\data"
)

# ---------------------------------------------------------------------------
# File → ADLS path mapping
# ---------------------------------------------------------------------------

def build_upload_plan(run_date: datetime.date) -> list[dict]:
    """
    Returns a list of dicts:
      { "local_path": Path, "remote_path": str, "description": str }
    """
    YYYY = run_date.strftime("%Y")
    MM = run_date.strftime("%m")
    DD = run_date.strftime("%d")
    stamp = run_date.strftime("%Y%m%d")
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    plan = []

    # --- Dimension / reference files (single file per dataset) ---
    simple_datasets = [
        ("customers.csv",       f"customers/customers_{stamp}.csv",       "Customers"),
        ("accounts.csv",        f"accounts/accounts_{stamp}.csv",         "Accounts"),
        ("loans.csv",           f"loans/loans_{stamp}.csv",               "Loans"),
        ("payments.csv",        f"payments/payments_{stamp}.csv",         "Payments"),
        ("fraud_alerts.csv",    f"fraud_alerts/fraud_alerts_{stamp}.csv", "Fraud Alerts"),
    ]
    for local_name, remote_path, description in simple_datasets:
        local = LOCAL_DATA_DIR / local_name
        if local.exists():
            plan.append({
                "local_path": local,
                "remote_path": remote_path,
                "description": description,
            })
        else:
            print(f"  [WARN] File not found, skipping: {local}")

    # --- Transactions — partitioned by date ---
    tx_local = LOCAL_DATA_DIR / "transactions.csv"
    if tx_local.exists():
        plan.append({
            "local_path": tx_local,
            "remote_path": f"transactions/{YYYY}/{MM}/{DD}/transactions_{ts}.csv",
            "description": "Transactions (10M rows)",
        })
    else:
        print(f"  [WARN] File not found, skipping: {tx_local}")

    # --- Card transactions — partitioned by date ---
    card_local = LOCAL_DATA_DIR / "card_transactions.csv"
    if card_local.exists():
        plan.append({
            "local_path": card_local,
            "remote_path": f"card_transactions/{YYYY}/{MM}/{DD}/card_transactions_{ts}.csv",
            "description": "Card Transactions (2M rows)",
        })
    else:
        print(f"  [WARN] File not found, skipping: {card_local}")

    return plan


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

def sizeof_fmt(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def upload_file_chunked(
    client,           # DataLakeFileClient
    local_path: pathlib.Path,
    chunk_size_mb: int = 64,
) -> None:
    """Upload a (potentially large) file in chunks with progress reporting."""
    total_bytes = local_path.stat().st_size
    chunk_bytes = chunk_size_mb * 1024 * 1024
    uploaded = 0
    t_start = time.time()

    with open(local_path, "rb") as f:
        # Create the file on ADLS (overwrite if exists)
        client.create_file()
        offset = 0
        while True:
            chunk = f.read(chunk_bytes)
            if not chunk:
                break
            client.append_data(data=chunk, offset=offset, length=len(chunk))
            offset += len(chunk)
            uploaded += len(chunk)
            pct = uploaded / total_bytes * 100
            elapsed = time.time() - t_start
            speed = uploaded / elapsed / (1024 * 1024) if elapsed > 0 else 0
            print(
                f"    {sizeof_fmt(uploaded)} / {sizeof_fmt(total_bytes)}"
                f"  ({pct:.1f}%)  {speed:.1f} MB/s",
                end="\r",
                flush=True,
            )
        client.flush_data(offset)

    elapsed = time.time() - t_start
    print(
        f"    {sizeof_fmt(total_bytes)} uploaded in {elapsed:.1f}s"
        f"  ({total_bytes / elapsed / (1024*1024):.1f} MB/s)"
        + " " * 10
    )


def get_credential():
    """
    Return an appropriate credential object based on environment variables.
    Priority: Account Key -> SAS Token -> DefaultAzureCredential (Azure AD)
    """
    account_key = os.environ.get("ADLS_ACCOUNT_KEY")
    if account_key:
        print("  [Auth] Using Storage Account Key")
        return {"account_key": account_key}

    sas_token = os.environ.get("ADLS_SAS_TOKEN")
    if sas_token:
        print("  [Auth] Using SAS Token")
        return {"sas_token": sas_token}

    print("  [Auth] Using DefaultAzureCredential (Azure AD / Managed Identity)")
    try:
        from azure.identity import DefaultAzureCredential
        return {"credential": DefaultAzureCredential()}
    except ImportError:
        print("  [ERROR] azure-identity not installed. Run: pip install azure-identity")
        sys.exit(1)


def build_adls_clients(credential_kwargs: dict):
    """
    Return (DataLakeServiceClient, FileSystemClient) for the container.
    """
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        print("  [ERROR] azure-storage-blob not installed.")
        print("          Run: pip install azure-storage-blob azure-storage-file-datalake")
        sys.exit(1)

    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, **credential_kwargs)
    fs_client = service_client.get_file_system_client(CONTAINER_NAME)

    # Ensure container exists
    try:
        fs_client.create_file_system()
        print(f"  [ADLS] Container '{CONTAINER_NAME}' created.")
    except Exception:
        pass  # Already exists

    return service_client, fs_client


def ensure_directory(fs_client, path: str):
    """Create intermediate directories on ADLS if they don't exist."""
    parts = path.rsplit("/", 1)
    if len(parts) > 1:
        dir_path = parts[0]
        try:
            fs_client.create_directory(dir_path)
        except Exception:
            pass  # already exists


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Upload Prime Capital Bank synthetic data to ADLS Gen2"
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.date.fromisoformat(s),
        default=datetime.date.today(),
        help="Partition date for transaction files (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print upload plan without actually uploading.",
    )
    parser.add_argument(
        "--chunk-mb",
        type=int,
        default=64,
        help="Upload chunk size in MB (default: 64).",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  Prime Capital Bank — ADLS Gen2 Upload")
    print(f"  Storage account : {STORAGE_ACCOUNT_NAME}")
    print(f"  Container       : {CONTAINER_NAME}")
    print(f"  Partition date  : {args.date}")
    print(f"  Dry run         : {args.dry_run}")
    print("=" * 70)

    # Build the upload plan
    plan = build_upload_plan(args.date)

    if not plan:
        print("\n  [ERROR] No files found to upload. Run generate_bank_data.py first.")
        sys.exit(1)

    # Print the plan
    print(f"\n  Files to upload ({len(plan)}):\n")
    total_bytes = 0
    for item in plan:
        size = item["local_path"].stat().st_size
        total_bytes += size
        print(f"    {item['description']:<35}  {sizeof_fmt(size):>10}  ->  {CONTAINER_NAME}/{item['remote_path']}")

    print(f"\n  Total upload size: {sizeof_fmt(total_bytes)}")

    if args.dry_run:
        print("\n  [DRY RUN] No files uploaded.")
        return

    # Authenticate & connect
    print()
    cred_kwargs = get_credential()
    service_client, fs_client = build_adls_clients(cred_kwargs)
    print(f"  [ADLS] Connected to {STORAGE_ACCOUNT_NAME}\n")

    # Upload each file
    t_total_start = time.time()
    uploaded_count = 0
    failed = []

    for item in plan:
        local_path: pathlib.Path = item["local_path"]
        remote_path: str = item["remote_path"]
        description: str = item["description"]

        print(f"\n  Uploading: {description}")
        print(f"    Local  : {local_path}")
        print(f"    Remote : {CONTAINER_NAME}/{remote_path}")

        try:
            ensure_directory(fs_client, remote_path)
            file_client = fs_client.get_file_client(remote_path)
            upload_file_chunked(file_client, local_path, chunk_size_mb=args.chunk_mb)
            uploaded_count += 1
            print(f"    [OK] {description} uploaded successfully.")
        except Exception as exc:
            print(f"\n    [ERROR] Failed to upload {description}: {exc}")
            failed.append({"file": description, "error": str(exc)})

    # Summary
    elapsed_total = time.time() - t_total_start
    print("\n" + "=" * 70)
    print(f"  Upload complete: {uploaded_count}/{len(plan)} files in {elapsed_total:.1f}s")
    if failed:
        print(f"\n  FAILED ({len(failed)}):")
        for f in failed:
            print(f"    - {f['file']}: {f['error']}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
