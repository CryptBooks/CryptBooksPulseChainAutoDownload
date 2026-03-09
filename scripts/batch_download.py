"""
batch_download.py — Download PulseChain CSVs in batches with configurable delays.

Usage:
  python batch_download.py token-transfers [--batch-size 10] [--delay 120]
  python batch_download.py transactions [--batch-size 10] [--delay 120]
  python batch_download.py both [--batch-size 10] [--delay 120]

Reads wallets from wallets.txt, downloads CSVs from PulseChain explorer,
and spaces batches apart to avoid rate limiting.
"""

import os
import sys
import time
import urllib.request
import urllib.error
import argparse
from datetime import datetime

WALLETS_FILE = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\wallets.txt"
TOKEN_TRANSFERS_DIR = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\SyncedData\token-transfers"
TRANSACTIONS_DIR = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\SyncedData\transactions"

TOKEN_TRANSFERS_URL = "https://api.scan.pulsechain.com/api/v1/token-transfers-csv?address_id={addr}&from_period=2000-01-01&to_period=2056-01-01&filter_type=address&filter_value="
TRANSACTIONS_URL = "https://api.scan.pulsechain.com/api/v1/transactions-csv?address_id={addr}&from_period=2000-01-01&to_period=2056-01-01&filter_type=address&filter_value=null"


SKIP_TYPES = {"spam", "scam"}


def load_wallets():
    """Read wallets.txt and return list of (address, friendly_name) tuples.
    Wallets typed as spam or scam are excluded automatically."""
    wallets = []
    skipped = 0
    with open(WALLETS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            address = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""
            wtype = parts[2].strip().lower() if len(parts) > 2 else "unknown"
            if wtype in SKIP_TYPES:
                skipped += 1
                continue
            wallets.append((address, name))
    if skipped:
        print(f"  Skipped {skipped} spam/scam wallets")
    return wallets


def make_filename(address, name):
    """Generate CSV filename from address and friendly name."""
    if name:
        return f"{name}_{address}.csv"
    return f"{address}.csv"


def download_one(url, filepath, timeout=60):
    """Download a single URL to filepath. Returns (success, row_count, error_msg)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptBooks/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        with open(filepath, "wb") as f:
            f.write(data)
        # Count rows (subtract header)
        text = data.decode("utf-8", errors="replace")
        rows = len([l for l in text.strip().split("\n") if l.strip()]) - 1
        rows = max(rows, 0)
        return True, rows, None
    except Exception as e:
        return False, 0, str(e)


def download_type(wallets, download_type_name, output_dir, url_template, batch_size, delay_seconds):
    """Download CSVs for all wallets in batches."""
    os.makedirs(output_dir, exist_ok=True)

    total_wallets = len(wallets)
    total_rows = 0
    errors = 0
    error_details = []

    num_batches = (total_wallets + batch_size - 1) // batch_size
    print(f"\n{'='*60}")
    print(f"Downloading {download_type_name} for {total_wallets} wallets")
    print(f"Batch size: {batch_size}, Delay: {delay_seconds}s, Batches: {num_batches}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")

    for batch_idx in range(num_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, total_wallets)
        batch = wallets[start:end]

        print(f"--- Batch {batch_idx + 1}/{num_batches} (wallets {start + 1}-{end}) ---")

        for i, (address, name) in enumerate(batch):
            wallet_num = start + i + 1
            filename = make_filename(address, name)
            filepath = os.path.join(output_dir, filename)
            url = url_template.format(addr=address)

            display_name = name if name else address[:10] + "..."
            sys.stdout.write(f"  [{wallet_num}/{total_wallets}] {display_name}: ")
            sys.stdout.flush()

            success, rows, err = download_one(url, filepath)
            if success:
                total_rows += rows
                print(f"{rows} rows")
            else:
                errors += 1
                error_details.append((address, name, err))
                print(f"ERROR - {err}")

            # Small delay between individual downloads to be polite
            if i < len(batch) - 1:
                time.sleep(1)

        # Delay between batches (but not after the last batch)
        if batch_idx < num_batches - 1:
            print(f"\n  Waiting {delay_seconds}s before next batch...\n")
            # Show countdown every 30 seconds
            remaining = delay_seconds
            while remaining > 0:
                wait = min(remaining, 30)
                time.sleep(wait)
                remaining -= wait
                if remaining > 0:
                    print(f"  ...{remaining}s remaining...")

    # Write status file
    status_file = os.path.join(os.path.dirname(output_dir),
                               f"{download_type_name.replace(' ', '-')}-status.txt")
    with open(status_file, "w") as f:
        f.write(f"Status: {'complete' if errors == 0 else 'complete with errors'}\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Wallets: {total_wallets}\n")
        f.write(f"TotalRows: {total_rows}\n")
        f.write(f"Errors: {errors}\n")
        if error_details:
            f.write("\nError Details:\n")
            for addr, nm, err in error_details:
                f.write(f"  {nm or addr}: {err}\n")

    print(f"\n{'='*60}")
    print(f"{download_type_name} COMPLETE")
    print(f"  Wallets: {total_wallets}")
    print(f"  Total rows: {total_rows}")
    print(f"  Errors: {errors}")
    if error_details:
        print(f"  Failed wallets:")
        for addr, nm, err in error_details:
            print(f"    - {nm or addr}: {err}")
    print(f"{'='*60}\n")

    return total_rows, errors


def main():
    parser = argparse.ArgumentParser(description="Batch download PulseChain CSVs")
    parser.add_argument("type", choices=["token-transfers", "transactions", "both"],
                        help="What to download")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Wallets per batch (default: 10)")
    parser.add_argument("--delay", type=int, default=120,
                        help="Seconds between batches (default: 120)")
    parser.add_argument("--noclean", action="store_true",
                        help="Don't delete existing CSVs first")
    args = parser.parse_args()

    wallets = load_wallets()
    print(f"Loaded {len(wallets)} wallets from {WALLETS_FILE}")

    if args.type in ("token-transfers", "both"):
        if not args.noclean:
            # Clean directory
            if os.path.exists(TOKEN_TRANSFERS_DIR):
                for f in os.listdir(TOKEN_TRANSFERS_DIR):
                    if f.endswith(".csv"):
                        os.remove(os.path.join(TOKEN_TRANSFERS_DIR, f))
                print(f"Cleaned {TOKEN_TRANSFERS_DIR}")

        download_type(wallets, "token-transfers", TOKEN_TRANSFERS_DIR,
                      TOKEN_TRANSFERS_URL, args.batch_size, args.delay)

    if args.type in ("transactions", "both"):
        if not args.noclean:
            if os.path.exists(TRANSACTIONS_DIR):
                for f in os.listdir(TRANSACTIONS_DIR):
                    if f.endswith(".csv"):
                        os.remove(os.path.join(TRANSACTIONS_DIR, f))
                print(f"Cleaned {TRANSACTIONS_DIR}")

        download_type(wallets, "transactions", TRANSACTIONS_DIR,
                      TRANSACTIONS_URL, args.batch_size, args.delay)

    print("All downloads complete!")


if __name__ == "__main__":
    main()
