"""
spider_wallets.py — Discover external wallets from token transfer CSVs.

Scans token-transfer CSVs for counterparty addresses not yet tracked in
wallets.txt or wellknowncontracts.js, adds them as Ext_ unknowns, downloads
their CSVs, and optionally repeats at depth 2.

Usage:
  python spider_wallets.py                        # spider all wallets, depth 2
  python spider_wallets.py --depth 1              # direct counterparties only
  python spider_wallets.py --wallets 0xABC 0xDEF  # spider specific wallets only

Reads:
  - wallets.txt           (known addresses)
  - wellknowncontracts.js (known contracts to exclude)
  - SyncedData/token-transfers/*.csv  (counterparty addresses)

Writes:
  - Appends new Ext_ entries to wallets.txt
  - Downloads token-transfer + transaction CSVs for new externals
"""

import os
import re
import sys
import csv
import json
import time
import glob
import argparse
import urllib.request
import urllib.error
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────
PORTAL_DIR = r"C:\SourceCode\CryptBooks\CryptBooksPortal"
DATA_DIR = os.path.join(PORTAL_DIR, "Data")
WALLETS_FILE = os.path.join(DATA_DIR, "wallets.txt")
WKC_FILE = os.path.join(DATA_DIR, "wellknowncontracts.js")
TOKEN_TRANSFERS_DIR = os.path.join(DATA_DIR, "SyncedData", "token-transfers")
TRANSACTIONS_DIR = os.path.join(DATA_DIR, "SyncedData", "transactions")

TOKEN_TRANSFERS_URL = "https://api.scan.pulsechain.com/api/v1/token-transfers-csv?address_id={addr}&from_period=2000-01-01&to_period=2056-01-01&filter_type=address&filter_value="
TRANSACTIONS_URL = "https://api.scan.pulsechain.com/api/v1/transactions-csv?address_id={addr}&from_period=2000-01-01&to_period=2056-01-01&filter_type=address&filter_value=null"

SKIP_TYPES = {"spam", "scam"}

# ── Helpers ──────────────────────────────────────────────────────────────

def load_wallet_addresses():
    """Return set of lowercase addresses from wallets.txt."""
    addrs = set()
    if not os.path.exists(WALLETS_FILE):
        return addrs
    with open(WALLETS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            addrs.add(parts[0].strip().lower())
    return addrs


def load_wallet_types():
    """Return dict of lowercase address -> type from wallets.txt."""
    types = {}
    if not os.path.exists(WALLETS_FILE):
        return types
    with open(WALLETS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            addr = parts[0].strip().lower()
            wtype = parts[2].strip() if len(parts) > 2 else "unknown"
            types[addr] = wtype
    return types


def load_contract_addresses():
    """Parse wellknowncontracts.js and return set of lowercase addresses."""
    addrs = set()
    if not os.path.exists(WKC_FILE):
        return addrs
    with open(WKC_FILE, "r", encoding="utf-8") as f:
        text = f.read()
    # Match quoted hex addresses used as object keys
    for m in re.finditer(r'"(0x[0-9a-fA-F]{40})"', text):
        addrs.add(m.group(1).lower())
    return addrs


def get_target_csv_files(target_addrs=None):
    """Return list of token-transfer CSV paths to scan.
    If target_addrs is provided, only return CSVs whose filename contains
    one of those addresses. Otherwise return all CSVs."""
    all_csvs = glob.glob(os.path.join(TOKEN_TRANSFERS_DIR, "*.csv"))
    if target_addrs is None:
        return all_csvs

    target_lower = {a.lower() for a in target_addrs}
    matched = []
    for fp in all_csvs:
        base = os.path.basename(fp).lower()
        for addr in target_lower:
            if addr.lower() in base:
                matched.append(fp)
                break
    return matched


def scan_csvs_for_addresses(csv_files):
    """Read token-transfer CSVs and return set of all unique lowercase
    FromAddress + ToAddress values that look like 0x... addresses."""
    discovered = set()
    addr_pattern = re.compile(r'^0x[0-9a-fA-F]{40}$')

    for fp in csv_files:
        try:
            with open(fp, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for field in ("FromAddress", "ToAddress"):
                        val = row.get(field, "").strip()
                        if addr_pattern.match(val):
                            discovered.add(val.lower())
        except Exception as e:
            print(f"  Warning: could not read {os.path.basename(fp)}: {e}")

    return discovered


def make_ext_name(addr):
    """Generate Ext_0xABCD name from full address."""
    return f"Ext_{addr[:6]}"


def append_to_wallets_txt(new_externals):
    """Append new external wallet entries to wallets.txt.
    new_externals is a list of lowercase addresses, sorted."""
    if not new_externals:
        return
    with open(WALLETS_FILE, "a") as f:
        for addr in new_externals:
            name = make_ext_name(addr)
            f.write(f"{addr},{name},unknown\n")


def download_one(url, filepath, timeout=60):
    """Download a single URL to filepath. Returns (success, row_count, error_msg)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptBooks/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        with open(filepath, "wb") as f:
            f.write(data)
        text = data.decode("utf-8", errors="replace")
        rows = len([l for l in text.strip().split("\n") if l.strip()]) - 1
        return True, max(rows, 0), None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False, 0, "404 Not Found"
        return False, 0, str(e)
    except Exception as e:
        return False, 0, str(e)


def download_externals(addresses, batch_size=5, delay_seconds=30):
    """Download token-transfer + transaction CSVs for a list of addresses.
    Returns (total_tt_rows, total_tx_rows, errors)."""
    total = len(addresses)
    total_tt_rows = 0
    total_tx_rows = 0
    errors = 0
    downloaded_addrs = []

    num_batches = (total + batch_size - 1) // batch_size

    for batch_idx in range(num_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, total)
        batch = addresses[start:end]

        print(f"  --- Batch {batch_idx + 1}/{num_batches} (wallets {start + 1}-{end}) ---")

        for i, addr in enumerate(batch):
            wallet_num = start + i + 1
            name = make_ext_name(addr)
            filename = f"{name}_{addr}.csv"
            display = f"[{wallet_num}/{total}] {name}"

            # Token transfers
            url_tt = TOKEN_TRANSFERS_URL.format(addr=addr)
            fp_tt = os.path.join(TOKEN_TRANSFERS_DIR, filename)
            ok_tt, rows_tt, err_tt = download_one(url_tt, fp_tt)

            # Transactions
            url_tx = TRANSACTIONS_URL.format(addr=addr)
            fp_tx = os.path.join(TRANSACTIONS_DIR, filename)
            ok_tx, rows_tx, err_tx = download_one(url_tx, fp_tx)

            if ok_tt and ok_tx:
                total_tt_rows += rows_tt
                total_tx_rows += rows_tx
                print(f"  {display}: {rows_tt} transfers, {rows_tx} txns")
                if rows_tt > 0 or rows_tx > 0:
                    downloaded_addrs.append(addr)
            elif err_tt == "404 Not Found":
                print(f"  {display}: no on-chain data (404)")
            else:
                errors += 1
                err_msg = err_tt or err_tx
                print(f"  {display}: ERROR - {err_msg}")

            # Small delay between individual downloads
            if i < len(batch) - 1:
                time.sleep(1)

        # Delay between batches
        if batch_idx < num_batches - 1:
            print(f"\n  Waiting {delay_seconds}s before next batch...\n")
            remaining = delay_seconds
            while remaining > 0:
                wait = min(remaining, 10)
                time.sleep(wait)
                remaining -= wait
                if remaining > 0:
                    print(f"  ...{remaining}s remaining...")

    return total_tt_rows, total_tx_rows, errors, downloaded_addrs


# ── Main ─────────────────────────────────────────────────────────────────

def spider_one_level(target_csv_files, known_addrs, contract_addrs, level_label):
    """Discover new external wallets from CSVs.
    Returns sorted list of new addresses found."""

    print(f"\n{'='*60}")
    print(f"  {level_label}: Scanning {len(target_csv_files)} CSV files")
    print(f"{'='*60}")

    discovered = scan_csvs_for_addresses(target_csv_files)
    print(f"  Total unique addresses in CSVs: {len(discovered)}")

    # Subtract known
    new_addrs = discovered - known_addrs - contract_addrs
    # Filter out any that don't look like EOA (40 hex chars)
    new_addrs = sorted(a for a in new_addrs if re.match(r'^0x[0-9a-f]{40}$', a))

    print(f"  Already tracked (wallets.txt): {len(discovered & known_addrs)}")
    print(f"  Known contracts (excluded):    {len(discovered & contract_addrs)}")
    print(f"  NEW external wallets:          {len(new_addrs)}")

    return new_addrs


def main():
    parser = argparse.ArgumentParser(description="Spider token-transfer CSVs to discover external wallets")
    parser.add_argument("--depth", type=int, default=2,
                        help="Spider depth: 1 = direct counterparties, 2 = also spider their CSVs (default: 2)")
    parser.add_argument("--wallets", nargs="*",
                        help="Specific wallet addresses to spider (default: all non-spam/scam)")
    parser.add_argument("--batch-size", type=int, default=5,
                        help="Downloads per batch (default: 5)")
    parser.add_argument("--delay", type=int, default=30,
                        help="Seconds between download batches (default: 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    args = parser.parse_args()

    print(f"CryptBooks Wallet Spider")
    print(f"Depth: {args.depth}, Dry run: {args.dry_run}")
    print(f"Time: {datetime.now().isoformat()}")

    # Load known sets
    known_addrs = load_wallet_addresses()
    contract_addrs = load_contract_addresses()
    wallet_types = load_wallet_types()
    print(f"\nKnown wallets:   {len(known_addrs)}")
    print(f"Known contracts: {len(contract_addrs)}")

    # Determine which CSVs to scan
    if args.wallets:
        target_addrs = [a.lower() for a in args.wallets]
        # Filter out spam/scam
        target_addrs = [a for a in target_addrs if wallet_types.get(a, "unknown") not in SKIP_TYPES]
    else:
        # All non-spam/scam wallets
        target_addrs = [a for a, t in wallet_types.items() if t not in SKIP_TYPES]

    csv_files = get_target_csv_files(target_addrs)
    if not csv_files:
        print("\nNo CSV files found for target wallets. Nothing to spider.")
        return

    # ── Level 1 ──────────────────────────────────────────────────────
    new_addrs = spider_one_level(csv_files, known_addrs, contract_addrs, "Level 1: Direct counterparties")

    if not new_addrs:
        print("\nNo new external wallets discovered. Nothing to do.")
        return

    if args.dry_run:
        print(f"\n[DRY RUN] Would add {len(new_addrs)} new externals:")
        for a in new_addrs[:20]:
            print(f"  {a},{make_ext_name(a)},unknown")
        if len(new_addrs) > 20:
            print(f"  ... and {len(new_addrs) - 20} more")
        return

    # Add to wallets.txt
    print(f"\nAppending {len(new_addrs)} new externals to wallets.txt...")
    append_to_wallets_txt(new_addrs)
    known_addrs.update(new_addrs)

    # Download CSVs
    print(f"\nDownloading CSVs for {len(new_addrs)} new externals...")
    tt_rows, tx_rows, errs, downloaded = download_externals(
        new_addrs, batch_size=args.batch_size, delay_seconds=args.delay
    )

    print(f"\n  Level 1 downloads complete:")
    print(f"    Token transfers: {tt_rows} rows")
    print(f"    Transactions:    {tx_rows} rows")
    print(f"    Errors:          {errs}")
    print(f"    Wallets with data: {len(downloaded)}")

    level1_new = len(new_addrs)
    level2_new = 0

    # ── Level 2 (if depth >= 2) ─────────────────────────────────────
    if args.depth >= 2 and downloaded:
        # Scan the newly downloaded CSVs
        level2_csvs = get_target_csv_files(downloaded)
        if level2_csvs:
            new_addrs_l2 = spider_one_level(
                level2_csvs, known_addrs, contract_addrs,
                "Level 2: Counterparties of counterparties"
            )

            if new_addrs_l2:
                print(f"\nAppending {len(new_addrs_l2)} level-2 externals to wallets.txt...")
                append_to_wallets_txt(new_addrs_l2)
                known_addrs.update(new_addrs_l2)
                level2_new = len(new_addrs_l2)

                print(f"\nDownloading CSVs for {len(new_addrs_l2)} level-2 externals...")
                tt2, tx2, errs2, dl2 = download_externals(
                    new_addrs_l2, batch_size=args.batch_size, delay_seconds=args.delay
                )
                print(f"\n  Level 2 downloads complete:")
                print(f"    Token transfers: {tt2} rows")
                print(f"    Transactions:    {tx2} rows")
                print(f"    Errors:          {errs2}")
                print(f"    Wallets with data: {len(dl2)}")
            else:
                print("\n  No new level-2 externals discovered.")
        else:
            print("\n  No level-2 CSVs to scan (none of the level-1 externals had data).")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"SPIDER COMPLETE")
    print(f"  Level 1 new externals: {level1_new}")
    print(f"  Level 2 new externals: {level2_new}")
    print(f"  Total new externals:   {level1_new + level2_new}")
    print(f"{'='*60}")
    print(f"\nRun build scripts to rebuild portal data:")
    print(f"  python build_token_transfers.py")
    print(f"  python build_transactions.py")


if __name__ == "__main__":
    main()
