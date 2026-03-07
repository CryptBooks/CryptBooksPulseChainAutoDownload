"""
build_token_transfers.py

Reads all CSV files from CryptBooksPortal/Data/SyncedData/token-transfers,
aggregates the data, and writes a JS file to CryptBooksPortal/Data/token-transfers.js.

Outputs both aggregate data (for dashboard) and per-wallet breakdowns
(for report pages with wallet selector).

Uses only Python standard library — no external dependencies.
"""

import csv
import json
import os
import glob
from datetime import datetime, timedelta
from collections import Counter, defaultdict

# --- Configuration ---
SOURCE_DIR = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\SyncedData\token-transfers"
OUTPUT_FILE = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\token-transfers.js"

def parse_wallet_name(filename):
    """Extract wallet name from filename like 'Chris_0xEF7a...csv'"""
    base = os.path.basename(filename).replace(".csv", "")
    parts = base.split("_0x", 1)
    if len(parts) == 2:
        return parts[0], "0x" + parts[1]
    return base, base

def parse_timestamp(ts_str):
    """Parse timestamp like '2023-09-25 19:26:45.000000Z' to datetime"""
    try:
        ts_str = ts_str.strip().rstrip("Z")
        if "." in ts_str:
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return None

def truncate_address(addr):
    """Shorten address to 0xYYYY...YYYY format (first 6 + last 4)"""
    if addr and len(addr) > 10:
        return addr[:6] + "..." + addr[-4:]
    return addr or ""

def format_amount(amount_str):
    """Format large token amounts to be human-readable"""
    try:
        val = float(amount_str)
        if val >= 1e18:
            return f"{val/1e18:,.2f}"
        elif val >= 1e9:
            return f"{val/1e9:,.2f}B"
        elif val >= 1e6:
            return f"{val/1e6:,.2f}M"
        elif val >= 1e3:
            return f"{val:,.0f}"
        else:
            return f"{val:,.2f}"
    except (ValueError, TypeError):
        return amount_str or "0"

def compute_transfers_over_time(transfers, most_recent, cutoff):
    """Compute daily transfer counts between cutoff and most_recent."""
    daily_counts = defaultdict(int)
    for t in transfers:
        if t["timestamp"] >= cutoff:
            day = t["timestamp"].strftime("%Y-%m-%d")
            daily_counts[day] += 1
    result = []
    current = cutoff
    while current <= most_recent:
        day = current.strftime("%Y-%m-%d")
        result.append({"date": day, "count": daily_counts.get(day, 0)})
        current += timedelta(days=1)
    return result

def compute_by_token(transfers):
    """Compute top 10 tokens by transfer count."""
    token_counts = Counter(t["token"] for t in transfers if t["token"])
    total_count = sum(token_counts.values())
    result = []
    for symbol, count in token_counts.most_common(10):
        result.append({
            "symbol": symbol,
            "count": count,
            "percentage": round(count / total_count * 100, 1) if total_count else 0,
        })
    return result

def compute_all_transfers(transfers, wallet_names):
    """Get all transfers formatted for display (no limit)."""
    result = []
    for t in transfers:
        from_name = wallet_names.get(t["from"].lower())
        to_name = wallet_names.get(t["to"].lower())
        result.append({
            "txHash": truncate_address(t["txHash"]),
            "txHashFull": t["txHash"],
            "token": t["token"],
            "from": from_name or truncate_address(t["from"]),
            "to": to_name or truncate_address(t["to"]),
            "amount": format_amount(t["amount"]),
            "amountRaw": t["amount"],
            "date": t["timestamp"].strftime("%Y-%m-%d"),
        })
    return result

def compute_by_counterparty(transfers, wallet_addr, wallet_names):
    """Compute transfer count and volume grouped by counterparty address.

    Looks at both directions (in and out) and aggregates by the OTHER address
    in each transfer. Excludes the wallet itself from results.
    """
    addr_counts = Counter()
    addr_volume = defaultdict(float)
    wallet_low = wallet_addr.lower()

    for t in transfers:
        from_low = t["from"].lower()
        to_low = t["to"].lower()

        # Determine the counterparty (the address that ISN'T this wallet)
        if from_low == wallet_low:
            counterparty = to_low
        elif to_low == wallet_low:
            counterparty = from_low
        else:
            # Transfer doesn't directly involve this wallet, skip
            continue

        # Skip self-transfers
        if counterparty == wallet_low:
            continue

        addr_counts[counterparty] += 1
        try:
            addr_volume[counterparty] += float(t["amount"])
        except (ValueError, TypeError):
            pass

    result = []
    for addr, count in addr_counts.most_common(15):
        name = wallet_names.get(addr, truncate_address(addr))
        result.append({
            "name": name,
            "address": truncate_address(addr),
            "fullAddress": addr,
            "transferCount": count,
            "totalVolume": addr_volume.get(addr, 0),
        })
    return result

def main():
    print(f"Reading CSVs from: {SOURCE_DIR}")

    all_transfers = []
    wallet_names = {}   # lowercase address -> friendly name
    wallet_list = []    # list of {name, address, fullAddress} for selector
    csv_files = glob.glob(os.path.join(SOURCE_DIR, "*.csv"))

    if not csv_files:
        print("ERROR: No CSV files found!")
        return

    print(f"Found {len(csv_files)} CSV files")

    for filepath in csv_files:
        name, address = parse_wallet_name(filepath)
        wallet_names[address.lower()] = name
        wallet_list.append({
            "name": name,
            "address": truncate_address(address),
            "fullAddress": address.lower(),
        })

        try:
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = parse_timestamp(row.get("UnixTimestamp", ""))
                    if ts:
                        all_transfers.append({
                            "txHash": row.get("TxHash", ""),
                            "timestamp": ts,
                            "from": row.get("FromAddress", ""),
                            "to": row.get("ToAddress", ""),
                            "token": row.get("TokenSymbol", ""),
                            "amount": row.get("TokensTransferred", "0"),
                            "type": row.get("Type", ""),
                            "status": row.get("Status", ""),
                            "walletFile": address.lower(),
                        })
        except Exception as e:
            print(f"  Warning: could not read {os.path.basename(filepath)}: {e}")

    if not all_transfers:
        print("ERROR: No transfer records found!")
        return

    # Sort wallets alphabetically by name
    wallet_list.sort(key=lambda w: w["name"].lower())

    # Sort transfers by timestamp descending (most recent first)
    all_transfers.sort(key=lambda x: x["timestamp"], reverse=True)
    print(f"Total transfers loaded: {len(all_transfers)}")

    # --- Global time range ---
    most_recent = all_transfers[0]["timestamp"]
    oldest = all_transfers[-1]["timestamp"]
    cutoff_all = datetime(oldest.year, oldest.month, oldest.day)

    # === AGGREGATE DATA (for dashboard) ===
    all_addresses = set()
    token_symbols = set()
    for t in all_transfers:
        all_addresses.add(t["from"].lower())
        all_addresses.add(t["to"].lower())
        if t["token"]:
            token_symbols.add(t["token"])

    summary = {
        "totalTransfers": len(all_transfers),
        "totalTokens": len(token_symbols),
        "uniqueWallets": len(all_addresses),
        "lastUpdated": most_recent.strftime("%Y-%m-%d"),
    }

    transfers_over_time = compute_transfers_over_time(all_transfers, most_recent, cutoff_all)
    by_token = compute_by_token(all_transfers)

    # Top Holders (aggregate)
    wallet_activity = Counter()
    for t in all_transfers:
        wallet_activity[t["from"].lower()] += 1
        wallet_activity[t["to"].lower()] += 1

    top_holders = []
    for addr, count in wallet_activity.most_common(20):
        name = wallet_names.get(addr, truncate_address(addr))
        top_holders.append({
            "name": name,
            "address": truncate_address(addr),
            "transferCount": count,
        })

    recent_transfers = compute_all_transfers(all_transfers, wallet_names)

    # === PER-WALLET DATA (for report pages) ===
    # Group transfers by wallet file
    wallet_transfers = defaultdict(list)
    for t in all_transfers:
        wallet_transfers[t["walletFile"]].append(t)

    by_wallet = {}
    for wallet_addr, transfers in wallet_transfers.items():
        # Already sorted descending since all_transfers was sorted
        w_addresses = set()
        w_tokens = set()
        for t in transfers:
            w_addresses.add(t["from"].lower())
            w_addresses.add(t["to"].lower())
            if t["token"]:
                w_tokens.add(t["token"])

        w_latest = transfers[0]["timestamp"].strftime("%Y-%m-%d") if transfers else ""

        by_wallet[wallet_addr] = {
            "summary": {
                "totalTransfers": len(transfers),
                "totalTokens": len(w_tokens),
                "uniqueWallets": len(w_addresses),
                "lastUpdated": w_latest,
            },
            "transfersOverTime": compute_transfers_over_time(transfers, most_recent, cutoff_all),
            "byToken": compute_by_token(transfers),
            "recentTransfers": compute_all_transfers(transfers, wallet_names),
            "byAddress": compute_by_counterparty(transfers, wallet_addr, wallet_names),
        }

    print(f"Per-wallet data computed for {len(by_wallet)} wallets")

    # --- Build output ---
    data = {
        "wallets": wallet_list,
        "summary": summary,
        "transfersOverTime": transfers_over_time,
        "byToken": by_token,
        "topHolders": top_holders,
        "recentTransfers": recent_transfers,
        "byWallet": by_wallet,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    js_content = "/* Auto-generated by build_token_transfers.py — do not edit manually */\n"
    js_content += "var TOKEN_TRANSFER_DATA = " + json.dumps(data, indent=2) + ";\n"

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"Output written to: {OUTPUT_FILE}")
    print(f"  Summary: {summary['totalTransfers']} transfers, {summary['totalTokens']} tokens, {summary['uniqueWallets']} wallets")

if __name__ == "__main__":
    main()
