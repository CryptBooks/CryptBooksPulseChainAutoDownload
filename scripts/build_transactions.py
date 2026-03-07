"""
build_transactions.py

Reads all CSV files from CryptBooksPortal/Data/SyncedData/transactions,
aggregates the data, and writes a JS file to CryptBooksPortal/Data/transactions.js.

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
SOURCE_DIR = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\SyncedData\transactions"
OUTPUT_FILE = r"C:\SourceCode\CryptBooks\CryptBooksPortal\Data\transactions.js"

def parse_wallet_name(filename):
    """Extract wallet name from filename like 'Chris_0xEF7a...csv'"""
    base = os.path.basename(filename).replace(".csv", "")
    parts = base.split("_0x", 1)
    if len(parts) == 2:
        return parts[0], "0x" + parts[1]
    return base, base

def parse_timestamp(ts_str):
    try:
        ts_str = ts_str.strip().rstrip("Z")
        if "." in ts_str:
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return None

def truncate_address(addr):
    if addr and len(addr) > 10:
        return addr[:6] + "..." + addr[-4:]
    return addr or ""

def format_fee(fee_str):
    try:
        val = float(fee_str)
        pls = val / 1e18
        if pls >= 1e6:
            return f"{pls/1e6:,.1f}M PLS"
        elif pls >= 1e3:
            return f"{pls/1e3:,.1f}K PLS"
        else:
            return f"{pls:,.2f} PLS"
    except (ValueError, TypeError):
        return "0 PLS"

def format_value(val_str):
    try:
        val = float(val_str)
        if val == 0:
            return "0"
        pls = val / 1e18
        if pls >= 1e9:
            return f"{pls/1e9:,.2f}B"
        elif pls >= 1e6:
            return f"{pls/1e6:,.2f}M"
        elif pls >= 1e3:
            return f"{pls/1e3:,.1f}K"
        else:
            return f"{pls:,.2f}"
    except (ValueError, TypeError):
        return val_str or "0"

def estimate_usd(fee_str, price_str):
    try:
        fee = float(fee_str) / 1e18
        price = float(price_str) if price_str else 0
        return fee * price
    except (ValueError, TypeError):
        return 0

def compute_daily_volume(txns, most_recent, cutoff):
    """Compute daily tx counts and USD values."""
    daily_counts = defaultdict(int)
    daily_usd = defaultdict(float)
    for t in txns:
        if t["timestamp"] >= cutoff:
            day = t["timestamp"].strftime("%Y-%m-%d")
            daily_counts[day] += 1
            daily_usd[day] += estimate_usd(t["fee"], t["currentPrice"])
    result = []
    current = cutoff
    while current <= most_recent:
        day = current.strftime("%Y-%m-%d")
        result.append({
            "date": day,
            "count": daily_counts.get(day, 0),
            "valueUsd": round(daily_usd.get(day, 0), 2),
        })
        current += timedelta(days=1)
    return result

def compute_volume_by_hour(txns, most_recent_day):
    """Compute hourly tx counts for a given day."""
    hourly_counts = defaultdict(int)
    for t in txns:
        if t["timestamp"].strftime("%Y-%m-%d") == most_recent_day:
            hourly_counts[t["timestamp"].hour] += 1
    return [{"hour": h, "count": hourly_counts.get(h, 0)} for h in range(24)]

def compute_recent_txns(txns, wallet_names, limit=50):
    """Get most recent transactions formatted for display."""
    result = []
    for t in txns[:limit]:
        from_name = wallet_names.get(t["from"].lower())
        to_name = wallet_names.get(t["to"].lower())
        result.append({
            "txHash": truncate_address(t["txHash"]),
            "txHashFull": t["txHash"],
            "from": from_name or truncate_address(t["from"]),
            "to": to_name or truncate_address(t["to"]),
            "value": format_value(t["value"]),
            "fee": format_fee(t["fee"]),
            "date": t["timestamp"].strftime("%Y-%m-%d"),
            "status": t["status"],
        })
    return result

def main():
    print(f"Reading CSVs from: {SOURCE_DIR}")

    all_txns = []
    wallet_names = {}
    wallet_list = []
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
                        all_txns.append({
                            "txHash": row.get("TxHash", ""),
                            "timestamp": ts,
                            "from": row.get("FromAddress", ""),
                            "to": row.get("ToAddress", ""),
                            "contractAddress": row.get("ContractAddress", ""),
                            "type": row.get("Type", ""),
                            "value": row.get("Value", "0"),
                            "fee": row.get("Fee", "0"),
                            "status": row.get("Status", ""),
                            "currentPrice": row.get("CurrentPrice", ""),
                            "walletFile": address.lower(),
                        })
        except Exception as e:
            print(f"  Warning: could not read {os.path.basename(filepath)}: {e}")

    if not all_txns:
        print("ERROR: No transaction records found!")
        return

    # Sort wallets alphabetically
    wallet_list.sort(key=lambda w: w["name"].lower())

    # Sort by timestamp descending
    all_txns.sort(key=lambda x: x["timestamp"], reverse=True)
    print(f"Total transactions loaded: {len(all_txns)}")

    # --- Global time range ---
    most_recent = all_txns[0]["timestamp"]
    oldest = all_txns[-1]["timestamp"]
    cutoff_all = datetime(oldest.year, oldest.month, oldest.day)
    most_recent_day = most_recent.strftime("%Y-%m-%d")

    # === AGGREGATE DATA ===
    all_addresses = set()
    ok_count = 0
    for t in all_txns:
        all_addresses.add(t["from"].lower())
        all_addresses.add(t["to"].lower())
        if t["status"] == "ok":
            ok_count += 1

    summary = {
        "totalTransactions": len(all_txns),
        "uniqueWallets": len(all_addresses),
        "successRate": round(ok_count / len(all_txns) * 100, 1) if all_txns else 0,
        "lastUpdated": most_recent.strftime("%Y-%m-%d"),
    }

    daily_volume = compute_daily_volume(all_txns, most_recent, cutoff_all)
    volume_by_hour = compute_volume_by_hour(all_txns, most_recent_day)

    # By Wallet aggregate (top 20)
    wallet_txcount = Counter()
    wallet_fees = defaultdict(float)
    for t in all_txns:
        addr = t["from"].lower()
        wallet_txcount[addr] += 1
        try:
            wallet_fees[addr] += float(t["fee"])
        except (ValueError, TypeError):
            pass

    by_wallet_aggregate = []
    for addr, count in wallet_txcount.most_common(20):
        name = wallet_names.get(addr, truncate_address(addr))
        by_wallet_aggregate.append({
            "name": name,
            "address": truncate_address(addr),
            "txCount": count,
            "totalFees": format_fee(str(wallet_fees.get(addr, 0))),
        })

    recent_txns = compute_recent_txns(all_txns, wallet_names, 50)

    # === PER-WALLET DATA ===
    wallet_txns = defaultdict(list)
    for t in all_txns:
        wallet_txns[t["walletFile"]].append(t)

    per_wallet = {}
    for wallet_addr, txns in wallet_txns.items():
        w_addresses = set()
        w_ok = 0
        for t in txns:
            w_addresses.add(t["from"].lower())
            w_addresses.add(t["to"].lower())
            if t["status"] == "ok":
                w_ok += 1

        w_latest = txns[0]["timestamp"].strftime("%Y-%m-%d") if txns else ""
        w_most_recent_day = txns[0]["timestamp"].strftime("%Y-%m-%d") if txns else most_recent_day

        per_wallet[wallet_addr] = {
            "summary": {
                "totalTransactions": len(txns),
                "uniqueWallets": len(w_addresses),
                "successRate": round(w_ok / len(txns) * 100, 1) if txns else 0,
                "lastUpdated": w_latest,
            },
            "dailyVolume": compute_daily_volume(txns, most_recent, cutoff_all),
            "volumeByHour": compute_volume_by_hour(txns, w_most_recent_day),
            "recentTransactions": compute_recent_txns(txns, wallet_names, 50),
        }

    print(f"Per-wallet data computed for {len(per_wallet)} wallets")

    # --- Build output ---
    data = {
        "wallets": wallet_list,
        "summary": summary,
        "dailyVolume": daily_volume,
        "volumeByHour": volume_by_hour,
        "byWallet": by_wallet_aggregate,
        "recentTransactions": recent_txns,
        "perWallet": per_wallet,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    js_content = "/* Auto-generated by build_transactions.py — do not edit manually */\n"
    js_content += "var TRANSACTION_DATA = " + json.dumps(data, indent=2) + ";\n"

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"Output written to: {OUTPUT_FILE}")
    print(f"  Summary: {summary['totalTransactions']} txns, {summary['uniqueWallets']} wallets, {summary['successRate']}% success")

if __name__ == "__main__":
    main()
