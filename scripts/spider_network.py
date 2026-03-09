"""
spider_network.py — Recursive wallet network spider.

Discovers ALL connected EOA wallets by spidering token-transfer CSVs,
downloads their data, detects hub wallets, tracks per-token edge data
with tx hashes, and produces spider-network.js for the network graph page.

Usage:
  python spider_network.py                          # full recursive spider
  python spider_network.py --resume                 # resume interrupted run
  python spider_network.py --dry-run                # show what would happen
  python spider_network.py --output-only            # rebuild JS from checkpoint
  python spider_network.py --max-wallets 10         # safety cap
  python spider_network.py --hub-threshold 50       # EOA limit for "human" wallets
  python spider_network.py --max-depth 10           # max BFS hops from seed
  python spider_network.py --delay 30               # seconds between downloads
  python spider_network.py --wallets 0xABC 0xDEF    # specific seed wallets
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
CHECKPOINT_FILE = os.path.join(DATA_DIR, "SyncedData", "spider-checkpoint.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "spider-network.js")

TOKEN_TRANSFERS_URL = ("https://api.scan.pulsechain.com/api/v1/token-transfers-csv"
                       "?address_id={addr}&from_period=2000-01-01"
                       "&to_period=2056-01-01&filter_type=address&filter_value=")

SKIP_TYPES = {"spam", "scam"}
ADDR_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')


# ── Data Loaders ─────────────────────────────────────────────────────────

def load_wallet_data():
    """Parse wallets.txt -> {addr_lower: {name, type, address_original}}.
    Also returns set of all known addresses (lowercase)."""
    wallet_info = {}
    if not os.path.exists(WALLETS_FILE):
        return wallet_info
    with open(WALLETS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            addr = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""
            wtype = parts[2].strip() if len(parts) > 2 else "unknown"
            wallet_info[addr.lower()] = {
                "name": name,
                "type": wtype,
                "address": addr,
            }
    return wallet_info


def load_contract_addresses():
    """Parse wellknowncontracts.js -> set of lowercase addresses."""
    addrs = set()
    if not os.path.exists(WKC_FILE):
        return addrs
    with open(WKC_FILE, "r", encoding="utf-8") as f:
        text = f.read()
    for m in re.finditer(r'"(0x[0-9a-fA-F]{40})"', text):
        addrs.add(m.group(1).lower())
    return addrs


# ── Checkpoint ───────────────────────────────────────────────────────────

def new_checkpoint():
    return {
        "version": 1,
        "started_at": datetime.now().isoformat(),
        "last_updated": datetime.now().isoformat(),
        "status": "in_progress",
        "processed": {},
        "queue": [],
        "edges": {},
        "discovered_wallets": [],
        "stats": {
            "total_processed": 0,
            "total_hubs": 0,
            "total_downloads": 0,
            "total_errors": 0,
        },
    }


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_checkpoint(cp):
    cp["last_updated"] = datetime.now().isoformat()
    cp["stats"]["total_processed"] = len(cp["processed"])
    cp["stats"]["total_hubs"] = sum(
        1 for d in cp["processed"].values() if d.get("is_hub")
    )
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=1)
    os.replace(tmp, CHECKPOINT_FILE)


# ── CSV helpers ──────────────────────────────────────────────────────────

def make_ext_name(addr):
    """Generate Ext_0xABCD name from full address."""
    return f"Ext_{addr[:6]}"


def find_csv_on_disk(addr_lower):
    """Look for an existing token-transfer CSV on disk for this address.
    Returns filepath or None."""
    all_csvs = glob.glob(os.path.join(TOKEN_TRANSFERS_DIR, "*.csv"))
    for fp in all_csvs:
        if addr_lower in os.path.basename(fp).lower():
            return fp
    return None


def download_one(url, filepath, timeout=60):
    """Download a URL to filepath. Returns (success, row_count, error_msg)."""
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
            return False, 0, "404"
        return False, 0, str(e)
    except Exception as e:
        return False, 0, str(e)


def find_or_download_csv(addr_lower, wallet_info, delay, stats, dry_run=False):
    """Reuse existing CSV or download. Returns filepath or None."""
    existing = find_csv_on_disk(addr_lower)
    if existing:
        return existing

    if dry_run:
        return None

    info = wallet_info.get(addr_lower, {})
    name = info.get("name", make_ext_name(addr_lower))
    filename = f"{name}_{addr_lower}.csv"
    filepath = os.path.join(TOKEN_TRANSFERS_DIR, filename)

    sys.stdout.write(f"    Downloading {name}... ")
    sys.stdout.flush()
    time.sleep(delay)

    url = TOKEN_TRANSFERS_URL.format(addr=addr_lower)
    ok, rows, err = download_one(url, filepath)
    stats["total_downloads"] += 1

    if ok:
        print(f"{rows} rows")
        return filepath
    elif err == "404":
        print("no on-chain data (404)")
        return None
    else:
        print(f"ERROR: {err}")
        stats["total_errors"] += 1
        return None


# ── CSV Parsing & Edge Aggregation ───────────────────────────────────────

def parse_token_transfers(csv_path):
    """Read a token-transfer CSV. Returns list of row dicts."""
    rows = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"    Warning: could not read {os.path.basename(csv_path)}: {e}")
    return rows


def extract_eoa_counterparties(rows, wallet_lower, contract_addrs):
    """From token-transfer rows, return set of unique EOA counterparty addrs."""
    eoas = set()
    for row in rows:
        for field in ("FromAddress", "ToAddress"):
            val = row.get(field, "").strip()
            if ADDR_RE.match(val):
                addr = val.lower()
                if addr != wallet_lower and addr not in contract_addrs:
                    eoas.add(addr)
    return eoas


def aggregate_edges(rows, wallet_lower, contract_addrs):
    """Build directed edge data from CSV rows.
    Returns dict of edge_key -> {from, to, tokens: {symbol: {total_amount, transfer_count, tx_hashes}}}
    Only includes EOA ↔ EOA edges (contracts excluded)."""
    edges = {}
    for row in rows:
        from_addr = row.get("FromAddress", "").strip().lower()
        to_addr = row.get("ToAddress", "").strip().lower()

        if not ADDR_RE.match(from_addr) or not ADDR_RE.match(to_addr):
            continue

        # Determine counterparty — skip if it's a contract
        if from_addr == wallet_lower:
            counterparty = to_addr
        elif to_addr == wallet_lower:
            counterparty = from_addr
        else:
            continue
        if counterparty in contract_addrs:
            continue

        edge_key = f"{from_addr}|{to_addr}"
        if edge_key not in edges:
            edges[edge_key] = {"from": from_addr, "to": to_addr, "tokens": {}}

        symbol = row.get("TokenSymbol", "UNKNOWN").strip()
        tx_hash = row.get("TxHash", "").strip()
        raw_amount = row.get("TokensTransferred", "0").strip().replace(",", "")
        try:
            amount = float(raw_amount)
        except ValueError:
            amount = 0.0

        tok = edges[edge_key]["tokens"]
        if symbol not in tok:
            tok[symbol] = {"total_amount": 0.0, "transfer_count": 0, "tx_hashes": []}

        tok[symbol]["total_amount"] += amount
        tok[symbol]["transfer_count"] += 1
        if tx_hash:
            tok[symbol]["tx_hashes"].append(tx_hash)

    return edges


def merge_edges(global_edges, new_edges):
    """Merge new_edges into global_edges, deduplicating tx_hashes."""
    for key, edge in new_edges.items():
        if key not in global_edges:
            global_edges[key] = edge
        else:
            existing = global_edges[key]
            for symbol, tdata in edge["tokens"].items():
                if symbol not in existing["tokens"]:
                    existing["tokens"][symbol] = tdata
                else:
                    et = existing["tokens"][symbol]
                    et["total_amount"] += tdata["total_amount"]
                    et["transfer_count"] += tdata["transfer_count"]
                    existing_hashes = set(et["tx_hashes"])
                    for h in tdata["tx_hashes"]:
                        if h not in existing_hashes:
                            et["tx_hashes"].append(h)
                            existing_hashes.add(h)


# ── Main Spider Loop ────────────────────────────────────────────────────

def spider_loop(cp, wallet_info, contract_addrs, args):
    """BFS spider. Modifies checkpoint in-place.

    Queue entries are [addr, depth] pairs where depth tracks BFS hops
    from the nearest seed wallet (seeds = depth 0).
    """
    processed = cp["processed"]
    queue = cp["queue"]  # list of [addr, depth]
    edges = cp["edges"]
    queue_set = {entry[0] for entry in queue}  # addr-only set for O(1) lookup
    stats = cp["stats"]
    all_known_wallets = set(wallet_info.keys())
    max_depth = args.max_depth

    iteration = 0
    while queue:
        entry = queue.pop(0)
        # Support both old format (plain string) and new format ([addr, depth])
        if isinstance(entry, str):
            addr = entry.lower()
            depth = 0
        else:
            addr = entry[0].lower()
            depth = entry[1]

        queue_set.discard(addr)

        if addr in processed:
            continue

        # Skip spam/scam
        info = wallet_info.get(addr, {})
        if info.get("type") in SKIP_TYPES:
            processed[addr] = {
                "status": "skipped_spam",
                "is_hub": False,
                "eoa_count": 0,
                "transfer_count": 0,
                "depth": depth,
            }
            save_checkpoint(cp)
            continue

        # Safety cap
        if args.max_wallets > 0:
            real_processed = sum(
                1 for d in processed.values() if d.get("status") == "complete"
            )
            if real_processed >= args.max_wallets:
                print(f"\n  Reached --max-wallets {args.max_wallets}, stopping.")
                break

        iteration += 1
        display = info.get("name", make_ext_name(addr))
        remaining = len(queue)
        depth_str = f"depth {depth}"
        print(f"\n[{iteration}] {display} ({addr[:10]}...) — {remaining} in queue — {depth_str}")

        # Step 1: Get CSV
        csv_path = find_or_download_csv(
            addr, wallet_info, args.delay, stats, dry_run=args.dry_run
        )
        if csv_path is None:
            processed[addr] = {
                "status": "no_data",
                "is_hub": False,
                "eoa_count": 0,
                "transfer_count": 0,
                "depth": depth,
            }
            save_checkpoint(cp)
            continue

        # Step 2: Parse
        rows = parse_token_transfers(csv_path)
        print(f"    {len(rows)} transfer rows")

        # Step 3: EOA counterparties
        eoas = extract_eoa_counterparties(rows, addr, contract_addrs)
        print(f"    {len(eoas)} unique EOA counterparties")

        # Step 4: Hub detection — >= threshold means not a human wallet
        is_hub = len(eoas) >= args.hub_threshold
        if is_hub:
            print(f"    ** HUB detected ({len(eoas)} EOAs >= {args.hub_threshold}) — not spidering deeper **")

        # Step 5: Aggregate edges
        wallet_edges = aggregate_edges(rows, addr, contract_addrs)
        merge_edges(edges, wallet_edges)
        print(f"    {len(wallet_edges)} directed edges")

        # Step 6: If not hub AND within depth limit, enqueue new addresses
        child_depth = depth + 1
        at_depth_limit = max_depth > 0 and child_depth > max_depth
        if not is_hub and not at_depth_limit:
            new_count = 0
            for eoa in sorted(eoas):
                if eoa not in processed and eoa not in queue_set and eoa not in contract_addrs:
                    queue.append([eoa, child_depth])
                    queue_set.add(eoa)
                    new_count += 1
                    if eoa not in all_known_wallets:
                        cp["discovered_wallets"].append(eoa)
            if new_count:
                print(f"    +{new_count} new addresses added to queue (depth {child_depth})")
        elif at_depth_limit and not is_hub:
            print(f"    Depth limit reached ({max_depth}) — not enqueuing {len(eoas)} counterparties")

        # Step 7: Mark processed
        processed[addr] = {
            "status": "complete",
            "is_hub": is_hub,
            "eoa_count": len(eoas),
            "transfer_count": len(rows),
            "depth": depth,
        }

        # Step 8: Save checkpoint
        save_checkpoint(cp)


# ── Output Generation ────────────────────────────────────────────────────

def build_network_js(cp, wallet_info, contract_addrs, seed_addrs):
    """Generate spider-network.js from checkpoint data."""

    # Build nodes from processed wallets
    nodes = {}
    for addr, data in cp["processed"].items():
        if data["status"] == "skipped_spam":
            continue
        info = wallet_info.get(addr, {})
        nodes[addr] = {
            "address": addr,
            "name": info.get("name", make_ext_name(addr)),
            "type": info.get("type", "unknown"),
            "is_hub": data.get("is_hub", False),
            "eoa_count": data.get("eoa_count", 0),
            "transfer_count": data.get("transfer_count", 0),
            "depth": data.get("depth", 0),
            "is_seed": addr in seed_addrs,
        }

    # Also include edge endpoints that weren't fully processed
    for edge_key, edge_data in cp["edges"].items():
        for field in ("from", "to"):
            addr = edge_data[field]
            if addr not in nodes and addr not in contract_addrs:
                info = wallet_info.get(addr, {})
                nodes[addr] = {
                    "address": addr,
                    "name": info.get("name", make_ext_name(addr)),
                    "type": info.get("type", "unknown"),
                    "is_hub": False,
                    "eoa_count": 0,
                    "transfer_count": 0,
                    "is_seed": addr in seed_addrs,
                }

    # Build edges array
    edges_out = []
    for edge_key, edge_data in cp["edges"].items():
        tokens = []
        total_xfers = 0
        for symbol, tdata in sorted(edge_data["tokens"].items()):
            # Deduplicate tx_hashes for final count
            unique_hashes = list(dict.fromkeys(tdata["tx_hashes"]))
            tokens.append({
                "symbol": symbol,
                "total_amount": str(tdata["total_amount"]),
                "transfer_count": len(unique_hashes),
                "tx_hashes": unique_hashes,
            })
            total_xfers += len(unique_hashes)
        edges_out.append({
            "from": edge_data["from"],
            "to": edge_data["to"],
            "tokens": tokens,
            "total_transfers": total_xfers,
        })

    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "seed_wallets": len(seed_addrs),
            "total_nodes": len(nodes),
            "total_edges": len(edges_out),
            "total_hubs": sum(1 for n in nodes.values() if n.get("is_hub")),
            "spider_complete": cp.get("status") == "complete",
        },
        "nodes": nodes,
        "edges": edges_out,
    }

    js = "/* Auto-generated by spider_network.py -- do not edit manually */\n"
    js += "var SPIDER_NETWORK = " + json.dumps(output, indent=2) + ";\n"

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js)

    print(f"\nWrote {OUTPUT_FILE}")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Edges: {len(edges_out)}")
    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"  Size:  {size_mb:.1f} MB")


def update_wallets_txt(discovered, wallet_info):
    """Append newly discovered wallets to wallets.txt."""
    new_addrs = sorted(set(a for a in discovered if a not in wallet_info))
    if not new_addrs:
        return 0
    with open(WALLETS_FILE, "a") as f:
        for addr in new_addrs:
            name = make_ext_name(addr)
            f.write(f"{addr},{name},unknown\n")
    return len(new_addrs)


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Recursive wallet network spider with graph output"
    )
    parser.add_argument("--wallets", nargs="*",
                        help="Specific seed wallet addresses (default: all non-spam/scam)")
    parser.add_argument("--hub-threshold", type=int, default=50,
                        help="Max unique EOA counterparties before hub flag (default: 50)")
    parser.add_argument("--delay", type=int, default=30,
                        help="Seconds between API downloads (default: 30)")
    parser.add_argument("--max-wallets", type=int, default=0,
                        help="Max wallets to process, 0=unlimited (default: 0)")
    parser.add_argument("--max-depth", type=int, default=10,
                        help="Max BFS hops from seed wallets, 0=unlimited (default: 10)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without downloading or writing")
    parser.add_argument("--output-only", action="store_true",
                        help="Skip spidering, just build spider-network.js from checkpoint")
    args = parser.parse_args()

    print("=" * 60)
    print("CryptBooks Recursive Wallet Network Spider")
    print(f"Time:          {datetime.now().isoformat()}")
    print(f"Hub threshold: {args.hub_threshold} EOAs (>= this = hub)")
    print(f"Max depth:     {'unlimited' if args.max_depth == 0 else args.max_depth}")
    print(f"Download delay: {args.delay}s")
    print(f"Max wallets:   {'unlimited' if args.max_wallets == 0 else args.max_wallets}")
    print(f"Resume:        {args.resume}")
    print(f"Dry run:       {args.dry_run}")
    print("=" * 60)

    # Load reference data
    wallet_info = load_wallet_data()
    contract_addrs = load_contract_addresses()
    print(f"\nLoaded {len(wallet_info)} wallets, {len(contract_addrs)} contracts")

    # Determine seed wallets
    if args.wallets:
        seed_addrs = {a.lower() for a in args.wallets}
    else:
        seed_addrs = {
            a for a, info in wallet_info.items()
            if info["type"] not in SKIP_TYPES
        }
    print(f"Seed wallets: {len(seed_addrs)}")

    # Output-only mode
    if args.output_only:
        cp = load_checkpoint()
        if cp is None:
            print("ERROR: No checkpoint file found. Run the spider first.")
            return
        build_network_js(cp, wallet_info, contract_addrs, seed_addrs)
        return

    # Load or create checkpoint
    cp = None
    if args.resume:
        cp = load_checkpoint()
        if cp:
            print(f"\nResuming from checkpoint ({len(cp['processed'])} already processed, "
                  f"{len(cp['queue'])} in queue)")
        else:
            print("No checkpoint found, starting fresh.")

    if cp is None:
        cp = new_checkpoint()
        # Seed the queue — each entry is [addr, depth] with seeds at depth 0
        cp["queue"] = [[a, 0] for a in sorted(seed_addrs)]

    # Dry run — just show the queue
    if args.dry_run:
        q_len = len(cp["queue"])
        done = len(cp["processed"])
        print(f"\n[DRY RUN] Queue: {q_len} wallets, Already processed: {done}")
        print(f"Max depth: {'unlimited' if args.max_depth == 0 else args.max_depth}")
        csvs_on_disk = len(glob.glob(os.path.join(TOKEN_TRANSFERS_DIR, "*.csv")))
        print(f"CSVs on disk: {csvs_on_disk}")
        print("No downloads or writes will be performed.")
        return

    # Run spider
    try:
        spider_loop(cp, wallet_info, contract_addrs, args)
    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving checkpoint...")
        save_checkpoint(cp)
        print(f"Resume with: python spider_network.py --resume")
        return

    # Mark complete
    cp["status"] = "complete"
    save_checkpoint(cp)

    # Update wallets.txt with discovered externals
    new_count = update_wallets_txt(cp["discovered_wallets"], wallet_info)
    if new_count:
        print(f"\nAppended {new_count} new externals to wallets.txt")

    # Generate output
    build_network_js(cp, wallet_info, contract_addrs, seed_addrs)

    # Final summary
    stats = cp["stats"]
    print(f"\n{'=' * 60}")
    print(f"SPIDER COMPLETE")
    print(f"  Total processed: {stats['total_processed']}")
    print(f"  Total hubs:      {stats['total_hubs']}")
    print(f"  Total edges:     {len(cp['edges'])}")
    print(f"  Downloads:       {stats['total_downloads']}")
    print(f"  Errors:          {stats['total_errors']}")
    print(f"  New externals:   {new_count}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
