# CryptBooksPulseChainAutoDownload

Python build scripts and Claude skills for downloading and processing PulseChain wallet data into the [CryptBooksPortal](https://github.com/CryptBooks/CryptBooksPortal) dashboard.

## Overview

This repo contains:

- **Python build scripts** that transform raw CSV data (in `SyncedData/`) into JS data files consumed by the portal
- **Claude skills** that automate downloading transaction and token transfer CSVs from the PulseChain block explorer

## Directory Layout

```
CryptBooksPulseChainAutoDownload/
  scripts/
    build_transactions.py        Reads SyncedData CSVs → transactions.js
    build_token_transfers.py     Reads SyncedData CSVs → token-transfers.js
```

## How It Works

### Data Flow

```
PulseChain Explorer API
        │
        ▼  (Claude skills download CSVs)
CryptBooksPortal/Data/SyncedData/
  ├── transactions/*.csv
  └── token-transfers/*.csv
        │
        ▼  (Python build scripts)
CryptBooksPortal/Data/
  ├── transactions.js          → var TRANSACTION_DATA = {...}
  └── token-transfers.js       → var TOKEN_TRANSFER_DATA = {...}
        │
        ▼  (loaded via <script> tags)
CryptBooksPortal HTML pages
```

### Wallet List

Both download skills read wallet addresses from:
```
CryptBooksPortal/Data/wallets.txt
```

Format (comma-delimited, one per line):
```
0xADDRESS,FriendlyName
```
Lines starting with `#` are comments. Friendly name is optional.

## Claude Skills

Three skills automate the pipeline:

| Skill | Purpose |
|-------|---------|
| `cryptBooksPulseChainTokenTransfers` | Download token transfer CSVs from PulseChain explorer |
| `cryptBooksPulseChainTransactions` | Download transaction CSVs from PulseChain explorer |
| `cryptBooksBuildPortalData` | Run both Python build scripts to generate JS data files |

### Usage

From Claude Code, invoke the skills:
```
/cryptBooksPulseChainTokenTransfers
/cryptBooksPulseChainTransactions
/cryptBooksBuildPortalData
```

## Build Scripts

### Prerequisites

- Python 3.6+ (uses only standard library)
- CSV data must already exist in `CryptBooksPortal/Data/SyncedData/`

### Running Manually

```bash
python scripts/build_transactions.py
python scripts/build_token_transfers.py
```

### Output

Each script reads CSVs from the corresponding `SyncedData/` subfolder and writes a single JS file to `CryptBooksPortal/Data/`. The JS files define global variables loaded by the portal's HTML pages via `<script>` tags.

## Status Files

Each skill writes a status file to `SyncedData/` after completion:
- `token-transfers-status.txt`
- `transactions-status.txt`
- `build-status.txt`

> **TODO:** Define a structured format (possibly `.js` with a `var SYNC_STATUS = {...}`) for portal display of import/build status.
