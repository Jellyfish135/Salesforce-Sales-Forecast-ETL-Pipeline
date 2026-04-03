# Salesforce Sales Forecast ETL Pipeline

A Python ETL pipeline that transforms a multi-sheet Excel sales forecast workbook into Salesforce-ready CSVs for bulk upsert — eliminating manual data entry and reducing sync time by over 85%.

Built during a software engineering internship to automate a recurring manual process that previously required 15+ hours of work per upload cycle.

---

## What It Does

Sales forecast data lives in a structured Excel workbook maintained by the sales team. This pipeline:

1. **Extracts** data from a multi-sheet `.xlsm` workbook using dynamic header detection (no hardcoded row numbers)
2. **Transforms** the raw data — normalizing sales statuses, resolving account names, generating composite keys, and computing transaction dates
3. **Loads** the output as two structured CSVs ready for Salesforce bulk upsert:
   - `output_opportunity.csv` — one row per sales opportunity
   - `output_products.csv` — one row per monthly revenue line item

After uploading opportunities, the pipeline reads back the Salesforce-assigned IDs from the Dataloader success file and stitches them into the product CSV automatically.

---

## Architecture

```
Excel Workbook (.xlsm)
        │
        ▼
┌──────────────────┐
│   load_data()    │  Dynamic header detection via "Sales Status" scan
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  build_core_df() │  Status normalization · Account mapping · Key generation
└────────┬─────────┘
         │
    ┌────┴────┐
    ▼         ▼
Opportunity  Product
   CSV        CSV   ← stitched with Salesforce IDs from success file
```

---

## Pipeline Stages

### Stage 1 — Opportunity Export

```bash
# Set MODE = "opportunity" in etl.py, then run:
python etl.py
```

Outputs `output_opportunity.csv`. Upload this to Salesforce via Dataloader.io using `OPX_Composite_Key__c` as the External ID.

### Stage 2 — Product Export

Download the Dataloader.io success file → save as `success_opp.csv`, then:

```bash
# Set MODE = "product" in etl.py, then run:
python etl.py
```

The pipeline reads `success_opp.csv` to map each opportunity to its Salesforce ID (`006...`), then outputs `output_products.csv` with one row per non-zero monthly revenue entry.

---

## Key Design Decisions

| Problem | Solution |
|---|---|
| Header row position varies per sheet | Scan first 80 rows for "Sales Status" dynamically |
| Excel date columns import as datetime objects | `clean_header()` normalizes all headers to `Mon-YY` string format |
| Same opportunity appears across multiple sheets | Composite key `OPX\|FiscalYear` deduplicates at upsert |
| Product rows need parent Opportunity SF ID | ID skimming from Dataloader success file, with unmatched key warnings |
| Currency values include symbols and commas | `parse_currency()` strips `¥`, `$`, `,` before numeric conversion |
| BOM characters in CSV headers | All CSVs written with `utf-8-sig` encoding |

---

## Salesforce Setup (One-Time)

Add two custom External ID fields before first use:

| Object | Field API Name | Type | Options |
|---|---|---|---|
| Opportunity | `OPX_Composite_Key__c` | Text(80) | External ID ✅ Unique ✅ |
| OpportunityLineItem | `OPX_Product_Key__c` | Text(100) | External ID ✅ Unique ✅ |

---

## Configuration

Edit the `CONFIGURATION` section at the top of `etl.py`:

```python
MODE        = "opportunity"     # "opportunity" or "product"
FILE        = Path("SALES-FORECAST.xlsm")
SHEET       = "2026"
FISCAL_YEAR = 2026

# Map Excel short names → Salesforce Account names
ACCOUNT_MAP = {
    "ACME":   "Acme Corporation",
    "GLOBEX": "Globex Industries",
}

# Map Excel short names → Salesforce user full names
USER_MAP = {
    "Alice": "Alice Johnson",
    "Bob":   "Bob Smith",
}
```

---

## Requirements

```
pandas
openpyxl
```

```bash
pip install pandas openpyxl
```

Python 3.10+ required (uses `float | None` union type hint).

---

## Output Format

### output_opportunity.csv

| Column | Description |
|---|---|
| `OPX_Composite_Key__c` | External ID for upsert: `OPX-1234\|2026` |
| `Account_Name` | Resolved Salesforce account name |
| `Customer Project` | Opportunity name (Customer + Project Name) |
| `Sales Status` | Normalized Salesforce stage name |
| `Closed Date` | Jan 1 or Dec 31 depending on OPX suffix |
| `Transaction Date` | Last day of month before first revenue |
| `Opportunity_Owner` | Resolved Salesforce user full name |

### output_products.csv

| Column | Description |
|---|---|
| `OPX_Product_Key__c` | External ID: `OPX-1234\|2026\|M06 Revenue` |
| `Opportunity_ID` | Salesforce Opportunity ID (006...) from success file |
| `Product_Name` | e.g. `M06 Revenue` |
| `UnitPrice` | Revenue value for that month |
| `Transaction_Date` | First day of the revenue month |

---

## Error Handling

- Missing `Sales Status` header → exits with a descriptive message
- Missing success file → exits before product stage begins
- Unmatched Salesforce IDs → logged as warnings, not silent failures
- Duplicate composite keys in success file → flagged with full row output
- Invalid currency values → skipped via `pd.to_numeric(errors="coerce")`
# Salesforce-Sales-Forecast-ETL-Pipeline
