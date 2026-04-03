"""
Salesforce Sales Forecast ETL Pipeline
=======================================

Transforms a multi-sheet Excel sales forecast workbook into
Salesforce-ready CSVs for bulk upsert via Dataloader.io.

WORKFLOW
--------
Step 1: Set MODE = "opportunity"  → generates output_opportunity.csv
        Upsert in Salesforce using OPX_Composite_Key__c as External ID

Step 2: Download success file from Dataloader.io → save as success_opp.csv

Step 3: Set MODE = "product"      → generates output_products.csv
        Upsert OpportunityLineItem using OPX_Product_Key__c as External ID

SALESFORCE PREREQUISITES (one-time setup)
-----------------------------------------
- Opportunity object:        add Text(80)  field OPX_Composite_Key__c  [External ID, Unique]
- OpportunityLineItem object: add Text(100) field OPX_Product_Key__c   [External ID, Unique]
"""

import pandas as pd
import re
import sys
from pathlib import Path

# =============================================================================
# CONFIGURATION — edit this section only
# =============================================================================

MODE        = "opportunity"       # "opportunity" or "product"
FILE        = Path("SALES-FORECAST.xlsm")
SHEET       = "2026"
FISCAL_YEAR = 2026

OUT_OPP     = Path("output_opportunity.csv")
OUT_PROD    = Path("output_products.csv")
SUCCESS_FILE = Path("success_opp.csv")

# Month columns expected in the Excel sheet (adjust to match your workbook)
MONTHS = [
    "Jan-26", "Feb-26", "Mar-26", "Apr-26", "May-26", "Jun-26",
    "Jul-26", "Aug-26", "Sep-26", "Oct-26", "Nov-26", "Dec-26",
]

# Salesforce picklist values — map raw Excel values to valid SF stage names
STATUS_MAP = {
    "opp identified": "Needs Analysis",
    "needs analysis": "Needs Analysis",
    "proposal":       "Proposal",
    "negotiation":    "Negotiation",
    "nego":           "Negotiation",
    "po":             "Closed Won",
    "delivered":      "Closed Won",
    "delivery & po":  "Closed Won",
}

# Rows with these statuses are excluded entirely
EXCLUDE_STATUSES = {"seed", "lost", "closed lost"}

# Short name in Excel → full name in Salesforce (add your team members here)
USER_MAP = {
    "Alice":  "Alice Johnson",
    "Bob":    "Bob Smith",
    "Carol":  "Carol White",
}

# Customer short name → Salesforce Account name (add your accounts here)
ACCOUNT_MAP = {
    "ACME":    "Acme Corporation",
    "GLOBEX":  "Globex Industries",
    "INITECH": "Initech LLC",
}

# =============================================================================
# HELPERS
# =============================================================================

def normalize(s: str) -> str:
    """Lowercase + collapse whitespace for robust string matching."""
    return re.sub(r"\s+", " ", str(s).replace("\n", " ").replace("\r", " ")).strip().lower()


def clean_header(col) -> str:
    """Convert date-like column headers to 'Mon-YY' format; leave others as-is."""
    try:
        return pd.to_datetime(col).strftime("%b-%y")
    except Exception:
        return re.sub(r"\s+", " ", str(col).replace("\n", " ").replace("\r", " ")).strip()


def clean_status(raw: str) -> str:
    """Strip leading numeric prefixes e.g. '3 - Proposal' → 'proposal'."""
    return re.sub(r"^\d+\s*-\s*", "", str(raw).strip().lower()).strip()


def parse_currency(value) -> float | None:
    """Strip currency symbols/commas and convert to float. Returns None if invalid."""
    cleaned = str(value).replace("¥", "").replace("$", "").replace(",", "").strip()
    result = pd.to_numeric(cleaned, errors="coerce")
    return None if pd.isna(result) else float(result)


def make_composite_key(opx: str, fiscal_year: int) -> str:
    """Opportunity-level dedup key: 'OPX-1234|2026'"""
    return f"{str(opx).strip()}|{fiscal_year}"


def make_product_key(opx: str, fiscal_year: int, month_label: str) -> str:
    """Product-level dedup key: 'OPX-1234|2026|M01 Revenue'"""
    return f"{str(opx).strip()}|{fiscal_year}|{month_label}"


def get_transaction_date_from_label(month_label: str, fiscal_year: int) -> str:
    """'M06 Revenue' → '2026-06-01'"""
    match = re.search(r"M(\d{2})", month_label)
    if not match:
        return ""
    month_num = int(match.group(1))
    return f"{fiscal_year}-{str(month_num).zfill(2)}-01"

# =============================================================================
# STEP 1 — LOAD & CLEAN DATA
# =============================================================================

def load_data(file: Path, sheet: str) -> pd.DataFrame:
    """
    Dynamically locate the header row by scanning for 'Sales Status',
    then load the full dataframe with cleaned column names.
    """
    print(f"Loading: {file.name} / sheet '{sheet}'")
    preview = pd.read_excel(file, sheet_name=sheet, header=None, nrows=80)

    mask = preview.apply(lambda col: col.astype(str).map(normalize).eq("sales status"))
    hits = mask.stack()
    if hits.empty or not hits.any():
        sys.exit(
            "ERROR: 'Sales Status' header not found in the first 80 rows. "
            "Check the sheet name or header row position."
        )

    header_row = hits.idxmax()[0]
    print(f"  → Header row detected at index {header_row}")

    df = pd.read_excel(file, sheet_name=sheet, header=int(header_row))
    df.columns = [clean_header(c) for c in df.columns]
    return df


def build_core_df(df: pd.DataFrame, fiscal_year: int) -> pd.DataFrame:
    """Apply filtering, field mapping, and derived column logic."""

    # Drop rows with no OPX number
    before = len(df)
    df = df.dropna(subset=["OPX Project number"]).copy()
    print(f"  → Dropped {before - len(df)} rows with empty OPX number")

    # Account name lookup
    customer_col = df["Customer"].fillna("").astype(str).str.strip()
    df["Account_Name"] = customer_col.map(ACCOUNT_MAP).fillna(customer_col)

    # Opportunity name = Customer + Project Name
    df["Customer Project"] = (
        customer_col + " " + df["Project Name"].fillna("").astype(str).str.strip()
    ).str.strip()

    # Normalize and map Sales Status; exclude unwanted statuses
    df["Sales Status"] = (
        df["Sales Status"].fillna("").astype(str).map(clean_status).map(STATUS_MAP)
    )
    before = len(df)
    df = df[~df["Sales Status"].str.lower().isin(EXCLUDE_STATUSES)].copy()
    print(f"  → Excluded {before - len(df)} rows with status: {EXCLUDE_STATUSES}")

    # Opportunity owner
    sales_col = df["Sales"].fillna("").astype(str).str.strip()
    df["Opportunity_Owner"] = sales_col.map(USER_MAP).fillna(sales_col)

    # Closed Date logic: suffix "-01" records get Dec 31; others get Jan 1
    opx    = df["OPX Project number"].astype(str).str.strip()
    base   = opx.str.rsplit("-", n=1).str[0]
    suffix = opx.str.rsplit("-", n=1).str[-1].str.zfill(2)
    has_01 = suffix.eq("01").groupby(base).transform("any")
    df["Closed Date"] = pd.to_datetime(f"{fiscal_year}-01-01")
    df.loc[has_01, "Closed Date"] = pd.to_datetime(f"{fiscal_year}-12-31")

    # Transaction Date: last day of month before first non-zero revenue month
    month_cols_present = [m for m in MONTHS if m in df.columns]
    df["Transaction Date"] = df.apply(
        lambda row: _get_transaction_date(row, month_cols_present), axis=1
    )

    df["Composite_Key"] = opx.apply(lambda x: make_composite_key(x, fiscal_year))
    df["Fiscal Year"]   = fiscal_year
    return df


def _get_transaction_date(row: pd.Series, month_cols: list) -> str:
    for m in month_cols:
        val = parse_currency(row[m])
        if val and val > 0:
            first_of_month = pd.to_datetime(m, format="%b-%y")
            return (first_of_month - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return ""

# =============================================================================
# STEP 2 — EXPORT OPPORTUNITY CSV
# =============================================================================

def export_opportunity(df: pd.DataFrame, out_path: Path) -> None:
    opp_cols = [
        "Fiscal Year", "Account_Name", "OPX Project number",
        "Composite_Key", "Customer Project", "Sales Status",
        "Closed Date", "Transaction Date", "Opportunity_Owner",
    ]
    out = df[opp_cols].copy().rename(columns={"Composite_Key": "OPX_Composite_Key__c"})
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Opportunity CSV → {out_path}  ({len(out)} rows)")
    print("Next: upsert in Salesforce using OPX_Composite_Key__c as External ID.")

# =============================================================================
# STEP 3 — ID SKIM + EXPORT PRODUCT CSV
# =============================================================================

def build_id_map(success_file: Path) -> dict:
    """
    Read the Dataloader.io success file and map
    OPX_Composite_Key__c → Salesforce Opportunity ID (006...).
    """
    if not success_file.exists():
        sys.exit(
            f"ERROR: Success file not found at {success_file}\n"
            "Download it from Dataloader.io after uploading opportunities."
        )

    df_s = pd.read_csv(success_file, encoding="utf-8-sig")
    df_s.columns = [c.strip() for c in df_s.columns]

    required = {"OPX_Composite_Key__c", "ID"}
    missing  = required - set(df_s.columns)
    if missing:
        sys.exit(f"ERROR: Success file missing columns: {missing}\nFound: {list(df_s.columns)}")

    duplicates = df_s[df_s.duplicated("OPX_Composite_Key__c", keep=False)]
    if not duplicates.empty:
        print(f"⚠️  WARNING: {len(duplicates)} duplicate composite keys in success file")
        print(duplicates[["OPX_Composite_Key__c", "ID"]].to_string(index=False))

    id_map = df_s.set_index("OPX_Composite_Key__c")["ID"].to_dict()
    print(f"  → Loaded {len(id_map)} Salesforce IDs")
    return id_map


def export_products(df: pd.DataFrame, id_map: dict, out_path: Path) -> None:
    month_cols_present = [m for m in MONTHS if m in df.columns]
    month_to_label = {
        m: f"M{str(i).zfill(2)} Revenue"
        for i, m in enumerate(month_cols_present, start=1)
    }

    product_rows   = []
    unmatched_keys = set()

    for _, row in df.iterrows():
        key   = row["Composite_Key"]
        sf_id = id_map.get(key)
        if sf_id is None:
            unmatched_keys.add(key)

        for m_col, label in month_to_label.items():
            val = parse_currency(row[m_col])
            if val and val > 0:
                product_rows.append({
                    "Opportunity_ID":       sf_id,
                    "OPX_Project_Number":   row["OPX Project number"],
                    "OPX_Composite_Key__c": key,
                    "OPX_Product_Key__c":   make_product_key(
                                                row["OPX Project number"],
                                                row["Fiscal Year"],
                                                label
                                            ),
                    "Product_Name":         label,
                    "UnitPrice":            val,
                    "Quantity":             1,
                    "Transaction_Date":     get_transaction_date_from_label(
                                                label, row["Fiscal Year"]
                                            ),
                })

    if unmatched_keys:
        print(f"\n⚠️  WARNING: {len(unmatched_keys)} records had no Salesforce ID match:")
        for k in sorted(unmatched_keys):
            print(f"   - {k}")

    out = pd.DataFrame(product_rows)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    mapped = out["Opportunity_ID"].notna().sum()
    print(f"\n✅ Product CSV → {out_path}  ({len(out)} rows, {mapped}/{len(out)} IDs mapped)")
    print("Next: upsert in Salesforce using OPX_Product_Key__c as External ID.")

# =============================================================================
# MAIN
# =============================================================================

def main():
    print(f"{'='*60}")
    print(f"  Mode: {MODE.upper()} | Fiscal Year: {FISCAL_YEAR}")
    print(f"{'='*60}\n")

    df = load_data(FILE, SHEET)
    df = build_core_df(df, FISCAL_YEAR)

    if MODE == "opportunity":
        export_opportunity(df, OUT_OPP)
    elif MODE == "product":
        id_map = build_id_map(SUCCESS_FILE)
        export_products(df, id_map, OUT_PROD)
    else:
        sys.exit(f"ERROR: Unknown MODE '{MODE}'. Use 'opportunity' or 'product'.")


if __name__ == "__main__":
    main()
