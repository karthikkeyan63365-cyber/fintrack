import hashlib
import re
import os
import json
import pandas as pd
from datetime import datetime


# ─── helpers ────────────────────────────────────────────────────────────────

def make_hash(date, description, amount):
    raw = f"{date}|{description}|{str(round(float(amount), 2))}"
    return hashlib.sha256(raw.encode()).hexdigest()


DATE_FORMATS = [
    "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y",
    "%Y-%m-%d", "%Y/%m/%d",
    "%d %b %Y", "%d-%b-%Y", "%d %b %y", "%d-%b-%y",
    "%d %B %Y", "%d-%B-%Y",
    "%-d/%m/%Y", "%-d-%-m-%Y",
]


def normalize_date(raw):
    if not raw or (isinstance(raw, float)):
        return str(raw)
    raw = str(raw).strip()
    # Handle pandas Timestamp
    if "Timestamp" in raw:
        try:
            ts = pd.Timestamp(raw)
            return ts.strftime("%d/%m/%Y")
        except Exception:
            pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    # Try pandas auto-parse
    try:
        return pd.to_datetime(raw, dayfirst=True).strftime("%d/%m/%Y")
    except Exception:
        pass
    return raw  # keep as-is, never crash


def clean_amount(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).replace("₹", "").replace(",", "").replace(" ", "").strip()
    if s in ("", "-", "—", "nan", "None"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def extract_merchant(description):
    s = str(description).upper().strip()
    # Strip leading protocol tokens
    prefixes = [
        r"^(UPI|NEFT|IMPS|RTGS|POS|ACH|MMT|PHONEPE|NACH|ECS|BILLPAY|IB|MB|INB)\s*[-/]\s*",
        r"^(UPI|NEFT|IMPS|RTGS|POS|ACH|MMT|PHONEPE|NACH|ECS|BILLPAY|IB|MB|INB)\s+",
    ]
    for pat in prefixes:
        s = re.sub(pat, "", s, flags=re.IGNORECASE)
    # Split on first @, /, |
    for sep in ["@", "/", "|"]:
        if sep in s:
            s = s.split(sep)[0]
            break
    # Remove trailing digits, hyphens, underscores
    s = re.sub(r"[\d\-_]+$", "", s)
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    # Truncate
    return s[:40]


# ─── column detection ───────────────────────────────────────────────────────

def detect_col(columns, keywords):
    for col in columns:
        cl = str(col).lower().strip()
        for kw in keywords:
            if kw in cl:
                return col
    return None


def map_columns(columns):
    cols = list(columns)
    date_col = detect_col(cols, ["date", "dt", "value date", "txn date", "trans date", "transaction date"])
    desc_col = detect_col(cols, ["narration", "description", "particulars", "remarks", "details", "transaction details"])
    debit_col = detect_col(cols, ["debit", "withdrawal", "dr", "debit amount", "debit amt", "withdrawl"])
    credit_col = detect_col(cols, ["credit", "deposit", "cr", "credit amount", "credit amt"])
    amount_col = detect_col(cols, ["amount", "amt"])
    balance_col = detect_col(cols, ["balance", "bal"])
    return date_col, desc_col, debit_col, credit_col, amount_col, balance_col


# ─── row → transaction ──────────────────────────────────────────────────────

def build_txn(date_raw, desc_raw, debit_raw, credit_raw, amount_raw, balance_raw):
    date = normalize_date(date_raw)
    description = str(desc_raw).strip() if desc_raw else ""
    merchant = extract_merchant(description)

    debit = clean_amount(debit_raw)
    credit = clean_amount(credit_raw)
    amount_single = clean_amount(amount_raw)
    balance = clean_amount(balance_raw)

    if debit is not None and debit > 0:
        amount = -abs(debit)
    elif credit is not None and credit > 0:
        amount = abs(credit)
    elif amount_single is not None:
        amount = amount_single
    else:
        return None

    if amount == 0:
        return None

    txn_type = "income" if amount > 0 else "expense"
    txn_hash = make_hash(date, description, amount)

    return {
        "txn_hash": txn_hash,
        "date": date,
        "description": description,
        "merchant": merchant,
        "amount": round(amount, 2),
        "balance": balance,
        "type": txn_type,
    }


# ─── CSV / Excel parser ─────────────────────────────────────────────────────

def parse_csv_excel(file, filename):
    ext = filename.lower().rsplit(".", 1)[-1]
    try:
        if ext in ("xls", "xlsx", "xlsm"):
            df = pd.read_excel(file, header=None, dtype=str)
        else:
            # Try common encodings
            try:
                df = pd.read_csv(file, header=None, dtype=str, encoding="utf-8")
            except Exception:
                file.seek(0)
                df = pd.read_csv(file, header=None, dtype=str, encoding="latin-1")
    except Exception as e:
        raise ValueError(f"Could not read file: {e}")

    # Find header row
    header_row = None
    for i, row in df.iterrows():
        vals = [str(v).lower() for v in row.values if pd.notna(v)]
        has_date = any("date" in v or "dt" in v for v in vals)
        has_desc = any(k in v for v in vals for k in ["narration", "description", "particular", "remark", "detail"])
        if has_date and has_desc:
            header_row = i
            break

    if header_row is None:
        # Try first row as header
        header_row = 0

    df.columns = df.iloc[header_row].values
    df = df.iloc[header_row + 1:].reset_index(drop=True)
    df = df.dropna(how="all")

    date_col, desc_col, debit_col, credit_col, amount_col, balance_col = map_columns(df.columns)

    if date_col is None or desc_col is None:
        raise ValueError("Could not detect required columns (date, description) in file")

    transactions = []
    seen_hashes = set()

    for _, row in df.iterrows():
        date_raw = row.get(date_col)
        desc_raw = row.get(desc_col)
        debit_raw = row.get(debit_col) if debit_col else None
        credit_raw = row.get(credit_col) if credit_col else None
        amount_raw = row.get(amount_col) if amount_col else None
        balance_raw = row.get(balance_col) if balance_col else None

        if pd.isna(date_raw) or str(date_raw).strip() in ("", "nan", "None"):
            continue
        if pd.isna(desc_raw) or str(desc_raw).strip() in ("", "nan", "None"):
            continue

        txn = build_txn(date_raw, desc_raw, debit_raw, credit_raw, amount_raw, balance_raw)
        if txn and txn["txn_hash"] not in seen_hashes:
            seen_hashes.add(txn["txn_hash"])
            transactions.append(txn)

    return transactions


# ─── PDF parser ─────────────────────────────────────────────────────────────

def parse_pdf(file):
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber is required for PDF parsing. Run: pip install pdfplumber")

    transactions = []
    seen_hashes = set()

    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                # Find header row
                header_row_idx = None
                for i, row in enumerate(table):
                    if row is None:
                        continue
                    vals = [str(v).lower() if v else "" for v in row]
                    has_date = any("date" in v or "dt" in v for v in vals)
                    has_desc = any(k in v for v in vals for k in ["narration", "description", "particular", "remark"])
                    if has_date and has_desc:
                        header_row_idx = i
                        break

                if header_row_idx is None:
                    continue

                headers = table[header_row_idx]
                date_col, desc_col, debit_col, credit_col, amount_col, balance_col = map_columns(headers)

                if date_col is None or desc_col is None:
                    continue

                col_indices = {h: i for i, h in enumerate(headers) if h}

                def get_val(row, col):
                    if col is None:
                        return None
                    idx = col_indices.get(col)
                    if idx is None or idx >= len(row):
                        return None
                    v = row[idx]
                    return v if v else None

                for row in table[header_row_idx + 1:]:
                    if not row or all((v is None or str(v).strip() == "") for v in row):
                        continue
                    txn = build_txn(
                        get_val(row, date_col),
                        get_val(row, desc_col),
                        get_val(row, debit_col),
                        get_val(row, credit_col),
                        get_val(row, amount_col),
                        get_val(row, balance_col),
                    )
                    if txn and txn["txn_hash"] not in seen_hashes:
                        seen_hashes.add(txn["txn_hash"])
                        transactions.append(txn)

    # Fallback: text parsing if too few transactions found
    if len(transactions) < 3:
        transactions = []
        seen_hashes = set()
        pattern = re.compile(
            r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s+(.+?)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)"
        )
        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines = text.split("\n")
                for line in lines:
                    m = pattern.search(line)
                    if m:
                        date_raw = m.group(1)
                        desc_raw = m.group(2).strip()
                        amt1 = m.group(3)
                        amt2 = m.group(4)
                        # Determine debit/credit from context
                        line_upper = line.upper()
                        if "DR" in line_upper or "DEBIT" in line_upper:
                            debit_raw, credit_raw = amt1, None
                        elif "CR" in line_upper or "CREDIT" in line_upper:
                            debit_raw, credit_raw = None, amt1
                        else:
                            # Treat as debit by default
                            debit_raw, credit_raw = amt1, None

                        txn = build_txn(date_raw, desc_raw, debit_raw, credit_raw, None, amt2)
                        if txn and txn["txn_hash"] not in seen_hashes:
                            seen_hashes.add(txn["txn_hash"])
                            transactions.append(txn)

    return transactions


# ─── main entry point ────────────────────────────────────────────────────────

def parse_file(file, filename):
    fn_lower = filename.lower()
    if fn_lower.endswith(".pdf"):
        return parse_pdf(file)
    elif fn_lower.endswith((".csv", ".txt")):
        return parse_csv_excel(file, filename)
    elif fn_lower.endswith((".xls", ".xlsx", ".xlsm")):
        return parse_csv_excel(file, filename)
    else:
        # Try CSV as fallback
        return parse_csv_excel(file, filename)
