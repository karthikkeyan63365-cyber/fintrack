import os
import json
import statistics
from datetime import datetime


DEFAULT_RULES = {
    "ZOMATO": "Food", "SWIGGY": "Food", "BLINKIT": "Groceries",
    "BIGBASKET": "Groceries", "DMART": "Groceries", "ZEPTO": "Groceries",
    "JIOMART": "Groceries", "DUNZO": "Groceries",
    "UBER": "Travel", "OLA": "Travel", "RAPIDO": "Travel", "IRCTC": "Travel",
    "MAKEMYTRIP": "Travel", "REDBUS": "Travel", "GOIBIBO": "Travel", "IXIGO": "Travel",
    "AMAZON": "Shopping", "FLIPKART": "Shopping", "MYNTRA": "Shopping",
    "MEESHO": "Shopping", "AJIO": "Shopping", "NYKAA": "Shopping",
    "TATACLIQ": "Shopping", "SNAPDEAL": "Shopping",
    "NETFLIX": "Entertainment", "HOTSTAR": "Entertainment", "DISNEY": "Entertainment",
    "SPOTIFY": "Entertainment", "YOUTUBE": "Entertainment",
    "BOOKMYSHOW": "Entertainment", "PRIMEVIDEO": "Entertainment",
    "SONYLIV": "Entertainment", "ZEE5": "Entertainment",
    "AIRTEL": "Bills", "JIO": "Bills", "BSNL": "Bills", "TNEB": "Bills",
    "BESCOM": "Bills", "TATAPOWER": "Bills", "ADANIGAS": "Bills",
    "INDANE": "Bills", "BHARATGAS": "Bills",
    "LIC": "Insurance", "NIACL": "Insurance", "STARHEALTH": "Insurance",
    "HDFCLIFE": "Insurance", "ICICIPRU": "Insurance", "MAXLIFE": "Insurance",
    "APOLLO": "Healthcare", "MEDPLUS": "Healthcare", "PRACTO": "Healthcare",
    "NETMEDS": "Healthcare", "PHARMEASY": "Healthcare", "TATAHEALTH": "Healthcare",
    "BYJU": "Education", "UNACADEMY": "Education", "UDEMY": "Education",
    "COURSERA": "Education", "WHITEHATJR": "Education", "VEDANTU": "Education",
    "SALARY": "Income", "STIPEND": "Income", "FREELANCE": "Income",
    "ATM": "Cash", "CASH WITHDRAWAL": "Cash",
    "NEFT": "Transfer", "IMPS": "Transfer", "RTGS": "Transfer",
    "PHONEPE": "Transfer", "GPAY": "Transfer", "PAYTM": "Transfer",
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
RECURRING_CACHE = os.path.join(DATA_DIR, "recurring_cache.json")


def load_data(path, default_val):
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        return default_val
    except Exception:
        return default_val


def save_data(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def categorize_transaction(txn, overrides, learned, recurring_patterns, learned_amounts):
    txn = dict(txn)
    merchant = txn.get("merchant", "").upper().strip()
    amount = txn.get("amount", 0)
    txn_hash = txn.get("txn_hash", "")

    # 1. Override by hash
    if txn_hash in overrides:
        txn["category"] = overrides[txn_hash]
        txn["source"] = "override"
        txn["flagged"] = False
        txn["flag_reason"] = ""
        return txn

    # 2. Recurring pattern
    if merchant in recurring_patterns:
        pat = recurring_patterns[merchant]
        typical = pat.get("typical_amount", 0)
        if typical and abs(amount) != 0:
            ratio = abs(abs(amount) - typical) / typical if typical != 0 else 1
            if ratio <= 0.15:
                txn["category"] = pat.get("suggested_category", "Misc")
                txn["source"] = "recurring"
                txn["flagged"] = False
                txn["flag_reason"] = ""
                return txn

    # 3. Learned rules
    if merchant in learned:
        category = learned[merchant]
        amt_data = learned_amounts.get(merchant, {})
        median_abs = amt_data.get("median_abs")
        flagged = False
        flag_reason = ""
        if median_abs and median_abs > 0:
            abs_amount = abs(amount)
            if abs_amount > 3 * median_abs or abs_amount < median_abs / 3:
                flagged = True
                flag_reason = f"Amount ₹{abs_amount:.0f} is unusual for {merchant} (typical: ₹{median_abs:.0f})"
        txn["category"] = category
        txn["source"] = "learned"
        txn["flagged"] = flagged
        txn["flag_reason"] = flag_reason
        return txn

    # 4. Default rules — substring match
    for key, cat in DEFAULT_RULES.items():
        if key in merchant:
            txn["category"] = cat
            txn["source"] = "default"
            txn["flagged"] = False
            txn["flag_reason"] = ""
            return txn

    # 5. Fallback
    txn["category"] = "Uncategorized"
    txn["source"] = "uncategorized"
    txn["flagged"] = False
    txn["flag_reason"] = ""
    return txn


def _parse_date_to_ym(date_str):
    """Return 'YYYY-MM' from a DD/MM/YYYY string, or None on failure."""
    try:
        parts = str(date_str).strip().split("/")
        if len(parts) == 3:
            d, m, y = parts
            return f"{y.zfill(4)}-{m.zfill(2)}"
        # Try YYYY-MM-DD
        parts2 = str(date_str).strip().split("-")
        if len(parts2) == 3 and len(parts2[0]) == 4:
            return f"{parts2[0]}-{parts2[1].zfill(2)}"
    except Exception:
        pass
    return None


def detect_recurring(transactions):
    from collections import defaultdict

    merchant_txns = defaultdict(list)
    for txn in transactions:
        merchant = txn.get("merchant", "").upper().strip()
        if not merchant:
            continue
        merchant_txns[merchant].append(txn)

    recurring = {}

    for merchant, txns in merchant_txns.items():
        if len(txns) < 2:
            continue

        # Group by month
        monthly = defaultdict(list)
        for txn in txns:
            ym = _parse_date_to_ym(txn.get("date", ""))
            if ym:
                monthly[ym].append(abs(txn.get("amount", 0)))

        if len(monthly) < 2:
            continue

        # Max per month
        monthly_max = {ym: max(amounts) for ym, amounts in monthly.items()}
        amounts_list = list(monthly_max.values())
        med = statistics.median(amounts_list)

        # All within ±20% of median
        if med == 0:
            continue
        all_within = all(abs(a - med) / med <= 0.20 for a in amounts_list)
        if not all_within:
            continue

        # Suggested category
        if 3000 <= med <= 30000:
            suggested = "Rent"
        elif med < 500:
            suggested = "Misc"
        else:
            suggested = "Transfer"

        recurring[merchant] = {
            "merchant": merchant,
            "typical_amount": round(med, 2),
            "months_seen": sorted(monthly_max.keys()),
            "frequency": "monthly",
            "suggested_category": suggested,
        }

    save_data(RECURRING_CACHE, recurring)
    return recurring


def get_all_categories():
    defaults = sorted(set(DEFAULT_RULES.values()))
    extras = ["Borrowed", "Lent", "Rent", "EMI", "Investment", "Personal Care",
              "Uncategorized", "Income", "Transfer", "Cash", "Misc"]
    all_cats = sorted(set(defaults + extras))
    return all_cats
