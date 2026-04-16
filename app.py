import os
import json
import uuid
import statistics
from datetime import datetime
from flask import Flask, request, jsonify, render_template

from parser import parse_file
from categorizer import (
    categorize_transaction, detect_recurring,
    get_all_categories, load_data, save_data, DEFAULT_RULES
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# ─── paths ───────────────────────────────────────────────────────────────────

LEARNED_RULES     = os.path.join(DATA_DIR, "learned_rules.json")
LEARNED_AMOUNTS   = os.path.join(DATA_DIR, "learned_amounts.json")
OVERRIDES         = os.path.join(DATA_DIR, "transaction_overrides.json")
CORRECTION_LOG    = os.path.join(DATA_DIR, "correction_log.json")
BUDGETS           = os.path.join(DATA_DIR, "budgets.json")
GOALS             = os.path.join(DATA_DIR, "goals.json")
BORROWED_LENT     = os.path.join(DATA_DIR, "borrowed_lent.json")
RECURRING_CACHE   = os.path.join(DATA_DIR, "recurring_cache.json")
SESSION_TXNS      = os.path.join(DATA_DIR, "session_transactions.json")


def init_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    defaults = {
        LEARNED_RULES:   {},
        LEARNED_AMOUNTS: {},
        OVERRIDES:       {},
        CORRECTION_LOG:  [],
        BUDGETS:         {},
        GOALS:           {},
        BORROWED_LENT:   [],
        RECURRING_CACHE: {},
        SESSION_TXNS:    {"transactions": [], "uploaded_at": None},
    }
    for path, default in defaults.items():
        if not os.path.exists(path):
            save_data(path, default)


init_data()


# ─── helpers ─────────────────────────────────────────────────────────────────

def compute_summary(transactions):
    summary = {}
    for txn in transactions:
        if txn.get("type") == "expense":
            cat = txn.get("category", "Uncategorized")
            summary[cat] = summary.get(cat, 0) + abs(txn.get("amount", 0))
    return {k: round(v, 2) for k, v in sorted(summary.items(), key=lambda x: -x[1])}


def compute_monthly(transactions):
    monthly = {}
    for txn in transactions:
        date_str = txn.get("date", "")
        # Parse YYYY-MM
        try:
            parts = date_str.split("/")
            if len(parts) == 3:
                ym = f"{parts[2]}-{parts[1].zfill(2)}"
            else:
                parts2 = date_str.split("-")
                if len(parts2) == 3 and len(parts2[0]) == 4:
                    ym = f"{parts2[0]}-{parts2[1].zfill(2)}"
                else:
                    ym = "Unknown"
        except Exception:
            ym = "Unknown"

        if ym not in monthly:
            monthly[ym] = {"income": 0, "expense": 0, "savings": 0}
        amount = txn.get("amount", 0)
        if amount > 0:
            monthly[ym]["income"] += amount
        else:
            monthly[ym]["expense"] += abs(amount)

    for ym in monthly:
        monthly[ym]["savings"] = round(monthly[ym]["income"] - monthly[ym]["expense"], 2)
        monthly[ym]["income"] = round(monthly[ym]["income"], 2)
        monthly[ym]["expense"] = round(monthly[ym]["expense"], 2)

    return monthly


def compute_cashflow(transactions):
    income = sum(t["amount"] for t in transactions if t.get("amount", 0) > 0)
    expense = sum(abs(t["amount"]) for t in transactions if t.get("amount", 0) < 0)
    savings = income - expense
    rate = (savings / income * 100) if income > 0 else 0
    return {
        "income": round(income, 2),
        "expense": round(expense, 2),
        "savings": round(savings, 2),
        "savings_rate": round(rate, 1),
    }


# ─── routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    try:
        if "file" not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400

        file = request.files["file"]
        filename = file.filename or "upload.csv"
        mode = request.form.get("mode", "replace")

        transactions = parse_file(file, filename)
        if not transactions:
            return jsonify({"status": "error", "message": "No transactions found in file. Check that the file has date and description columns."}), 400

        if mode == "append":
            session = load_data(SESSION_TXNS, {"transactions": [], "uploaded_at": None})
            existing = {t["txn_hash"]: t for t in session.get("transactions", [])}
            for t in transactions:
                existing[t["txn_hash"]] = t  # new wins
            transactions = list(existing.values())

        # Detect recurring
        recurring_patterns = detect_recurring(transactions)

        # Load rules
        overrides       = load_data(OVERRIDES, {})
        learned         = load_data(LEARNED_RULES, {})
        learned_amounts = load_data(LEARNED_AMOUNTS, {})

        # Categorize
        categorized = []
        for txn in transactions:
            categorized.append(categorize_transaction(txn, overrides, learned, recurring_patterns, learned_amounts))

        # Sort by date desc
        def sort_key(t):
            date_str = t.get("date", "")
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            except Exception:
                pass
            return date_str

        categorized.sort(key=sort_key, reverse=True)

        # Save session
        save_data(SESSION_TXNS, {
            "transactions": categorized,
            "uploaded_at": datetime.now().isoformat(),
        })

        # Build response
        summary  = compute_summary(categorized)
        monthly  = compute_monthly(categorized)
        cashflow = compute_cashflow(categorized)
        flagged  = [t for t in categorized if t.get("flagged")]
        recurring_count = len(recurring_patterns)

        auto_cat   = sum(1 for t in categorized if t.get("source") in ("default", "learned", "override", "recurring"))
        uncat      = sum(1 for t in categorized if t.get("category") == "Uncategorized")
        income_cnt = sum(1 for t in categorized if t.get("type") == "income")
        expense_cnt = sum(1 for t in categorized if t.get("type") == "expense")

        return jsonify({
            "status": "ok",
            "transactions": categorized,
            "summary": summary,
            "monthly": monthly,
            "cashflow": cashflow,
            "flagged": flagged,
            "recurring_count": recurring_count,
            "import_summary": {
                "total": len(categorized),
                "income_count": income_cnt,
                "expense_count": expense_cnt,
                "auto_categorized": auto_cat,
                "uncategorized": uncat,
                "flagged": len(flagged),
                "recurring": recurring_count,
            },
        })

    except Exception as e:
        import traceback
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@app.route("/learn", methods=["POST"])
def learn():
    try:
        body = request.get_json()
        merchant     = str(body.get("merchant", "")).upper().strip()
        category     = body.get("category", "")
        old_category = body.get("old_category", "")
        sample_amount = body.get("sample_amount")

        if not merchant or not category:
            return jsonify({"status": "error", "message": "merchant and category required"}), 400

        learned = load_data(LEARNED_RULES, {})
        learned[merchant] = category
        save_data(LEARNED_RULES, learned)

        # Update amounts
        if sample_amount is not None:
            try:
                amt = float(sample_amount)
                learned_amts = load_data(LEARNED_AMOUNTS, {})
                entry = learned_amts.get(merchant, {"amounts": [], "median_abs": None})
                entry["amounts"].append(round(abs(amt), 2))
                entry["median_abs"] = round(statistics.median(entry["amounts"]), 2)
                learned_amts[merchant] = entry
                save_data(LEARNED_AMOUNTS, learned_amts)
            except Exception:
                pass

        # Correction log
        log = load_data(CORRECTION_LOG, [])
        log.append({
            "merchant": merchant,
            "old": old_category,
            "new": category,
            "amount": sample_amount,
            "ts": datetime.now().isoformat(),
            "source": "learn",
        })
        save_data(CORRECTION_LOG, log)

        return jsonify({"status": "ok", "total_rules": len(learned)})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/override", methods=["POST"])
def override():
    try:
        body = request.get_json()
        txn_hash     = body.get("txn_hash", "")
        category     = body.get("category", "")
        old_category = body.get("old_category", "")
        merchant     = body.get("merchant", "")
        amount       = body.get("amount")

        if not txn_hash or not category:
            return jsonify({"status": "error", "message": "txn_hash and category required"}), 400

        overrides = load_data(OVERRIDES, {})
        overrides[txn_hash] = category
        save_data(OVERRIDES, overrides)

        log = load_data(CORRECTION_LOG, [])
        log.append({
            "txn_hash": txn_hash,
            "merchant": merchant,
            "old": old_category,
            "new": category,
            "amount": amount,
            "ts": datetime.now().isoformat(),
            "source": "override",
        })
        save_data(CORRECTION_LOG, log)

        prompt_ledger = category in ("Borrowed", "Lent")
        resp = {"status": "ok", "prompt_ledger": prompt_ledger}
        if prompt_ledger:
            resp["merchant"] = merchant
            resp["amount"] = amount
        return jsonify(resp)

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/learned-rules", methods=["GET"])
def get_learned_rules():
    try:
        rules = load_data(LEARNED_RULES, {})
        amounts = load_data(LEARNED_AMOUNTS, {})
        return jsonify({"rules": rules, "amounts": amounts})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/learned-rules/<merchant>", methods=["DELETE"])
def delete_learned_rule(merchant):
    try:
        rules = load_data(LEARNED_RULES, {})
        amounts = load_data(LEARNED_AMOUNTS, {})
        merchant_up = merchant.upper().strip()
        rules.pop(merchant_up, None)
        amounts.pop(merchant_up, None)
        save_data(LEARNED_RULES, rules)
        save_data(LEARNED_AMOUNTS, amounts)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/overrides/<txn_hash>", methods=["DELETE"])
def delete_override(txn_hash):
    try:
        overrides = load_data(OVERRIDES, {})
        overrides.pop(txn_hash, None)
        save_data(OVERRIDES, overrides)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/recurring", methods=["GET"])
def get_recurring():
    try:
        return jsonify(load_data(RECURRING_CACHE, {}))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/recurring/confirm", methods=["POST"])
def confirm_recurring():
    try:
        body = request.get_json()
        merchant = str(body.get("merchant", "")).upper().strip()
        category = body.get("category", "")

        learned = load_data(LEARNED_RULES, {})
        learned[merchant] = category
        save_data(LEARNED_RULES, learned)

        log = load_data(CORRECTION_LOG, [])
        log.append({
            "merchant": merchant,
            "old": "",
            "new": category,
            "ts": datetime.now().isoformat(),
            "source": "recurring_confirm",
        })
        save_data(CORRECTION_LOG, log)

        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/session", methods=["GET"])
def get_session():
    try:
        return jsonify(load_data(SESSION_TXNS, {"transactions": [], "uploaded_at": None}))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/budgets", methods=["GET"])
def get_budgets():
    try:
        return jsonify(load_data(BUDGETS, {}))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/budgets", methods=["POST"])
def save_budgets():
    try:
        body = request.get_json()
        save_data(BUDGETS, body)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/goals", methods=["GET"])
def get_goals():
    try:
        goals = load_data(GOALS, {})
        return jsonify(sorted(goals.values(), key=lambda g: g.get("name", "")))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/goals", methods=["POST"])
def upsert_goal():
    try:
        body = request.get_json()
        goal_id = body.get("id") or str(uuid.uuid4())
        body["id"] = goal_id
        goals = load_data(GOALS, {})
        goals[goal_id] = body
        save_data(GOALS, goals)
        return jsonify({"status": "ok", "id": goal_id})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/goals/update", methods=["POST"])
def update_goal():
    try:
        body = request.get_json()
        goal_id = body.get("id")
        saved = body.get("saved")
        goals = load_data(GOALS, {})
        if goal_id not in goals:
            return jsonify({"status": "error", "message": "Goal not found"}), 404
        goals[goal_id]["saved"] = saved
        save_data(GOALS, goals)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/goals/<goal_id>", methods=["DELETE"])
def delete_goal(goal_id):
    try:
        goals = load_data(GOALS, {})
        goals.pop(goal_id, None)
        save_data(GOALS, goals)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/ledger", methods=["GET"])
def get_ledger():
    try:
        return jsonify(load_data(BORROWED_LENT, []))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/ledger", methods=["POST"])
def add_ledger():
    try:
        body = request.get_json()
        entries = load_data(BORROWED_LENT, [])
        entry = {
            "id": str(uuid.uuid4()),
            "person": body.get("person", ""),
            "amount": float(body.get("amount", 0)),
            "direction": body.get("direction", "lent"),
            "date": body.get("date", datetime.now().strftime("%d/%m/%Y")),
            "notes": body.get("notes", ""),
            "settled": False,
        }
        entries.append(entry)
        save_data(BORROWED_LENT, entries)
        return jsonify({"status": "ok", "id": entry["id"]})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/ledger/<entry_id>/settle", methods=["POST"])
def settle_ledger(entry_id):
    try:
        entries = load_data(BORROWED_LENT, [])
        for e in entries:
            if e["id"] == entry_id:
                e["settled"] = not e.get("settled", False)
                break
        save_data(BORROWED_LENT, entries)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/ledger/<entry_id>", methods=["DELETE"])
def delete_ledger(entry_id):
    try:
        entries = load_data(BORROWED_LENT, [])
        entries = [e for e in entries if e["id"] != entry_id]
        save_data(BORROWED_LENT, entries)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/categories", methods=["GET"])
def get_categories():
    try:
        learned = load_data(LEARNED_RULES, {})
        cats = get_all_categories()
        learned_cats = list(learned.values())
        all_cats = sorted(set(cats + learned_cats))
        return jsonify(all_cats)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/stats", methods=["GET"])
def get_stats():
    try:
        rules     = load_data(LEARNED_RULES, {})
        overrides = load_data(OVERRIDES, {})
        log       = load_data(CORRECTION_LOG, [])
        session   = load_data(SESSION_TXNS, {"transactions": []})

        flagged_count = sum(1 for t in session.get("transactions", []) if t.get("flagged"))

        # Top corrected merchants
        from collections import Counter
        merchants = [e.get("merchant", "") for e in log if e.get("merchant")]
        top = Counter(merchants).most_common(5)

        return jsonify({
            "total_rules": len(rules),
            "total_overrides": len(overrides),
            "total_corrections": len(log),
            "flagged_count": flagged_count,
            "top_corrected": [{"merchant": m, "count": c} for m, c in top],
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
        
@app.route('/ping')
def ping():
    return 'ok', 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)
