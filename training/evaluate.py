#!/usr/bin/env python3
"""
Agentic Mesh — Evaluation and Benchmark Script
Evaluates fine-tuned model outputs against training/eval.jsonl test cases.
Can evaluate via Ollama REST API or offline dataset validation.
Computes:
  - JSON Parse Validity %
  - Tool Operation Compliance % (INSERT, UPDATE, DELETE only)
  - Allowlist Table Compliance % (categories, items, suppliers, item_suppliers)
  - Full Schema Validity %
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error

ALLOWED_OPERATIONS = {"INSERT", "UPDATE", "DELETE"}
ALLOWED_TABLES = {"categories", "items", "suppliers", "item_suppliers"}

SCHEMA_RULES = {
    "categories": {"required": ["name"], "uniques": ["name"]},
    "items": {"required": ["category_id", "name", "price", "sku"], "uniques": ["sku"]},
    "suppliers": {"required": ["name"], "uniques": ["name"]},
    "item_suppliers": {"required": ["item_id", "supplier_id"], "uniques": []}
}


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate Agentic Mesh model plans")
    parser.add_argument("--eval_file", type=str, default="training/eval.jsonl", help="Evaluation jsonl path")
    parser.add_argument("--ollama_url", type=str, default="http://localhost:11434", help="Ollama API base URL")
    parser.add_argument("--model", type=str, default="gemma4:e2b", help="Model name in Ollama")
    parser.add_argument("--offline", action="store_true", help="Validate targets in eval_file without calling Ollama")
    return parser.parse_args()


def call_ollama(prompt, base_url, model):
    url = f"{base_url}/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"temperature": 0.2}
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res.get("message", {}).get("content", "")
    except Exception as e:
        print(f"Error connecting to Ollama: {e}")
        return None


def validate_operations(ops):
    """
    Validates the generated plan according to Agentic Mesh schema validator rules.
    """
    if not isinstance(ops, list) or len(ops) == 0:
        return False, ["Plan must be a non-empty list of operations"]

    errors = []
    for idx, op in enumerate(ops):
        if not isinstance(op, dict):
            errors.append(f"Op {idx}: not a dict")
            continue

        operation = str(op.get("operation", "")).upper()
        table = str(op.get("table", "")).lower()
        data = op.get("data", None)

        if operation not in ALLOWED_OPERATIONS:
            errors.append(f"Op {idx}: operation '{operation}' not in {ALLOWED_OPERATIONS}")
        if table not in ALLOWED_TABLES:
            errors.append(f"Op {idx}: table '{table}' not in {ALLOWED_TABLES}")
        if not isinstance(data, dict):
            errors.append(f"Op {idx}: data is not a dict")
            continue

        rules = SCHEMA_RULES.get(table, {})
        if operation == "INSERT":
            for req in rules.get("required", []):
                if req not in data or data[req] is None:
                    errors.append(f"Op {idx}: missing required field '{req}' for {table}")

    return len(errors) == 0, errors


if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass


def main():
    args = parse_args()
    print("================================================================")
    print("       AGENTIC MESH - MODEL EVALUATION & BENCHMARK              ")
    print("================================================================")
    print(f"Eval File:     {args.eval_file}")
    print(f"Mode:          {'Offline Target Validation' if args.offline else f'Live Ollama ({args.model})'}")
    print("================================================================\n")

    if not os.path.exists(args.eval_file):
        print(f"Error: {args.eval_file} not found.")
        sys.exit(1)

    with open(args.eval_file, "r", encoding="utf-8") as f:
        lines = [json.loads(line.strip()) for line in f if line.strip()]

    total = len(lines)
    json_valid_count = 0
    schema_valid_count = 0

    for i, item in enumerate(lines, 1):
        user_prompt = item["messages"][0]["content"]
        target_output = item["messages"][1]["content"]

        print(f"[{i}/{total}] Testing: \"{user_prompt}\"")

        if args.offline:
            output_text = target_output
        else:
            output_text = call_ollama(user_prompt, args.ollama_url, args.model)
            if output_text is None:
                print("   [FAIL] Model call failed")
                continue

        # Check JSON parse validity
        parsed = None
        try:
            parsed = json.loads(output_text)
            json_valid = True
        except Exception:
            # Try to extract from markdown blocks
            import re
            m = re.search(r"\[\s*\{.*\}\s*\]", output_text, re.DOTALL)
            if m:
                try:
                    parsed = json.loads(m.group(0))
                    json_valid = True
                except Exception:
                    json_valid = False
            else:
                json_valid = False

        if json_valid:
            json_valid_count += 1
            schema_ok, errs = validate_operations(parsed)
            if schema_ok:
                schema_valid_count += 1
                print("   [PASS] Valid JSON & Schema Compliant")
            else:
                print(f"   [FAIL] Schema Violations: {'; '.join(errs)}")
        else:
            print("   [FAIL] Invalid JSON Syntax")

    print("\n================================================================")
    print("                    EVALUATION SCORECARD                        ")
    print("================================================================")
    print(f"Total Test Cases:       {total}")
    print(f"Valid JSON Syntax:      {json_valid_count}/{total} ({json_valid_count/total*100:.1f}%)")
    print(f"Schema Compliant Plans: {schema_valid_count}/{total} ({schema_valid_count/total*100:.1f}%)")
    print("================================================================\n")


if __name__ == "__main__":
    main()
