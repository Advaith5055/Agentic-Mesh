# Agentic Mesh — AI Fine-Tuning & Model Guidance (Gemma / QLoRA)

This directory contains the complete pipeline for fine-tuning compact local language models (such as **Google Gemma 3 1B**, **Gemma 2B**, or **Gemma 4 E2B**) using **4-bit QLoRA** to function as safe, deterministic operation planners for the Agentic Mesh distributed database.

---

## 1. Overview & Architectural Role

In Agentic Mesh, AI models **never** receive raw SQL execution privileges or shell access. Instead, they act as semantic planners that translate natural language requests into structured tool operations:

```
[ User Request ]
       │
       ▼
[ Local LLM (Gemma) ] ── (Low Temp = 0.2)
       │
       ▼
[ Structured JSON Plan ]
[ { "operation": "INSERT", "table": "items", "data": { ... } } ]
       │
       ▼
[ JavaScript Schema Validator & Allowlists (<1ms) ]
       │
       ├── INVALID  ──► Rollback / Reject with detailed error
       │
       ▼ VALID
[ Safe Atomic SQLite Transaction (Write + _mesh_log) ]
       │
       ▼
[ GossipSub Mesh Broadcast to P2P Peers ]
```

Fine-tuning ensures the model:
1. Emits **strict JSON arrays** without conversational filler or Markdown fences.
2. Only targets allowed tables: `categories`, `items`, `suppliers`, `item_suppliers`.
3. Only uses supported operations: `INSERT`, `UPDATE`, `DELETE`.
4. Accurately maps entity names to primary/foreign key integer IDs.
5. Respects 3NF normalization rules and table schemas.

---

## 2. Directory Contents

- **`train.jsonl`**: 32 diverse, 3NF-compliant training examples covering single and multi-step inserts, updates, deletes, and junction table associations.
- **`eval.jsonl`**: 12 held-out evaluation examples for validation and benchmarking.
- **`train_qlora.py`**: Production-ready PyTorch / Hugging Face QLoRA fine-tuning script with 4-bit NF4 quantization and PEFT.
- **`evaluate.py`**: Automated evaluation scorecard script measuring JSON validity, schema compliance, and tool operation accuracy.
- **`Modelfile`**: Ready-to-use Ollama configuration file with system prompts, deterministic sampling parameters, and stop tokens.

---

## 3. Environment Setup

To run fine-tuning on a machine with a CUDA-enabled GPU (minimum 6GB VRAM recommended):

```bash
pip install torch transformers peft trl datasets bitsandbytes accelerate
```

*(For Apple Silicon or CPU testing, training without 4-bit quantization is supported by omitting `--device_map auto`).*

---

## 4. Fine-Tuning with QLoRA

Execute `train_qlora.py` with custom hyperparameters or defaults:

```bash
python training/train_qlora.py \
  --model_id google/gemma-3-1b-it \
  --train_file training/train.jsonl \
  --eval_file training/eval.jsonl \
  --output_dir training/output_adapter \
  --epochs 3 \
  --batch_size 2 \
  --grad_accum 4 \
  --lr 2e-4 \
  --lora_r 16 \
  --lora_alpha 32
```

### Key Hyperparameters:
- **Quantization**: 4-bit NormalFloat4 (NF4) with double quantization.
- **LoRA Targets**: `q_proj`, `k_proj`, `v_proj`, `o_proj`, `gate_proj`, `up_proj`, `down_proj`.
- **Rank & Alpha**: $r=16, \alpha=32$ for optimal parameter efficiency without losing instruction comprehension.
- **Temperature for Inference**: $0.2$ to minimize hallucination in JSON structure.

---

## 5. Benchmarking & Evaluation

Run `evaluate.py` to test accuracy against the held-out test suite:

### Offline Dataset Target Verification:
```bash
python training/evaluate.py --offline
```

### Live Evaluation against Local Ollama:
```bash
python training/evaluate.py --ollama_url http://localhost:11434 --model agentic-mesh-planner
```

The script produces a comprehensive scorecard:
- **Valid JSON Syntax %**: Verifies output parses as valid JSON.
- **Operation Compliance %**: Verifies operations are strictly in `{INSERT, UPDATE, DELETE}`.
- **Table Allowlist Compliance %**: Verifies tables are strictly in `{categories, items, suppliers, item_suppliers}`.
- **Full Schema Validity %**: Verifies required fields, types, and constraints.

---

## 6. Exporting to Ollama

After fine-tuning, merge the LoRA weights or export GGUF via `llama.cpp`:

1. Merge adapter into base weights:
```python
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

base = AutoModelForCausalLM.from_pretrained("google/gemma-3-1b-it")
model = PeftModel.from_pretrained(base, "training/output_adapter")
merged = model.merge_and_unload()
merged.save_pretrained("training/merged_model")
```

2. Convert to GGUF using `llama.cpp`:
```bash
python llama.cpp/convert_hf_to_gguf.py training/merged_model --outtype q4_k_m --outfile training/agentic-mesh.gguf
```

3. Create the Ollama model:
Update `training/Modelfile` to point to `FROM ./agentic-mesh.gguf`, then:
```bash
ollama create agentic-mesh-planner -f training/Modelfile
```

4. Configure Agentic Mesh node:
```bash
export OLLAMA_MODEL="agentic-mesh-planner"
npm start
```
