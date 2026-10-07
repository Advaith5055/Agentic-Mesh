# Ollama CPU Inference Guide for Agentic Mesh

## Overview
Agentic Mesh uses local Ollama models (`gemma4:e2b` or other configured SLMs) to provide autonomous transaction planning and developer assistance without sending data or queries to third-party cloud providers.

When running on systems without a dedicated GPU (e.g., Apple Silicon or discrete NVIDIA/AMD GPU), Ollama executes models entirely on the **CPU** (quantized GGUF format, `Q4_K_M`).

## Key Performance Characteristics on CPU

| Metric | Expected Range on CPU | Notes |
| :--- | :--- | :--- |
| **Time to First Token (TTFT)** | 10–25 seconds | Initial model context loading and prompt evaluation |
| **Generation Speed** | ~8–15 tokens/sec | CPU memory bandwidth bound |
| **Total Response Time (128 tokens)** | ~20–35 seconds | Fully completed response |
| **Memory Footprint** | ~3.5–4.5 GB RAM | For 2B–4B parameter models |

## Critical Configuration Parameters

In `.env`:
```ini
# Model selection
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=gemma4:e2b

# CPU safety and latency bounding
OLLAMA_NUM_PREDICT=128     # Bounds maximum generation tokens to prevent CPU lockup
OLLAMA_TIMEOUT_MS=90000    # 90-second hard server & client timeout
OLLAMA_KEEP_ALIVE=5m       # Keeps model in memory between chat turns (avoids reload overhead)
```

## Best Practices for CPU Inference

1. **Keep Only One Model Loaded**:
   - Run `ollama ps` to verify active models.
   - If multiple models are loaded, they will contend for CPU cores and RAM, drastically increasing latency or triggering timeouts.
   - To unload an idle model:
     ```bash
     ollama stop <model_name>
     ```

2. **Distinct Operational Modes**:
   - **General Code Assistance**: Queries like *"write Java code to check even or odd"* use a compact prompt without injecting the database schema or transaction log, keeping prompt processing under 1–2 seconds.
   - **Database Mutation Planning**: Write requests (e.g., *"add item Laptop for 999"*) inject schema DDL and generate strict JSON plans requiring explicit 1-click user approval.

3. **Client-Side Keepalives & Timeouts**:
   - The Agentic Mesh gateway sends SSE `: keepalive\n\n` comments every 12 seconds to keep connections alive while the CPU computes initial tokens.
   - The React dashboard enforces an `AbortController` timeout at 90 seconds. If the CPU is pegged by background tasks, an actionable error message is shown immediately instead of an infinite loading state.
