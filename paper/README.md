# Agentic Mesh — IEEE Research Paper LaTeX Source

This directory contains the complete research paper source code formatted according to the official **IEEE Conference/Transactions** specifications.

## 📄 File Structure

- **`main.tex`**: The primary LaTeX document containing the complete research paper (Abstract, Introduction, Related Work, System Architecture, Mathematical Model, Edge-AI Normalization, Empirical Evaluation, and Conclusion).
- **`references.bib`**: BibTeX bibliography containing authoritative citations for CRDTs, libp2p, GossipSub, Vector Clocks, SQLite, relational normalization (3NF/BCNF), and LLMs.

---

## 🛠️ How to Compile to PDF

### Option 1: Overleaf (Fastest & Recommended)
1. Create a new blank project on [Overleaf](https://www.overleaf.com/).
2. Upload `main.tex` and `references.bib`.
3. Set the compiler to **pdfLaTeX** and click **Recompile**.

### Option 2: Local TeX Live / MiKTeX (Command Line)
In your terminal, navigate to the `paper` directory and execute:

```bash
pdflatex main.tex
bibtex main
pdflatex main.tex
pdflatex main.tex
```

This will produce the finalized publication-ready `main.pdf`.

### Option 3: VS Code with LaTeX Workshop
1. Open the `paper` folder in VS Code.
2. Install the **LaTeX Workshop** extension.
3. Open `main.tex` and press `Ctrl+Alt+B` (or `Cmd+Option+B` on Mac) to build.

---

## 📑 Paper Overview
- **Title**: *Agentic Mesh: An Autonomous, Masterless Peer-to-Peer Database Network with Asynchronous Edge-LLM Schema Normalization and CRDT Synchronization*
- **Format**: IEEE 2-Column Standard (`IEEEtran.cls`)
- **Key Concepts Covered**:
  - Decentralized Zero-Cloud P2P Architecture with libp2p and GossipSub
  - Dual-Path Write Processing (Sub-millisecond fast path vs Asynchronous edge-LLM)
  - Vector Clock CRDT Replication & Last-Writer-Wins (LWW) resolution
  - Edge LLM (Gemma 4 E2B / Llama 3) for Conversational Relational Tool Abstraction
  - Formal 3NF / BCNF Schema Violation Interception & Anomaly Audits
  - Empirical Latency & Partition Convergence Benchmarks
