#!/usr/bin/env python3
"""
Agentic Mesh — Gemma QLoRA Fine-Tuning Script
Fine-tunes Gemma (e.g. google/gemma-3-1b-it or google/gemma-2b-it) using 4-bit QLoRA
to output safe, 3NF-compliant JSON operation plans for local SQLite mesh nodes.

Prerequisites:
  pip install torch transformers peft trl datasets bitsandbytes accelerate
"""

import argparse
import os
import json
import torch
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
from trl import SFTTrainer


def parse_args():
    parser = argparse.ArgumentParser(description="Fine-tune Gemma with QLoRA for Agentic Mesh tool calling")
    parser.add_argument("--model_id", type=str, default="google/gemma-3-1b-it", help="Hugging Face model ID or path")
    parser.add_argument("--train_file", type=str, default="training/train.jsonl", help="Path to training jsonl")
    parser.add_argument("--eval_file", type=str, default="training/eval.jsonl", help="Path to evaluation jsonl")
    parser.add_argument("--output_dir", type=str, default="training/output_adapter", help="Directory to save LoRA adapter")
    parser.add_argument("--max_seq_length", type=int, default=1024, help="Maximum sequence length")
    parser.add_argument("--batch_size", type=int, default=2, help="Per-device training batch size")
    parser.add_argument("--grad_accum", type=int, default=4, help="Gradient accumulation steps")
    parser.add_argument("--epochs", type=int, default=3, help="Number of training epochs")
    parser.add_argument("--lr", type=float, default=2e-4, help="Learning rate")
    parser.add_argument("--lora_r", type=int, default=16, help="LoRA rank r")
    parser.add_argument("--lora_alpha", type=int, default=32, help="LoRA alpha scaling parameter")
    parser.add_argument("--lora_dropout", type=float, default=0.05, help="LoRA dropout rate")
    return parser.parse_args()


def format_chat_prompt(batch, tokenizer):
    """
    Formats the conversation messages using the tokenizer chat template.
    """
    formatted_texts = []
    for messages in batch["messages"]:
        # If tokenizer has apply_chat_template, use it
        if hasattr(tokenizer, "apply_chat_template") and tokenizer.chat_template:
            text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
        else:
            # Fallback simple formatting
            text = ""
            for msg in messages:
                text += f"<|im_start|>{msg['role']}\n{msg['content']}<|im_end|>\n"
        formatted_texts.append(text)
    return {"text": formatted_texts}


def main():
    args = parse_args()
    print("================================================================")
    print("  AGENTIC MESH — GEMMA QLoRA FINE-TUNING PIPELINE               ")
    print("================================================================")
    print(f"Base Model:       {args.model_id}")
    print(f"Training Data:    {args.train_file}")
    print(f"Evaluation Data:  {args.eval_file}")
    print(f"Output Adapter:   {args.output_dir}")
    print(f"LoRA Rank:        {args.lora_r} (alpha: {args.lora_alpha})")
    print("================================================================\n")

    # 1. 4-bit Quantization Configuration (NF4)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16 if torch.cuda.is_available() and torch.cuda.is_bf16_supported() else torch.float16,
        bnb_4bit_use_double_quant=True
    )

    # 2. Load Tokenizer
    print("Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(args.model_id, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # 3. Load Base Model in 4-bit
    print(f"Loading base model '{args.model_id}' in 4-bit precision...")
    device_map = "auto" if torch.cuda.is_available() else None
    model = AutoModelForCausalLM.from_pretrained(
        args.model_id,
        quantization_config=bnb_config if torch.cuda.is_available() else None,
        device_map=device_map,
        trust_remote_code=True
    )

    if torch.cuda.is_available():
        model = prepare_model_for_kbit_training(model)

    # 4. LoRA Adapter Configuration
    lora_config = LoraConfig(
        r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        target_modules=[
            "q_proj",
            "k_proj",
            "v_proj",
            "o_proj",
            "gate_proj",
            "up_proj",
            "down_proj"
        ],
        bias="none",
        task_type="CAUSAL_LM"
    )

    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # 5. Load and Preprocess Datasets
    print("\nLoading datasets...")
    train_dataset = load_dataset("json", data_files=args.train_file, split="train")
    eval_dataset = load_dataset("json", data_files=args.eval_file, split="train") if os.path.exists(args.eval_file) else None

    print(f"Loaded {len(train_dataset)} training examples.")
    if eval_dataset:
        print(f"Loaded {len(eval_dataset)} evaluation examples.")

    formatted_train = train_dataset.map(lambda b: format_chat_prompt(b, tokenizer), batched=True)
    formatted_eval = eval_dataset.map(lambda b: format_chat_prompt(b, tokenizer), batched=True) if eval_dataset else None

    # 6. Training Arguments
    training_args = TrainingArguments(
        output_dir=args.output_dir,
        per_device_train_batch_size=args.batch_size,
        gradient_accumulation_steps=args.grad_accum,
        num_train_epochs=args.epochs,
        learning_rate=args.lr,
        lr_scheduler_type="cosine",
        warmup_ratio=0.1,
        fp16=torch.cuda.is_available() and not (torch.cuda.is_bf16_supported()),
        bf16=torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
        logging_steps=5,
        save_strategy="epoch",
        evaluation_strategy="epoch" if formatted_eval else "no",
        save_total_limit=2,
        report_to="none"
    )

    # 7. SFT Trainer
    trainer = SFTTrainer(
        model=model,
        train_dataset=formatted_train,
        eval_dataset=formatted_eval,
        peft_config=lora_config,
        dataset_text_field="text",
        max_seq_length=args.max_seq_length,
        tokenizer=tokenizer,
        args=training_args
    )

    # 8. Train & Save
    print("\nStarting fine-tuning...")
    trainer.train()

    print(f"\nSaving LoRA adapter to {args.output_dir}...")
    trainer.model.save_pretrained(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)
    print("Fine-tuning completed successfully! Adapter is ready for Ollama export.")


if __name__ == "__main__":
    main()
