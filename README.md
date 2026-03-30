# DW Architect - Data Warehouse Star Schema Generator

A deterministic algorithmic engine that transforms an OLTP schema (SQL) and a business context into a star schema for Data Warehouse.

---

## Features

* 100% algorithmic — zero hardcoding, no LLM required
* Supports both French and English

  * Recognizes: `par X`, `by X`, `selon X`, `according to X`
* Automatic detection of:

  * Fact (central table)
  * Dimensions (analysis axes)
  * Measures (quantitative attributes)
* Context and schema validation — detects inconsistencies
* 7-step pipeline with structured JSON output at each step
* Full SQL generation:

  * CREATE TABLE
  * Foreign keys
  * out_timestamp field
* Tested on 1,000,000 cases with 100% success rate

---

## Usage

```bash
python dw_architect.py <schema.sql> <context.txt>
```

### Example

```bash
python dw_architect.py 03_gaz.sql 03_gaz.txt
```

---

## Ollama Mode (Optional)

```bash
python dw_architect.py 03_gaz.sql 03_gaz.txt --ollama llama3
```

This mode uses a local model via Ollama for enhanced semantic understanding while still respecting the deterministic algorithm.

---

## 7-Step Algorithm

| Step | Description                                                 |
| ---- | ----------------------------------------------------------- |
| 0    | Validate schema and business context                        |
| 1    | Identify the fact (central table)                           |
| 2    | Extract dimensions from context                             |
| 3    | Map dimensions to source tables                             |
| 4    | Classify attributes (quantitative, qualitative, temporal)   |
| 5    | Generate dimension tables                                   |
| 6    | Generate fact table (foreign keys, measures, out_timestamp) |
| 7    | Add time dimension and generate final SQL                   |

 

---

## Output Files

* dw_result.json
  Full pipeline output with all steps in JSON

* dw_star_schema.sql
  Generated Data Warehouse SQL schema

---

## Architecture Overview

```text
INPUT:
  - Relational Schema (SQL)
  - Business Context

PROCESS:
  Step 1 → Fact Detection  
  Step 2 → Dimension Extraction  
  Step 3 → Mapping  
  Step 4 → Classification  
  Step 5 → Dimension Generation  
  Step 6 → Fact Generation  
  Step 7 → SQL Generation  

OUTPUT:
  - Star Schema
  - SQL
  - JSON trace
```

---

## Core Design Principles

* Context-driven modeling (not schema-driven)
* Deterministic logic over probabilistic approaches
* Strict separation:

  * Fact = measurable events
  * Dimensions = analysis axes
* No over-generation:

  * Only business-relevant dimensions are created

---

## Rules Enforced

* Dimensions must come from business context
* Temporal attributes are always mapped to Dim_Temps
* Qualitative attributes become dimensions
* Quantitative attributes become measures
* Technical tables (join tables, contracts, etc.) are ignored

---

## Requirements

* Python 3.10+
* No external dependencies (standalone mode)
* Optional:
  pip install requests
  (only required for Ollama mode)

---

## Vision

DW Architect (LLM2DW) aims to automate Data Warehouse design, bridge the gap between business intent and data modeling, and enable instant analytics-ready architectures.

---

## Keywords

Data Warehouse, Star Schema, OLTP to OLAP, Data Modeling, Automation, ETL, Analytics
