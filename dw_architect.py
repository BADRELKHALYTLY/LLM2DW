"""
Data Warehouse Architect - Step-by-step star schema design.
100% algorithm-driven: zero hardcoded domain knowledge.

Two modes:
  1. Standalone (no LLM): deterministic SQL parsing + context analysis
  2. Ollama mode: uses a local LLM for each step

Usage:
  python dw_architect.py <schema.sql> <context.txt> [--ollama MODEL]
"""

import json
import re
import sys
import os
import unicodedata

# ─── Optional Ollama support ─────────────────────────────────────────
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

OLLAMA_URL = "http://localhost:11434/api/generate"


# ─── Text normalization ─────────────────────────────────────────────

def normalize(text: str) -> str:
    """Remove accents and lowercase for fuzzy matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def tokenize(text: str) -> set[str]:
    """Split text into normalized tokens (words)."""
    return set(re.findall(r"\w+", normalize(text)))


def similarity(text_a: str, text_b: str) -> float:
    """Token-based Jaccard similarity between two strings."""
    tokens_a = tokenize(text_a)
    tokens_b = tokenize(text_b)
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def fuzzy_match(needle: str, haystack: str) -> bool:
    """Check if needle is contained in haystack after normalization."""
    return normalize(needle) in normalize(haystack) or normalize(haystack) in normalize(needle)


def singularize(name: str) -> str:
    """Convert French/English plural table names to singular form."""
    n = name.strip()
    # French irregular: -eaux -> -eau (bateaux, morceaux)
    if n.endswith("eaux"):
        return n[:-1]
    # French irregular: -aux -> -al (journaux, animaux)
    if n.endswith("aux"):
        return n[:-3] + "al"
    # French: -oux -> -ou (bijoux, cailloux)
    if n.endswith("oux"):
        return n[:-1]
    # French: -eux stays -eux (jeux -> jeu)
    if n.endswith("eux"):
        return n[:-1]
    # English: -ies -> -y (categories -> category)
    if n.endswith("ies"):
        return n[:-3] + "y"
    # Protect -ses where stem ends in 's' (analyses -> analyse, devises -> devise)
    if n.endswith("ses"):
        return n[:-1]
    # English: -ches -> -ch, -xes -> -x, -shes -> -sh
    if n.endswith("ches") or n.endswith("xes") or n.endswith("shes"):
        return n[:-2]
    # Regular: -s (but not -ss)
    if n.endswith("s") and not n.endswith("ss"):
        return n[:-1]
    return n


def strip_common_prefixes(name: str) -> str:
    """Remove common SQL column prefixes like id_, methode_, type_, code_, nom_."""
    return re.sub(r"^(id_|methode_|type_|code_|nom_|num_|ref_)", "", name.lower())


# ─── SQL Parser ──────────────────────────────────────────────────────

NUMERIC_TYPES = {"INT", "INTEGER", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC", "REAL", "BIGINT", "SMALLINT", "TINYINT"}
TEXT_TYPES = {"VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "ENUM", "SET"}
DATE_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR"}


def parse_sql_schema(sql: str) -> list[dict]:
    """Parse CREATE TABLE statements into structured dicts."""
    tables = []
    pattern = re.compile(
        r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(sql):
        table_name = match.group(1).strip()
        body = match.group(2).strip()

        columns = []
        primary_key = None
        foreign_keys = []

        # Smart split: split on commas NOT inside parentheses
        parts = []
        depth = 0
        current = ""
        for ch in body:
            if ch == "(":
                depth += 1
                current += ch
            elif ch == ")":
                depth -= 1
                current += ch
            elif ch == "," and depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += ch
        if current.strip():
            parts.append(current.strip())

        for part in parts:
            part = part.strip()
            if not part:
                continue

            fk_match = re.match(
                r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
                part, re.IGNORECASE,
            )
            if fk_match:
                foreign_keys.append({
                    "column": fk_match.group(1),
                    "ref_table": fk_match.group(2),
                    "ref_column": fk_match.group(3),
                })
                continue

            col_match = re.match(r"(\w+)\s+(.+)", part)
            if col_match:
                col_name = col_match.group(1)
                col_rest = col_match.group(2).strip()

                is_pk = "PRIMARY KEY" in col_rest.upper()
                col_type = re.split(r"\s+PRIMARY\s+KEY", col_rest, flags=re.IGNORECASE)[0].strip()
                col_type = re.sub(r"\s*\(\s*", "(", col_type)
                col_type = re.sub(r"\s*,\s*", ",", col_type)
                col_type = re.sub(r"\s*\)", ")", col_type)

                if is_pk:
                    primary_key = col_name

                base_type = col_type.split("(")[0].upper().strip()
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "base_type": base_type,
                    "is_pk": is_pk,
                })

        tables.append({
            "name": table_name,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys,
        })

    return tables


# ─── Generic classifiers (no hardcoded names) ───────────────────────

def is_id_column(col_name: str) -> bool:
    """Detect ID/key columns by naming pattern."""
    name = col_name.lower()
    return name.startswith("id_") or name.endswith("_id") or name == "id"


def classify_column(col: dict) -> str:
    """Classify a column as 'key', 'quantitative', 'qualitative', or 'temporal'."""
    if col["is_pk"] or is_id_column(col["name"]):
        return "key"
    base = col["base_type"]
    if base in DATE_TYPES:
        return "temporal"
    if base in TEXT_TYPES:
        return "qualitative"
    if base in NUMERIC_TYPES:
        return "quantitative"
    return "qualitative"


# ─── Context axis extraction (no hardcoded keywords) ────────────────

def extract_axes_from_context(context: str) -> list[str]:
    """Extract analysis axes using structural patterns (par X, by X, selon X).
    Works line-by-line first, then falls back to inline parsing."""
    axes = []
    seen = set()

    def add_axis(raw: str):
        # Clean: remove parenthetical content, strip
        cleaned = re.sub(r"\([^)]*\)", "", raw).strip().rstrip(",. ")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if cleaned and len(cleaned) > 1 and normalize(cleaned) not in seen:
            axes.append(cleaned)
            seen.add(normalize(cleaned))

    # Pass 1: line-by-line — each line starting with "par/by/selon" is an axis
    for line in context.splitlines():
        line = line.strip()
        m = re.match(r"(?:par|selon|by|according\s+to)\s+(.+)", line, re.IGNORECASE)
        if m:
            add_axis(m.group(1))

    # Pass 2: inline patterns in the full text (for "par X, par Y, et par Z" style)
    text = re.sub(r"\([^)]*\)", "", context)
    text = re.sub(r"\s+", " ", text).strip()

    # Delimiters: FR (par, et, ainsi, notamment, selon, avec) + EN (by, and, as well as, according)
    delim = r"(?=\s*,|\s*\.|\s+par\s|\s+et\s|\s+ainsi|\s+notamment|\s+selon|\s+avec|\s+by\s|\s+and\s|\s+as\s+well|\s+according|\s*$)"
    patterns = [
        rf"par\s+([\w\séèêëàâäùûüôîïç'-]+?){delim}",
        rf"selon\s+([\w\séèêëàâäùûüôîïç'-]+?){delim}",
        rf"by\s+([\w\s'-]+?){delim}",
        rf"according\s+to\s+([\w\s'-]+?){delim}",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            add_axis(m.group(1))

    # Pass 3: "ainsi que X" / "as well as X"
    for pat in [rf"ainsi\s+que\s+([\w\séèêëàâäùûüôîïç'-]+?){delim}",
                rf"as\s+well\s+as\s+([\w\s'-]+?){delim}"]:
        for m in re.finditer(pat, text, re.IGNORECASE):
            add_axis(m.group(1))

    return axes


# ─── Generic axis-to-schema matching (no hardcoded mapping) ─────────

def find_best_column_match(axis: str, tables: list[dict]) -> tuple[dict | None, dict | None]:
    """Find the column+table that best matches an axis string. Pure fuzzy matching."""
    axis_norm = normalize(axis)
    axis_tokens = tokenize(axis)

    best_col = None
    best_table = None
    best_score = 0.0

    # Helper: check if needle appears as a whole word in haystack
    def word_match(needle: str, haystack: str) -> bool:
        return bool(re.search(r'\b' + re.escape(needle) + r'\b', haystack))

    for t in tables:
        for c in t["columns"]:
            if c["is_pk"] or is_id_column(c["name"]):
                continue
            col_norm = normalize(c["name"])
            col_clean = normalize(strip_common_prefixes(c["name"]))

            # Score: how well does this column name match the axis?
            score = 0.0

            # Pre-compute meaningful tokens for this column
            col_tokens = tokenize(c["name"].replace("_", " "))
            meaningful_col = {t for t in col_tokens if len(t) >= 3}
            meaningful_axis = {t for t in axis_tokens if len(t) >= 3}

            # Whole-word substring match (strongest signal)
            if len(col_norm) >= 3 and word_match(col_norm, axis_norm):
                score = 0.9
            elif len(col_norm) >= 3 and axis_norm in col_norm:
                score = 0.9
            elif len(col_clean) >= 3 and word_match(col_clean, axis_norm):
                score = 0.85
            elif len(col_clean) >= 3 and axis_norm in col_clean:
                score = 0.85

            # Token overlap
            overlap = meaningful_col & meaningful_axis
            if overlap:
                token_score = len(overlap) / max(len(meaningful_axis), len(meaningful_col), 1)
                score = max(score, token_score)

            # Penalize single-token column matching a long multi-word axis
            # e.g. "niveau" matching "niveau de bruit" — col has 1 meaningful token,
            # axis has 2+ meaningful tokens where only 1 overlaps → weak match
            if score > 0 and len(meaningful_col) == 1 and len(meaningful_axis) >= 2:
                if len(overlap) <= 1 and len(meaningful_axis - meaningful_col) >= 1:
                    score *= 0.4

            if score > best_score:
                best_score = score
                best_col = c
                best_table = t

    # Also try matching table names (for axes like "patient", "produit")
    for t in tables:
        t_norm = normalize(t["name"])
        t_singular = normalize(singularize(t["name"]))

        score = 0.0
        if len(t_singular) >= 3 and (word_match(t_norm, axis_norm) or word_match(t_singular, axis_norm)):
            score = 0.7
        elif len(t_singular) >= 3 and (axis_norm in t_norm or axis_norm in t_singular):
            score = 0.7

        if score > best_score:
            best_score = score
            best_col = None
            best_table = t

    if best_score < 0.4:
        return None, None

    return best_col, best_table


def detect_time_axis(axis: str) -> bool:
    """Detect if an axis refers to time/period — by checking against date-type schema patterns."""
    time_indicators = tokenize("periode temporel temporelle date temps time period mois annee year month semaine week jour day")
    axis_tokens = tokenize(axis)
    return bool(axis_tokens & time_indicators)


def map_axis_to_schema(axis: str, tables: list[dict], fact_table: dict) -> dict | None:
    """Map a context axis to a schema element. Purely algorithmic, no hardcoded names."""

    # Check if this axis is about TIME
    if detect_time_axis(axis):
        # Find a DATE column in fact table or related tables
        for t in [fact_table] + tables:
            for c in t["columns"]:
                if c["base_type"] in DATE_TYPES and not c["is_pk"]:
                    return {
                        "name": "Dim_Temps",
                        "source_table": t["name"],
                        "attribute": c["name"],
                        "type": "derived_dimension",
                    }
        return {
            "name": "Dim_Temps",
            "source_table": fact_table["name"],
            "attribute": None,
            "type": "derived_dimension",
        }

    # Find best matching column/table
    matched_col, matched_table = find_best_column_match(axis, tables)

    if not matched_table:
        return None

    if matched_col:
        col_class = classify_column(matched_col)

        # If it's in the FACT table
        if matched_table["name"] == fact_table["name"]:
            if col_class == "quantitative":
                return None  # Measure, not a dimension
            elif col_class == "temporal":
                return {
                    "name": "Dim_Temps",
                    "source_table": matched_table["name"],
                    "attribute": matched_col["name"],
                    "type": "derived_dimension",
                }
            else:
                dim_label = strip_common_prefixes(matched_col["name"]).capitalize()
                return {
                    "name": f"Dim_{dim_label}",
                    "source_table": matched_table["name"],
                    "attribute": matched_col["name"],
                    "type": "derived_dimension",
                }

        # It's in a related table -> existing_dimension, named after the TABLE
        dim_label = singularize(matched_table["name"]).capitalize()
        return {
            "name": f"Dim_{dim_label}",
            "source_table": matched_table["name"],
            "attribute": matched_col["name"],
            "type": "existing_dimension",
        }
    else:
        # Matched table but no specific column -> whole-table dimension
        dim_label = singularize(matched_table["name"]).capitalize()
        return {
            "name": f"Dim_{dim_label}",
            "source_table": matched_table["name"],
            "attribute": None,
            "type": "existing_dimension",
        }


# ─── Algorithm Steps ─────────────────────────────────────────────────

SEP = "=" * 70


def print_step(num: int, title: str, data: dict):
    print(f"\n{SEP}")
    print(f"  ETAPE {num} : {title}")
    print(SEP)
    print(json.dumps(data, indent=2, ensure_ascii=False))


def run_algorithm(schema_sql: str, context: str):
    """Execute the 7-step DW design algorithm."""

    tables = parse_sql_schema(schema_sql)
    table_map = {t["name"]: t for t in tables}

    # ── STEP 0: Validate schema & context ────────────────────────────
    warnings = []

    if not tables:
        error = {"step": 0, "status": "ERREUR", "message": "Aucune table trouvee dans le schema SQL."}
        print_step(0, "VALIDATION", error)
        return {"error": error}

    tables_with_fk = [t for t in tables if t["foreign_keys"]]
    if not tables_with_fk:
        error = {"step": 0, "status": "ERREUR", "message": "Aucune table avec des cles etrangeres. Impossible d'identifier un fait."}
        print_step(0, "VALIDATION", error)
        return {"error": error}

    raw_axes_check = extract_axes_from_context(context)
    if not raw_axes_check:
        warnings.append("Aucun axe d'analyse extrait du contexte (pas de 'par X', 'by X', 'selon X').")

    # Check how many axes actually match the schema
    matched_axes = 0
    unmatched_axes = []
    for axis in raw_axes_check:
        col, tbl = find_best_column_match(axis, tables)
        if tbl or detect_time_axis(axis):
            matched_axes += 1
        else:
            unmatched_axes.append(axis)

    if raw_axes_check and matched_axes == 0:
        error = {
            "step": 0, "status": "ERREUR",
            "message": "Le contexte ne correspond pas au schema SQL. Aucun axe d'analyse ne correspond aux tables/colonnes du schema.",
            "axes_extraits": raw_axes_check,
            "axes_non_trouves": unmatched_axes,
            "tables_disponibles": [t["name"] for t in tables],
        }
        print_step(0, "VALIDATION", error)
        return {"error": error}

    if unmatched_axes:
        warnings.append(f"Axes du contexte non trouves dans le schema: {unmatched_axes}")

    if warnings:
        print_step(0, "VALIDATION - AVERTISSEMENTS", {"warnings": warnings})

    # ── STEP 1: Identify the FACT ────────────────────────────────────
    # Score: structural (FK count, not referenced by others) + semantic (context, measures)
    context_norm = normalize(context)

    def fact_score(t):
        fk_count = len(t["foreign_keys"])
        if fk_count == 0:
            return -1
        # Context bonus: does the context mention this table as analysis object?
        t_norm = normalize(t["name"])
        t_singular = normalize(singularize(t["name"]))
        context_bonus = 1 if (t_norm in context_norm or t_singular in context_norm) else 0
        # Measure bonus: tables with quantitative columns are more likely facts
        has_measures = any(classify_column(c) == "quantitative" for c in t["columns"])
        measure_bonus = 2 if has_measures else 0
        # Structural: tables referenced by others are less likely to be facts (they are dims/bridges)
        referenced_by = sum(
            1 for other in tables
            for fk in other["foreign_keys"]
            if fk["ref_table"] == t["name"]
        )
        structural = fk_count - referenced_by
        return structural + context_bonus + measure_bonus

    fact_table = max(tables, key=fact_score)
    step1 = {
        "step": 1,
        "description": "Identification du FAIT",
        "fact": fact_table["name"],
        "justification": (
            f"La table '{fact_table['name']}' possede {len(fact_table['foreign_keys'])} "
            f"cles etrangeres — c'est la table centrale d'analyse."
        ),
    }
    print_step(1, "Identification du FAIT", step1)

    # ── STEP 2: Extract DIMENSIONS ───────────────────────────────────
    # 2.1 — Extract axes from context
    raw_axes = extract_axes_from_context(context)

    # 2.2 — Map each axis to schema
    dimensions_detail = []
    seen_dim_names = set()
    dim_to_table = {}

    for axis in raw_axes:
        result = map_axis_to_schema(axis, tables, fact_table)
        if result and result["name"] not in seen_dim_names:
            dimensions_detail.append(result)
            seen_dim_names.add(result["name"])
            dim_key = result["name"].replace("Dim_", "").lower()
            dim_to_table[dim_key] = result["source_table"]

    # 2.3 — Ensure Dim_Temps exists (find any DATE column)
    if "Dim_Temps" not in seen_dim_names:
        for t in [fact_table] + tables:
            for c in t["columns"]:
                if c["base_type"] in DATE_TYPES and not c["is_pk"]:
                    dimensions_detail.append({
                        "name": "Dim_Temps",
                        "source_table": t["name"],
                        "attribute": c["name"],
                        "type": "derived_dimension",
                    })
                    dim_to_table["temps"] = t["name"]
                    break
            if "Dim_Temps" in {d["name"] for d in dimensions_detail}:
                break

    # Build dim keys list
    context_dims = []
    for d in dimensions_detail:
        dim_key = d["name"].replace("Dim_", "").lower()
        context_dims.append(dim_key)

    step2 = {
        "step": 2,
        "description": "Extraction des dimensions",
        "fact": fact_table["name"],
        "raw_axes_extracted": raw_axes,
        "dimensions": dimensions_detail,
    }
    print_step(2, "Extraction des DIMENSIONS", step2)

    # ── STEP 3: Map dimensions to source tables ─────────────────────
    mapping = {}
    for d in dimensions_detail:
        dim_key = d["name"].replace("Dim_", "").lower()
        mapping[dim_key] = d["source_table"]

    step3 = {
        "step": 3,
        "description": "Mapping dimensions -> tables sources",
        "mapping": mapping,
    }
    print_step(3, "Mapping Dimensions -> Tables sources", step3)

    # ── STEP 4: Classify attributes ──────────────────────────────────
    quantitative = []
    qualitative = {}
    keys = []

    # Reverse map: OLTP table -> dimension key
    table_to_dim = {}
    for dim_key, tbl in mapping.items():
        if tbl in table_map and tbl not in table_to_dim:
            table_to_dim[tbl] = dim_key

    for t in tables:
        fk_cols = {fk["column"] for fk in t["foreign_keys"]}
        for col in t["columns"]:
            full_name = f"{t['name']}.{col['name']}"

            if col["is_pk"] or col["name"] in fk_cols:
                keys.append(full_name)
                continue

            cls = classify_column(col)

            if cls == "quantitative":
                quantitative.append(full_name)
            elif cls == "temporal":
                # ALL date/time columns go to Dim_Temps
                qualitative.setdefault("temps", []).append(col["name"])
            elif cls == "qualitative":
                # Assign to the matching dimension
                dim = table_to_dim.get(t["name"])
                if dim:
                    qualitative.setdefault(dim, []).append(col["name"])
                else:
                    # Try to match to a dimension by column name similarity
                    assigned = False
                    for d_key in context_dims:
                        if fuzzy_match(d_key, col["name"]) or fuzzy_match(d_key, strip_common_prefixes(col["name"])):
                            qualitative.setdefault(d_key, []).append(col["name"])
                            assigned = True
                            break
                    if not assigned:
                        qualitative.setdefault(t["name"], []).append(col["name"])

    step4 = {
        "step": 4,
        "description": "Classification des attributs",
        "quantitative_measures": quantitative,
        "qualitative_attributes": qualitative,
        "keys": keys,
    }
    print_step(4, "Classification des attributs", step4)

    # ── STEP 5: Generate DIMENSION tables ────────────────────────────
    dim_tables = []

    for d in dimensions_detail:
        dim_key = d["name"].replace("Dim_", "").lower()
        if dim_key == "temps":
            continue  # Step 7

        dim_table_name = d["name"]
        sk_name = f"SK_{dim_key}"
        columns = [{"name": sk_name, "type": "INT PRIMARY KEY AUTO_INCREMENT", "description": "Cle surrogate"}]

        if dim_key in qualitative and qualitative[dim_key]:
            for attr in qualitative[dim_key]:
                # Find original type
                orig_type = "VARCHAR(255)"
                src_table = mapping.get(dim_key, "")
                if src_table in table_map:
                    for c in table_map[src_table]["columns"]:
                        if c["name"] == attr:
                            orig_type = c["type"]
                            break
                else:
                    for t in tables:
                        for c in t["columns"]:
                            if c["name"] == attr:
                                orig_type = c["type"]
                                break
                columns.append({"name": attr, "type": orig_type, "description": f"Attribut {dim_key}"})
        else:
            # No qualitative attrs assigned yet — pull all qualitative columns from source
            src_table = mapping.get(dim_key, "")
            if src_table in table_map:
                for c in table_map[src_table]["columns"]:
                    cls = classify_column(c)
                    if cls == "qualitative" and not c["is_pk"] and not is_id_column(c["name"]):
                        columns.append({"name": c["name"], "type": c["type"], "description": f"Attribut {dim_key}"})

        dim_tables.append({"table_name": dim_table_name, "columns": columns})

    step5 = {
        "step": 5,
        "description": "Tables DIMENSION",
        "dimension_tables": dim_tables,
    }
    print_step(5, "Generation des tables DIMENSION", step5)

    # ── STEP 6: Generate FACT table ──────────────────────────────────
    fact_name = f"Fait_{fact_table['name'].capitalize()}"
    fact_columns = [
        {"name": f"SK_{fact_table['name']}", "type": "INT PRIMARY KEY AUTO_INCREMENT",
         "role": "PK", "description": "Cle surrogate du fait"},
    ]

    for d in dimensions_detail:
        dim_key = d["name"].replace("Dim_", "").lower()
        sk = f"SK_{dim_key}"
        fact_columns.append({"name": sk, "type": "INT", "role": "FK", "description": f"Cle vers {d['name']}"})

    # Measures from fact + related tables
    related_tables = {fact_table["name"]}
    for fk in fact_table["foreign_keys"]:
        related_tables.add(fk["ref_table"])
    for t in tables:
        for fk in t["foreign_keys"]:
            if fk["ref_table"] == fact_table["name"]:
                related_tables.add(t["name"])

    for measure in quantitative:
        tbl, col = measure.split(".")
        if tbl in related_tables:
            orig_type = "INT"
            for t in tables:
                for c in t["columns"]:
                    if c["name"] == col and t["name"] == tbl:
                        orig_type = c["type"]
                        break
            fact_columns.append({"name": col, "type": orig_type, "role": "MEASURE", "description": f"Mesure: {col}"})

    # Technical field: ETL load timestamp
    fact_columns.append({"name": "out_timestamp", "type": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "role": "TECHNICAL", "description": "Horodatage de chargement ETL"})

    step6 = {
        "step": 6,
        "description": "Table FAIT",
        "fact_table": {"table_name": fact_name, "columns": fact_columns},
    }
    print_step(6, "Generation de la table FAIT", step6)

    # ── STEP 7: Time dimension + Final SQL ───────────────────────────
    time_columns = [
        {"name": "SK_temps",      "type": "INT PRIMARY KEY AUTO_INCREMENT", "description": "Cle surrogate"},
        {"name": "date_complete", "type": "DATE",        "description": "Date complete"},
        {"name": "jour",          "type": "INT",         "description": "Jour du mois (1-31)"},
        {"name": "mois",          "type": "INT",         "description": "Mois (1-12)"},
        {"name": "nom_mois",      "type": "VARCHAR(20)", "description": "Nom du mois"},
        {"name": "trimestre",     "type": "INT",         "description": "Trimestre (1-4)"},
        {"name": "annee",         "type": "INT",         "description": "Annee"},
        {"name": "jour_semaine",  "type": "VARCHAR(20)", "description": "Jour de la semaine"},
        {"name": "est_weekend",   "type": "BOOLEAN",     "description": "Indicateur weekend"},
    ]
    time_table = {"table_name": "Dim_Temps", "columns": time_columns}

    # Build SQL
    all_dim_tables = [time_table] + dim_tables
    sql_lines = [
        "-- =============================================",
        "-- DATA WAREHOUSE - SCHEMA EN ETOILE (STAR SCHEMA)",
        "-- =============================================",
        "",
    ]

    for dt in all_dim_tables:
        sql_lines.append(f"-- {dt['table_name']}")
        sql_lines.append(f"CREATE TABLE {dt['table_name']} (")
        for i, col in enumerate(dt["columns"]):
            comma = "," if i < len(dt["columns"]) - 1 else ""
            sql_lines.append(f"  {col['name']} {col['type']}{comma}")
        sql_lines.append(");\n")

    sql_lines.append(f"-- Table de faits : {fact_name}")
    sql_lines.append(f"CREATE TABLE {fact_name} (")

    fk_constraints = []
    for col in fact_columns:
        if col.get("role") == "FK":
            for d in dimensions_detail:
                dim_key = d["name"].replace("Dim_", "").lower()
                if col["name"] == f"SK_{dim_key}":
                    fk_constraints.append(
                        f"  FOREIGN KEY ({col['name']}) REFERENCES {d['name']}({col['name']})"
                    )
                    break

    total_lines = len(fact_columns) + len(fk_constraints)
    line_idx = 0
    for col in fact_columns:
        line_idx += 1
        comma = "," if line_idx < total_lines else ""
        sql_lines.append(f"  {col['name']} {col['type']}{comma}")

    for fk in fk_constraints:
        line_idx += 1
        comma = "," if line_idx < total_lines else ""
        sql_lines.append(f"{fk}{comma}")

    sql_lines.append(");\n")

    complete_sql = "\n".join(sql_lines)

    step7 = {
        "step": 7,
        "description": "Dimension Temps + SQL final",
        "time_dimension": time_table,
    }
    print_step(7, "Dimension TEMPS + SQL final", step7)

    print(f"\n{'#' * 70}")
    print("  SQL FINAL - SCHEMA EN ETOILE (DATA WAREHOUSE)")
    print(f"{'#' * 70}\n")
    print(complete_sql)

    # Save
    final_output = {
        "steps": [step1, step2, step3, step4, step5, step6, step7],
        "fact_table": step6["fact_table"],
        "dimension_tables": all_dim_tables,
        "complete_sql": complete_sql,
    }

    output_dir = os.path.dirname(os.path.abspath(sys.argv[0])) if len(sys.argv) > 0 else "."
    json_path = os.path.join(output_dir, "dw_result.json")
    sql_path = os.path.join(output_dir, "dw_star_schema.sql")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(complete_sql)

    print(f"\n  Fichiers generes :")
    print(f"    - {json_path}")
    print(f"    - {sql_path}")

    return final_output


# ─── Ollama Mode ─────────────────────────────────────────────────────

SYSTEM_PROMPT = "You are a Data Warehouse Architect. Respond with valid JSON only."

STEP_PROMPTS = {
    1: "STEP 1: Identify the FACT (central object of analysis) from this OLTP schema:\n{schema}\n\nContext:\n{context}\n\nRespond JSON: {{\"step\":1,\"fact\":\"...\",\"justification\":\"...\"}}",
    2: """STEP 2: Extract DIMENSIONS using the following algorithm.

You MUST strictly follow this reasoning process:

1. Identify the object of analysis (FACT) from STEP 1.

2. From the CONTEXT, extract all analysis axes:
   - phrases like "par X", "by X", "according to X", "selon X"

3. For EACH extracted axis:
   a. Find where it exists in the relational schema:
      - if it belongs to a related table → candidate DIMENSION (type: "existing_dimension")
      - if it belongs to the FACT table → go to step b

   b. If attribute is in FACT table:
      - determine if it is qualitative or quantitative
      - IF qualitative → create a new DIMENSION (type: "derived_dimension")
      - IF quantitative → keep as FACT measure (do NOT include as dimension)

4. Only keep VALID dimensions:
   - must be descriptive (NOT numeric measures)
   - must support analysis (grouping / filtering)

5. Normalize names: Dim_<Name>

Schema:
{schema}

Context:
{context}

Previous:
{previous}

RETURN STRICT JSON FORMAT:
{{
  "step": 2,
  "fact": "...",
  "dimensions": [
    {{
      "name": "Dim_...",
      "source_table": "...",
      "attribute": "...",
      "type": "existing_dimension | derived_dimension"
    }}
  ]
}}""",
    3: "STEP 3: Map each dimension to source OLTP table.\nSchema:\n{schema}\nPrevious:\n{previous}\n\nRespond JSON: {{\"step\":3,\"mapping\":{{...}}}}",
    4: "STEP 4: Classify attributes as quantitative (FACT) or qualitative (DIMENSION).\nSchema:\n{schema}\nPrevious:\n{previous}\n\nRespond JSON: {{\"step\":4,\"quantitative\":[...],\"qualitative\":{{...}},\"keys\":[...]}}",
    5: "STEP 5: Generate DIMENSION tables (surrogate keys + qualitative attrs).\nPrevious:\n{previous}\n\nRespond JSON: {{\"step\":5,\"dimension_tables\":[{{\"table_name\":\"Dim_X\",\"columns\":[...]}}]}}",
    6: "STEP 6: Generate FACT table (surrogate key + FKs + measures).\nPrevious:\n{previous}\n\nRespond JSON: {{\"step\":6,\"fact_table\":{{\"table_name\":\"Fait_X\",\"columns\":[...]}}}}",
    7: "STEP 7: Add TIME dimension + generate FINAL complete SQL CREATE TABLE statements.\nPrevious:\n{previous}\n\nRespond JSON: {{\"step\":7,\"time_dimension\":{{...}},\"complete_sql\":\"...\"}}",
}


def call_ollama(prompt, model):
    resp = requests.post(OLLAMA_URL, json={
        "model": model, "prompt": prompt, "system": SYSTEM_PROMPT,
        "stream": False, "options": {"temperature": 0.1, "num_predict": 4096},
    }, timeout=300)
    resp.raise_for_status()
    return resp.json()["response"]


def extract_json_from_text(text):
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    start = None
    return {"raw": text[:500]}


def run_ollama_pipeline(schema, context, model):
    results = []
    for step in range(1, 8):
        prev = json.dumps(results, indent=2, ensure_ascii=False)
        prompt = STEP_PROMPTS[step].format(schema=schema, context=context, previous=prev)
        print(f"\n  ETAPE {step} - Interrogation de {model}...")
        raw = call_ollama(prompt, model)
        result = extract_json_from_text(raw)
        results.append(result)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    return results


# ─── CLI Entry Point ─────────────────────────────────────────────────

if __name__ == "__main__":
    use_ollama = "--ollama" in sys.argv
    model = None
    file_args = []

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--ollama":
            use_ollama = True
            if i + 1 < len(args) and not args[i + 1].startswith("-"):
                model = args[i + 1]
                i += 1
            else:
                model = "llama3"
        else:
            file_args.append(args[i])
        i += 1

    if len(file_args) >= 2:
        with open(file_args[0], "r", encoding="utf-8") as f:
            schema = f.read()
        with open(file_args[1], "r", encoding="utf-8") as f:
            context = f.read()

        print(f"\n{SEP}")
        print("  DATA WAREHOUSE ARCHITECT")
        print(f"  Mode: {'Ollama (' + model + ')' if use_ollama else 'Analyse directe'}")
        print(SEP)

        if use_ollama:
            if not HAS_REQUESTS:
                print("[ERREUR] pip install requests")
                sys.exit(1)
            run_ollama_pipeline(schema, context, model)
        else:
            run_algorithm(schema, context)
    else:
        print("\nUsage:")
        print("  python dw_architect.py <schema.sql> <context.txt>")
        print("  python dw_architect.py <schema.sql> <context.txt> --ollama [model]")
        print("\nExemple:")
        print('  python dw_architect.py "../db_source.sql" "../context.txt"')
        print('  python dw_architect.py "../db_source.sql" "../context.txt" --ollama llama3')
