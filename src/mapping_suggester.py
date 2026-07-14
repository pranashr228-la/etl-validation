import re


DIM_PREFIXES = ["dim_", "d_"]
FACT_PREFIXES = ["fact_", "f_", "fct_"]
STRIP_PREFIXES = DIM_PREFIXES + FACT_PREFIXES + ["stg_", "staging_", "src_", "ods_", "raw_"]

# Column name patterns that indicate ETL renaming
ID_KEY_PATTERNS = [
    (r"(\w+)id$",           r"\1key"),
    (r"(\w+)key$",          r"\1id"),
    (r"(\w+)number$",       r"\1num"),
    (r"(\w+)num$",          r"\1number"),
    (r"(\w+)qty$",          r"\1quantity"),
    (r"(\w+)quantity$",     r"\1qty"),
    (r"(\w+)pct$",          r"\1percent"),
    (r"(\w+)percent$",      r"\1pct"),
    (r"(\w+)amt$",          r"\1amount"),
    (r"(\w+)amount$",       r"\1amt"),
    (r"(\w+)desc$",         r"\1description"),
    (r"(\w+)description$",  r"\1desc"),
    (r"(\w+)date$",         r"\1dt"),
    (r"(\w+)dt$",           r"\1date"),
    (r"^english(\w+)$",     r"\1"),
    (r"^(\w+)$",            r"english\1"),
    (r"^(\w+)$",            r"spanish\1"),
    (r"^(\w+)$",            r"french\1"),
    (r"^(\w+)$",            r"\1name"),
    (r"(\w+)name$",         r"\1"),
    (r"^(\w+)$",            r"\1key"),
    (r"(\w+)key$",          r"\1"),
    (r"^(\w+)$",            r"\1alternatekey"),
    (r"(\w+)alternatekey$", r"\1"),
]

# Language prefix patterns for DWH columns — English listed first (preferred)
LANGUAGE_PREFIXES = ["english", "spanish", "french", "german", "arabic", "hebrew",
                     "thai", "turkish", "japanese", "chinese"]


def _table_name_only(name: str) -> str:
    return name.split(".")[-1] if "." in name else name


def _normalize_table(name: str) -> str:
    n = _table_name_only(name).lower()
    for prefix in STRIP_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):]
            break
    return n


def _strip_language_prefix(name: str) -> str:
    lower = name.lower()
    for lang in LANGUAGE_PREFIXES:
        if lower.startswith(lang):
            return lower[len(lang):]
    return lower


def _col_similarity_score(src: str, tgt: str) -> float:
    s = src.lower()
    t = tgt.lower()

    # Exact match
    if s == t:
        return 1.0

    # Strip language prefixes from target (e.g. EnglishProductCategoryName -> productcategoryname)
    t_stripped = _strip_language_prefix(t)
    s_stripped = _strip_language_prefix(s)

    if s == t_stripped or s_stripped == t:
        return 0.95

    # Strip trailing 'name', 'key', 'id' from both sides
    s_core = re.sub(r"(name|key|id|code|number|num|desc|description|date|dt)$", "", s)
    t_core = re.sub(r"(name|key|id|code|number|num|desc|description|date|dt)$", "", t_stripped)

    if s_core and t_core and s_core == t_core:
        return 0.90

    # Source is contained in target (e.g. "name" in "englishproductcategoryname")
    if len(s) >= 3 and s in t:
        base_score = min(0.85, len(s) / len(t) + 0.5)
        # Boost score if target starts with "english" (preferred language variant)
        if t.startswith("english"):
            base_score = min(0.95, base_score + 0.15)
        return base_score

    if len(t_stripped) >= 3 and t_stripped in s:
        return min(0.85, len(t_stripped) / len(s) + 0.5)

    if len(s_core) >= 3 and s_core in t_core:
        return min(0.80, len(s_core) / max(len(t_core), 1) + 0.4)

    # Word split overlap (camelCase and underscore)
    def split_words(n):
        n = re.sub(r"([A-Z])", r"_\1", n).lower()
        return set(w for w in re.split(r"[_\s]", n) if len(w) > 1)

    s_words = split_words(s)
    t_words = split_words(t)
    t_stripped_words = split_words(t_stripped)

    overlap = s_words & t_words
    overlap_stripped = s_words & t_stripped_words

    if overlap or overlap_stripped:
        best_overlap = max(len(overlap), len(overlap_stripped))
        total = len(s_words | t_words)
        return best_overlap / total * 0.75

    return 0.0


def _table_similarity_score(a: str, b: str) -> float:
    a_norm = _normalize_table(a)
    b_norm = _normalize_table(b)

    if a_norm == b_norm:
        return 1.0

    if a_norm in b_norm or b_norm in a_norm:
        longer = max(len(a_norm), len(b_norm))
        shorter = min(len(a_norm), len(b_norm))
        return shorter / longer * 0.9

    a_words = set(re.split(r"[_\s]", a_norm))
    b_words = set(re.split(r"[_\s]", b_norm))
    if a_words & b_words:
        overlap = len(a_words & b_words)
        total = len(a_words | b_words)
        return overlap / total * 0.8

    return 0.0


def _confidence_label(score: float) -> str:
    if score >= 1.0:
        return "high"
    if score >= 0.7:
        return "medium"
    if score >= 0.4:
        return "low"
    return "none"


def match_tables(source_tables: list, target_tables: list) -> list:
    matches = []
    used_targets = set()

    # Pass 1 — exact name match
    for src in source_tables:
        for tgt in target_tables:
            if src.lower() == tgt.lower() and tgt not in used_targets:
                matches.append({
                    "source_table": src,
                    "target_table": tgt,
                    "confidence": "high",
                    "match_reason": "exact name match",
                    "status": "AUTO",
                })
                used_targets.add(tgt)
                break

    matched_sources = {m["source_table"] for m in matches}

    # Pass 2 — normalized name match
    for src in source_tables:
        if src in matched_sources:
            continue
        best_score = 0.0
        best_tgt = None
        for tgt in target_tables:
            if tgt in used_targets:
                continue
            score = _table_similarity_score(src, tgt)
            if score > best_score:
                best_score = score
                best_tgt = tgt

        confidence = _confidence_label(best_score)
        if best_tgt and confidence != "none":
            matches.append({
                "source_table": src,
                "target_table": best_tgt,
                "confidence": confidence,
                "match_reason": f"name similarity (score={round(best_score, 2)})",
                "status": "AUTO" if confidence == "high" else "NEEDS_REVIEW",
            })
            used_targets.add(best_tgt)
        else:
            matches.append({
                "source_table": src,
                "target_table": None,
                "confidence": "none",
                "match_reason": "no match found",
                "status": "NEEDS_REVIEW",
            })

    return matches


def match_columns(src_columns: list, tgt_columns: list) -> list:
    tgt_map = {col["column_name"].lower(): col for col in tgt_columns}
    mapped = []
    used_tgt = set()

    # Pass 1 — exact case-insensitive match
    for src_col in src_columns:
        src_lower = src_col["column_name"].lower()
        if src_lower in tgt_map and src_lower not in used_tgt:
            tgt_col = tgt_map[src_lower]
            mapped.append({
                "source_col": src_col["column_name"],
                "target_col": tgt_col["column_name"],
                "source_type": src_col["column_type"],
                "target_type": tgt_col["column_type"],
                "confidence": "high",
                "match_reason": "exact name match",
                "status": "AUTO",
            })
            used_tgt.add(src_lower)

    matched_src = {m["source_col"].lower() for m in mapped}

    # Pass 2 — pattern transforms + similarity scoring
    for src_col in src_columns:
        src_lower = src_col["column_name"].lower()
        if src_lower in matched_src:
            continue

        best_tgt = None
        best_score = 0.0
        best_reason = ""

        # Try ID/Key pattern transforms
        # For source columns ending in ID, prefer AlternateKey over Key in target
        # because AlternateKey = business key (matches source ID)
        #         Key           = surrogate key (auto-generated, does not match source ID)
        for pattern, replacement in ID_KEY_PATTERNS:
            transformed = re.sub(pattern, replacement, src_lower)
            if transformed != src_lower and transformed in tgt_map and transformed not in used_tgt:
                tgt_col = tgt_map[transformed]
                # Prefer AlternateKey match over plain Key match for ID columns
                score = 0.95 if "alternatekey" in transformed else 0.88
                if score > best_score:
                    best_score = score
                    best_tgt = tgt_col
                    best_reason = f"pattern transform ({src_lower} -> {transformed})"

        # Also explicitly check if source ends in 'id' and target has matching 'alternatekey'
        if src_lower.endswith("id"):
            base = src_lower[:-2]  # strip 'id'
            alternate_key = base + "alternatekey"
            if alternate_key in tgt_map and alternate_key not in used_tgt:
                score = 0.95
                if score > best_score:
                    best_score = score
                    best_tgt = tgt_map[alternate_key]
                    best_reason = f"ID -> AlternateKey business key match ({src_lower} -> {alternate_key})"

        # Try similarity scoring against all unmatched target columns
        for tgt_lower, tgt_col in tgt_map.items():
            if tgt_lower in used_tgt:
                continue
            score = _col_similarity_score(src_lower, tgt_lower)
            if score > best_score:
                best_score = score
                best_tgt = tgt_col
                best_reason = f"name similarity (score={round(score, 2)})"

        # Only accept match if score is meaningful
        if best_tgt and best_score >= 0.40:
            confidence = _confidence_label(best_score)
            mapped.append({
                "source_col": src_col["column_name"],
                "target_col": best_tgt["column_name"],
                "source_type": src_col["column_type"],
                "target_type": best_tgt["column_type"],
                "confidence": confidence,
                "match_reason": best_reason,
                "status": "AUTO" if confidence in ("high", "medium") else "NEEDS_REVIEW",
            })
            used_tgt.add(best_tgt["column_name"].lower())
        else:
            # No match found — column exists in source but not in target
            mapped.append({
                "source_col": src_col["column_name"],
                "target_col": None,
                "source_type": src_col["column_type"],
                "target_type": None,
                "confidence": "none",
                "match_reason": "no matching target column found",
                "status": "NEEDS_REVIEW",
            })

    return mapped
