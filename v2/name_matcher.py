"""
name_matcher.py

This module provides functions for parsing, comparing, and matching names, extracted from
evaluation_processor.py. It supports nickname recognition and similarity scoring for
robust name matching across financial regulatory data sources.

Functions:
  - parse_name: Convert a name input into a standardized dictionary.
  - get_name_variants: Generate possible name variants including nicknames.
  - are_nicknames: Check if two names are nickname variants of each other.
  - match_name_part: Compare a single name part (first, middle, last) with a similarity score.
  - evaluate_name: Evaluate name match quality against a primary name and alternatives.
"""

import re
import logging
from typing import Dict, Any, Optional, Set, Tuple, List  # Added List import
import jellyfish

logger = logging.getLogger(__name__)

# Nickname mappings
nickname_dict = {
    "john": {"jon", "johnny", "jack"},
    "robert": {"bob", "rob", "bobby", "bert"},
    "elizabeth": {"liz", "beth", "lizzy", "eliza"},
}
reverse_nickname_dict = {}
for formal_name, nicknames in nickname_dict.items():
    for nickname in nicknames:
        reverse_nickname_dict.setdefault(nickname, set()).add(formal_name)

def parse_name(name_input: Any) -> Dict[str, Optional[str]]:
    """
    Convert a name input (string or dict) into a standardized dict with keys:
    'first', 'middle', 'last'.
    """
    if isinstance(name_input, dict):
        return {
            "first": name_input.get("first"),
            "middle": name_input.get("middle"),
            "last": name_input.get("last")
        }
    if isinstance(name_input, str):
        parts = name_input.strip().split()
        if len(parts) == 0:
            return {"first": None, "middle": None, "last": None}
        elif len(parts) == 1:
            return {"first": parts[0], "middle": None, "last": None}
        elif len(parts) == 2:
            return {"first": parts[0], "middle": None, "last": parts[1]}
        else:
            return {"first": parts[0], "middle": " ".join(parts[1:-1]), "last": parts[-1]}
    return {"first": None, "middle": None, "last": None}

def get_name_variants(name: str) -> Set[str]:
    """
    Return a set of possible name variants, including nicknames.
    """
    variants = {name.lower()}
    if name.lower() in nickname_dict:
        variants.update({n.lower() for n in nickname_dict[name.lower()]})
    if name.lower() in reverse_nickname_dict:
        variants.update({n.lower() for n in reverse_nickname_dict[name.lower()]})
    return variants

def are_nicknames(name1: str, name2: str) -> bool:
    """
    Check if two names are nicknames of each other.
    """
    variants1 = get_name_variants(name1)
    variants2 = get_name_variants(name2)
    return not variants1.isdisjoint(variants2)

def match_name_part(claim_part: Optional[str], fetched_part: Optional[str], name_type: str) -> float:
    """
    Compare a single name part (first, middle, or last) and return a similarity score between 0 and 1.
    Uses exact match, nickname checks, or string distance measures.
    """
    if not claim_part and not fetched_part:
        return 1.0
    if not claim_part or not fetched_part:
        return 0.5 if name_type == "middle" else 0.0

    claim_part = claim_part.strip().lower()
    fetched_part = fetched_part.strip().lower()

    if claim_part == fetched_part:
        return 1.0

    if name_type == "first" and are_nicknames(claim_part, fetched_part):
        return 1.0

    if name_type in ("first", "middle"):
        if len(claim_part) == 1 and len(fetched_part) == 1 and claim_part[0] == fetched_part[0]:
            return 1.0

    if name_type == "last":
        distance = jellyfish.damerau_levenshtein_distance(claim_part, fetched_part)
        max_len = max(len(claim_part), len(fetched_part))
        similarity = 1.0 - (distance / max_len) if max_len else 0.0
        return similarity if similarity >= 0.8 else 0.0
    elif name_type == "first":
        distance = jellyfish.levenshtein_distance(claim_part, fetched_part)
        similarity = 1.0 - (distance / max(len(claim_part), len(fetched_part)))
        return similarity if similarity >= 0.85 else 0.0
    else:
        try:
            code1 = jellyfish.nysiis(claim_part)
            code2 = jellyfish.nysiis(fetched_part)
        except Exception:
            code1 = code2 = ""
        return 0.8 if code1 == code2 and code1 != "" else 0.0

def evaluate_name(expected_name: Any, fetched_name: Any, other_names: List[Any],
                  score_threshold: float = 80.0) -> Tuple[Dict[str, Any], Optional[float]]:
    """
    Evaluate name compliance by comparing the expected name to the fetched name and alternatives.
    Returns evaluation details and the best match score (or None if no match).

    Args:
        expected_name: The name to match against (string or dict).
        fetched_name: The primary fetched name (string or dict).
        other_names: List of alternative names (strings or dicts).
        score_threshold: Minimum score for a match to be considered compliant (default: 80.0).

    Returns:
        Tuple[Dict[str, Any], Optional[float]]: Evaluation details and best match score.
    """
    claim_name = parse_name(expected_name)

    def score_single_name(claim: Dict[str, Any], fetched: Any) -> Dict[str, Any]:
        fetched_parsed = parse_name(fetched)
        weights = {"first": 40, "middle": 10, "last": 50}
        if not claim["middle"] and not fetched_parsed["middle"]:
            weights["middle"] = 0
        first_score = match_name_part(claim["first"], fetched_parsed["first"], "first")
        middle_score = match_name_part(claim["middle"], fetched_parsed["middle"], "middle") if weights["middle"] else 0.0
        last_score = match_name_part(claim["last"], fetched_parsed["last"], "last")
        total_weight = sum(weights.values())
        total_score = (first_score * weights["first"] +
                       middle_score * weights["middle"] +
                       last_score * weights["last"])
        normalized_score = (total_score / total_weight) * 100.0 if total_weight else 0.0
        return {
            "fetched_name": fetched_parsed,
            "score": round(normalized_score, 2),
            "first_score": round(first_score, 2),
            "middle_score": round(middle_score, 2),
            "last_score": round(last_score, 2),
        }

    matches = []
    main_match = score_single_name(claim_name, fetched_name)
    matches.append({"name_source": "main_fetched_name", **main_match})
    for idx, alt in enumerate(other_names):
        alt_match = score_single_name(claim_name, alt)
        matches.append({"name_source": f"other_names[{idx}]", **alt_match})

    best_match = max(matches, key=lambda x: x["score"])
    best_score = best_match["score"]
    compliance = best_score >= score_threshold

    evaluation_details = {
        "claimed_name": claim_name,
        "all_matches": matches,
        "best_match": best_match,
        "compliance": compliance
    }

    return evaluation_details, best_score