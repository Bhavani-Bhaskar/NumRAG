import re


def extract_numbers_from_text(text):
    """
    Extracts meaningful numeric values from LLM answer text.
    Filters out:
      - Row indices like [1], [2]  (val < 100)
      - Years like 1989, 2023      (1800–2100 range)
    """
    cleaned = text.replace(',', '')
    matches = re.findall(r'\b\d+\.?\d*\b', cleaned)

    numbers = []
    for m in matches:
        val = float(m)
        if val < 100:
            continue
        if 1800 <= val <= 2100 and val == int(val):
            continue
        numbers.append(val)

    return numbers


def check_number_against_rows(number, retrieved_rows):
    """
    Checks one number against all retrieved row values.
    Uses the NuFact paper's ±10% tolerance rule (Section 2.2.2):

      ratio = |answer_number - row_value| / row_value

      ratio == 0.0   → ✅ Grounded      (exact match)
      ratio <= 0.10  → ⚠️ Approximated  (within ±10%)
      ratio >  0.10  → ❌ Hallucinated

    Input  : number         — float from LLM answer
             retrieved_rows — list of result dicts from retriever
    Output : {
        number      : float,
        verdict     : 'grounded' / 'approximated' / 'hallucinated',
        badge       : emoji string,
        matched_row : int or None  (0-based index),
        best_ratio  : float or None
    }
    """
    best_ratio     = None
    best_row_index = None

    for i, row in enumerate(retrieved_rows):
        row_value = row.get('value')
        if row_value is None or row_value == 0:
            continue
        ratio = abs(number - row_value) / abs(row_value)
        if best_ratio is None or ratio < best_ratio:
            best_ratio     = ratio
            best_row_index = i

    if best_ratio is None:
        verdict, badge = 'hallucinated', '❌ Hallucinated'
    elif best_ratio == 0.0:
        verdict, badge = 'grounded',     '✅ Grounded'
    elif best_ratio <= 0.10:
        verdict, badge = 'approximated', '⚠️ Approximated'
    else:
        verdict, badge = 'hallucinated', '❌ Hallucinated'

    return {
        'number'     : number,
        'verdict'    : verdict,
        'badge'      : badge,
        'matched_row': best_row_index,
        'best_ratio' : round(best_ratio, 4) if best_ratio is not None else None
    }


def verify_answer(answer_text, retrieved_rows):
    """
    Full verification of one LLM answer. Called by pipeline.py.

    Input  : answer_text    — string from generator
             retrieved_rows — list of result dicts from retriever
    Output : {
        numbers_checked    : int,
        results            : list of per-number check dicts,
        grounded_count     : int,
        approximated_count : int,
        hallucinated_count : int,
        hallucination_rate : float 0.0–1.0,
        verdict_summary    : str
    }
    """
    numbers = extract_numbers_from_text(answer_text)

    if not numbers:
        return {
            'numbers_checked'    : 0,
            'results'            : [],
            'grounded_count'     : 0,
            'approximated_count' : 0,
            'hallucinated_count' : 0,
            'hallucination_rate' : 0.0,
            'verdict_summary'    : 'No verifiable numbers found in answer.'
        }

    results = [check_number_against_rows(n, retrieved_rows) for n in numbers]

    grounded_count     = sum(1 for r in results if r['verdict'] == 'grounded')
    approximated_count = sum(1 for r in results if r['verdict'] == 'approximated')
    hallucinated_count = sum(1 for r in results if r['verdict'] == 'hallucinated')
    total              = len(results)
    hallucination_rate = round(hallucinated_count / total, 4) if total > 0 else 0.0

    verdict_summary = (
        f"{grounded_count}/{total} Grounded  |  "
        f"{approximated_count}/{total} Approximated  |  "
        f"{hallucinated_count}/{total} Hallucinated  |  "
        f"Hallucination Rate: {hallucination_rate:.1%}"
    )

    return {
        'numbers_checked'    : total,
        'results'            : results,
        'grounded_count'     : grounded_count,
        'approximated_count' : approximated_count,
        'hallucinated_count' : hallucinated_count,
        'hallucination_rate' : hallucination_rate,
        'verdict_summary'    : verdict_summary
    }