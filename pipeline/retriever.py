from rank_bm25 import BM25Okapi


def qfact_to_text(row):
    """Converts one Qfact dict into a single lowercase string for BM25."""
    parts = [
        row.get('entity',          ''),
        row.get('attribute',       ''),
        str(row.get('value',       '')),
        row.get('unit',            ''),
        row.get('page_title',      ''),
        row.get('section_heading', ''),
        row.get('caption',         ''),
        row.get('row_context',     ''),
    ]
    return ' '.join(filter(None, parts)).lower()


def build_bm25_index(qfacts):
    """
    Builds BM25 index over all Qfacts.
    Call once after load_qfacts_from_db().
    Index position maps 1:1 to qfacts list.
    """
    documents        = [qfact_to_text(row) for row in qfacts]
    tokenized_corpus = [doc.split() for doc in documents]
    bm25             = BM25Okapi(tokenized_corpus)
    print(f"BM25 index built over {len(qfacts)} Qfacts")
    return bm25


def retrieve(parsed_query, bm25, qfacts, top_k=5):
    """
    Retrieves top-k Qfacts for a parsed query.

    Four constraint types:
      superlative → sort entire DB by value (skip BM25)
      threshold   → pre-filter by value, then BM25 on subset
      comparison  → BM25 per entity name separately
      lookup      → pure BM25

    Input  : parsed_query — dict from query_parser.parse_query()
             bm25         — BM25Okapi index
             qfacts       — list of Qfact dicts from DB
             top_k        — number of results to return
    Output : list of result dicts
    """
    constraint      = parsed_query.get('constraint', {})
    constraint_type = constraint.get('type', 'lookup')
    keywords        = parsed_query.get('keywords', [])
    entities        = parsed_query.get('entities', [])
    query_tokens    = [kw.lower() for kw in keywords]

    # ── SUPERLATIVE ────────────────────────────────────────────
    if constraint_type == 'superlative':
        direction   = constraint.get('direction', 'highest')
        reverse     = direction in ['highest', 'largest', 'most', 'biggest']
        valid_rows  = [r for r in qfacts if r.get('value') is not None]
        sorted_rows = sorted(valid_rows,
                             key=lambda x: x.get('value') or 0,
                             reverse=reverse)
        return [_format_result(i, row, score=None)
                for i, row in enumerate(sorted_rows[:top_k])]

    # ── COMPARISON ─────────────────────────────────────────────
    if constraint_type == 'comparison' and entities:
        results = []
        seen    = set()
        for entity in entities:
            entity_tokens = entity.lower().split()
            scores        = bm25.get_scores(entity_tokens)
            top_indices   = scores.argsort()[::-1][:top_k]
            for idx in top_indices:
                row = qfacts[idx]
                key = (row.get('entity'), row.get('value'))
                if key in seen:
                    continue
                seen.add(key)
                results.append(_format_result(len(results), row,
                               score=round(float(scores[idx]), 4)))
                break
        results = sorted(results, key=lambda x: x['value'] or 0, reverse=True)
        for i, r in enumerate(results):
            r['rank'] = i + 1
        return results

    # ── THRESHOLD ──────────────────────────────────────────────
    if constraint_type == 'threshold':
        operator = constraint.get('operator', '').lower()
        limit    = constraint.get('value', 0)

        def passes(val):
            if val is None: return False
            if operator in ['more than', 'above', 'over', 'greater than']: return val > limit
            if operator in ['less than', 'under', 'below']:               return val < limit
            if operator in ['at least']:                                   return val >= limit
            if operator in ['at most', 'no more than']:                   return val <= limit
            if operator in ['exactly']:                                    return val == limit
            return True

        filtered = [r for r in qfacts if passes(r.get('value'))]
        if not filtered:
            return []

        mini_docs   = [qfact_to_text(r) for r in filtered]
        mini_corpus = [doc.split() for doc in mini_docs]
        mini_bm25   = BM25Okapi(mini_corpus)
        mini_scores = mini_bm25.get_scores(query_tokens)
        top_indices = mini_scores.argsort()[::-1][:top_k]
        return [_format_result(i, filtered[idx],
                               score=round(float(mini_scores[idx]), 4))
                for i, idx in enumerate(top_indices)]

    # ── LOOKUP ─────────────────────────────────────────────────
    scores      = bm25.get_scores(query_tokens)
    top_indices = scores.argsort()[::-1][:top_k]
    return [_format_result(i, qfacts[idx],
                           score=round(float(scores[idx]), 4))
            for i, idx in enumerate(top_indices)]


def _format_result(index, row, score=None):
    return {
        'rank'     : index + 1,
        'score'    : score,
        'entity'   : row.get('entity'),
        'attribute': row.get('attribute'),
        'value'    : row.get('value'),
        'unit'     : row.get('unit') or '',
        'context'  : row.get('row_context', ''),
        'source'   : row.get('source_url', ''),
    }


def load_qfacts_from_db():
    """Loads all rows from extractedtable. Returns list of plain dicts."""
    from storage import get_connection
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM extractedtable")
    rows   = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]