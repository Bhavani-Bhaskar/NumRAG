from retriever    import load_qfacts_from_db, build_bm25_index, retrieve
from query_parser import parse_query
from generator    import generate_answer
from verifier     import verify_answer

# ─────────────────────────────────────────────
# GLOBAL STATE — loaded once when pipeline.py
# is first imported by app.py.
#
# WHY: Loading 3000+ Qfacts and building a BM25
# index takes ~1 second. We do it once at startup,
# not on every query. Streamlit reruns the script
# on each interaction — the `if not` guards prevent
# reloading on every button click.
# ─────────────────────────────────────────────
_qfacts = None
_bm25   = None


def _load():
    """Loads DB and builds BM25 index if not already loaded."""
    global _qfacts, _bm25
    if _qfacts is None:
        _qfacts = load_qfacts_from_db()
        _bm25   = build_bm25_index(_qfacts)


def run_pipeline(query: str, top_k: int = 5) -> dict:
    """
    Full NumRAG pipeline for one query.
    This is the single function app.py calls.

    Steps:
      1. parse_query      — extract entities, constraint, keywords
      2. retrieve         — BM25 retrieval from Qfacts DB
      3. generate_answer  — RAG prompt → GPT-4o-mini → answer
      4. verify_answer    — ±10% grounding check per number

    Input  : query — raw user query string
             top_k — number of Qfacts to retrieve (default 5)

    Output : {
        query        : str   — original query
        parsed       : dict  — entities, constraint, keywords
        retrieved    : list  — top-k Qfact result dicts
        context      : str   — formatted rows sent to LLM
        answer       : str   — LLM response
        verification : dict  — grounding verdicts + hallucination rate
    }
    """
    _load()

    # Step 1 — Parse
    parsed = parse_query(query)

    # Step 2 — Retrieve
    retrieved = retrieve(parsed, _bm25, _qfacts, top_k=top_k)

    # Step 3 — Generate
    generation = generate_answer(query, retrieved)

    # Step 4 — Verify
    verification = verify_answer(generation['answer'], retrieved)

    return {
        'query'       : query,
        'parsed'      : parsed,
        'retrieved'   : retrieved,
        'context'     : generation['context'],
        'answer'      : generation['answer'],
        'verification': verification,
    }


# ─────────────────────────────────────────────
# QUICK CLI TEST
# Run: python3 pipeline/pipeline.py
# ─────────────────────────────────────────────
if __name__ == "__main__":

    TEST_QUERIES = [
        "Which stadium has the highest capacity in the world?",
        "What is the capacity of Wembley Stadium?",
        "Stadiums in Australia with more than 80000 seats",
        "Which is bigger, Wembley or Camp Nou?",
        "Show me stadiums with at least 100000 seats",
    ]

    for query in TEST_QUERIES:
        print(f"\n{'='*65}")
        print(f"QUERY : {query}")
        print(f"{'='*65}")

        result = run_pipeline(query)

        print(f"\nCONSTRAINT : {result['parsed']['constraint']}")
        print(f"ENTITIES   : {result['parsed']['entities']}")
        print(f"KEYWORDS   : {result['parsed']['keywords']}")

        print(f"\nRETRIEVED ({len(result['retrieved'])} rows):")
        for r in result['retrieved']:
            print(f"  [{r['rank']}] {r['entity']} — "
                  f"{r['value']} {r['unit']}")

        print(f"\nANSWER:\n{result['answer']}")

        v = result['verification']
        print(f"\nVERIFICATION:")
        for r in v['results']:
            print(f"  {r['badge']}  {r['number']}")
        print(f"  SUMMARY: {v['verdict_summary']}")