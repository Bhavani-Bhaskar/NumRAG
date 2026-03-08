storage.py       — DB schema, connection
fetching.py      — Wikipedia scraper, Qfact extractor
query_parser.py  — entity extraction, constraint detection, keywords
retriever.py     — BM25 index, 4 constraint types
generator.py     — RAG prompt, LLM call
verifier.py      — ±10% grounding check, hallucination rate
pipeline.py      — combines all 6 files (you write this)
app.py           — Streamlit UI