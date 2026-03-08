import streamlit as st
import sys
import os

# ── Make sure pipeline/ is importable ─────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pipeline'))

from pipeline import run_pipeline

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title = "NumRAG",
    page_icon  = "🔢",
    layout     = "wide"
)

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.title("🔢 NumRAG")
st.caption(
    "Numerical Fact Retrieval and Verification · "
    "Grounded in Wikipedia tables · "
    "Inspired by QuTE (SIGMOD 2021) and NuFact (CIKM 2025)"
)
st.divider()

# ─────────────────────────────────────────────
# SIDEBAR — controls
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Settings")
    top_k = st.slider(
        label   = "Top-K retrieved rows",
        min_value = 1,
        max_value = 10,
        value     = 5,
        help      = "How many Qfact rows to retrieve before generating the answer."
    )
    st.divider()
    st.markdown("**Example queries**")
    examples = [
        "Which stadium has the highest capacity in the world?",
        "What is the population of China?",
        "Which is bigger, Wembley or Camp Nou?",
        "Stadiums with more than 100000 seats",
        "What is the length of the Nile river?",
        "Tallest building in the world?",
        "Countries with GDP above 1 trillion?",
        "What is the area of Russia?",
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state['query_input'] = ex

# ─────────────────────────────────────────────
# QUERY INPUT
# ─────────────────────────────────────────────
query = st.text_input(
    label       = "Ask a numerical question",
    placeholder = "e.g. Which river is the longest in the world?",
    key         = "query_input"
)

run = st.button("🔍 Run", type="primary", use_container_width=True)

# ─────────────────────────────────────────────
# PIPELINE — runs only when button clicked
# ─────────────────────────────────────────────
if run and query.strip():

    with st.spinner("Retrieving facts and generating answer..."):
        try:
            result = run_pipeline(query.strip(), top_k=top_k)
        except Exception as e:
            st.error(f"Pipeline error: {e}")
            st.stop()

    parsed       = result['parsed']
    retrieved    = result['retrieved']
    answer       = result['answer']
    verification = result['verification']

    # ── ANSWER ────────────────────────────────────────────────
    st.subheader("💬 Answer")
    st.markdown(f"> {answer}")

    # ── VERIFICATION SUMMARY ──────────────────────────────────
    st.subheader("🔎 Verification")

    v = verification
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Numbers checked",  v['numbers_checked'])
    col2.metric("✅ Grounded",       v['grounded_count'])
    col3.metric("⚠️ Approximated",   v['approximated_count'])
    col4.metric("❌ Hallucinated",   v['hallucinated_count'])

    # Hallucination rate colour
    rate = v['hallucination_rate']
    if rate == 0.0:
        st.success(f"Hallucination Rate: {rate:.1%} — Fully grounded")
    elif rate <= 0.25:
        st.warning(f"Hallucination Rate: {rate:.1%} — Mostly grounded")
    else:
        st.error(f"Hallucination Rate: {rate:.1%} — High hallucination")

    # Per-number verdicts
    if v['results']:
        st.markdown("**Per-number breakdown**")
        for r in v['results']:
            if r['matched_row'] is not None:
                matched  = retrieved[r['matched_row']]
                row_info = (f"Row {r['matched_row']+1} · "
                            f"{matched.get('entity')} = "
                            f"{matched.get('value')} {matched.get('unit','')}")
            else:
                row_info = "No matching row"

            deviation = (
                "exact" if r['best_ratio'] == 0.0
                else f"±{r['best_ratio']*100:.1f}%"
                if r['best_ratio'] is not None
                else "N/A"
            )
            st.markdown(
                f"{r['badge']} &nbsp; **{r['number']}** &nbsp;·&nbsp; "
                f"{row_info} &nbsp;·&nbsp; deviation: {deviation}",
                unsafe_allow_html=True
            )

    st.divider()

    # ── QUERY PARSE DETAILS ───────────────────────────────────
    with st.expander("🧠 Query Analysis", expanded=False):
        col_a, col_b, col_c = st.columns(3)
        col_a.markdown(f"**Constraint**\n\n`{parsed['constraint']}`")
        col_b.markdown(f"**Entities**\n\n`{parsed['entities']}`")
        col_c.markdown(f"**Keywords**\n\n`{parsed['keywords']}`")

    # ── RETRIEVED QFACTS ──────────────────────────────────────
    with st.expander(f"📋 Retrieved Qfacts (top {len(retrieved)})", expanded=False):
        for r in retrieved:
            score_str = f"{r['score']:.4f}" if r['score'] else "sorted"
            st.markdown(
                f"**[{r['rank']}]** {r['entity']} — "
                f"**{r['attribute']}**: {r['value']} {r['unit']}  \n"
                f"*Context*: {r['context'][:80]}...  \n"
                f"*Score*: {score_str} · "
                f"[Source]({r['source']})"
            )
            st.divider()

elif run and not query.strip():
    st.warning("Please enter a query first.")