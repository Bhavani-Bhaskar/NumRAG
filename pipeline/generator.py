import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def format_context(retrieved_qfacts):
    """
    Converts retrieved Qfact dicts into a numbered context string.
    Each row gets a [Row N] label so the LLM can cite it.
    """
    if not retrieved_qfacts:
        return "No relevant facts retrieved."

    lines = []
    for i, row in enumerate(retrieved_qfacts):
        unit = row.get('unit') or ''
        line = (
            f"[Row {i+1}] "
            f"Entity: {row.get('entity', 'Unknown')} | "
            f"{row.get('attribute', 'Value')}: {row.get('value', 'N/A')} {unit} | "
            f"Context: {row.get('context', '')} | "
            f"Source: {row.get('source', '')}"
        )
        lines.append(line)

    return "\n".join(lines)


def build_prompt(query, context):
    """
    Builds a strict RAG prompt.
    Forces the LLM to only use retrieved numbers and cite row sources.
    """
    return f"""You are a precise numerical fact-answering assistant.
Your only job is to answer questions using the retrieved facts below.

RETRIEVED FACTS:
{context}

QUESTION: {query}

INSTRUCTIONS:
- Answer using ONLY the numbers present in the RETRIEVED FACTS above.
- For every number you state, cite which Row it came from e.g. [Row 2].
- Do NOT use any numbers from your own training knowledge.
- Do NOT guess, estimate, or approximate unless the facts say "approximately".
- If the retrieved facts do not contain enough information to answer, say exactly:
  "Not found in retrieved data."
- Keep your answer concise and factual.

Answer:"""


def generate_answer(query, retrieved_qfacts):
    """
    Full RAG generation step. Called by pipeline.py.

    Input  : query            — user question string
             retrieved_qfacts — list of result dicts from retriever
    Output : {
        query   : str,
        context : str  (formatted rows),
        prompt  : str  (full prompt sent to LLM),
        answer  : str  (LLM response),
        rows    : list (original retrieved dicts)
    }
    """
    context  = format_context(retrieved_qfacts)
    prompt   = build_prompt(query, context)

    response = client.chat.completions.create(
        model       = "gpt-4o-mini",
        temperature = 0,
        max_tokens  = 512,
        messages    = [
            {
                "role"   : "system",
                "content": (
                    "You are a precise fact-answering assistant. "
                    "You only use numbers from retrieved facts. "
                    "You always cite row numbers."
                )
            },
            {"role": "user", "content": prompt}
        ]
    )

    return {
        "query"  : query,
        "context": context,
        "prompt" : prompt,
        "answer" : response.choices[0].message.content.strip(),
        "rows"   : retrieved_qfacts,
    }