import spacy
import json
import os
import openai
import dotenv

dotenv.load_dotenv()

nlp    = spacy.load("en_core_web_sm")
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ENTITY_LABELS = {
    'GPE',      # countries, cities, states
    'ORG',      # organisations
    'PERSON',   # people
    'LOC',      # non-GPE locations (Alps, Pacific)
    'NORP',     # nationalities / groups
    'FAC',      # facilities (stadiums, airports)
    'EVENT',    # events (World Cup, Olympics)
    'PRODUCT',  # products
}

SYSTEM_PROMPT = """
You are a query constraint extractor.
Given a user query, extract the constraint and return ONLY a JSON object.

Constraint types:
- threshold:   a number with an operator (more than, less than, at least, at most, exactly)
- range:       between two numbers
- superlative: highest, lowest, largest, smallest, best, worst, biggest
- comparison:  two or more named entities being compared (bigger, smaller, versus, vs, compare)
- lookup:      vague/qualitative or no constraint at all

Return format examples:
{"type": "threshold",   "operator": "at least", "value": 60000}
{"type": "range",       "min": 100, "max": 500}
{"type": "superlative", "direction": "highest", "keyword": "largest"}
{"type": "comparison",  "entities": ["Wembley", "Camp Nou"]}
{"type": "lookup"}

Return ONLY the JSON. No explanation. No markdown.
"""


def parse_query(query_text):
    """
    Main entry point. Called by pipeline.py.

    Input  : raw query string from user
    Output : {
        original   : str,
        entities   : list[str],
        constraint : dict,
        keywords   : list[str]
    }
    """
    return {
        "original"  : query_text,
        "entities"  : extract_entities(query_text),
        "constraint": extract_constraint(query_text),
        "keywords"  : extract_keywords(query_text),
    }


def extract_entities(query_text):
    doc = nlp(query_text)
    return [ent.text for ent in doc.ents if ent.label_ in ENTITY_LABELS]


def extract_constraint(query_text):
    response = client.chat.completions.create(
        model       = "gpt-4o-mini",
        temperature = 0,
        messages    = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": query_text}
        ]
    )
    return json.loads(response.choices[0].message.content.strip())


def extract_keywords(query_text):
    doc = nlp(query_text)
    return [
        token.lemma_.lower()
        for token in doc
        if not token.is_stop
        and not token.is_punct
        and not token.is_space
        and token.pos_ in ("NOUN", "PROPN", "ADJ", "VERB")
    ]