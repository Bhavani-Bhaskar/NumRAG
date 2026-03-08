import requests
from bs4 import BeautifulSoup
import re
import os
import json
from datetime import datetime
from storage import get_connection, init_db

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

LOG_FILE_PATH = "data/logs/output.txt"
JSON_SAVE_DIR = "data/raw"

# ─────────────────────────────────────────────
# 10 WIKIPEDIA URLS TO SCRAPE
# Run fetching.py once to populate the database.
# Each URL becomes a different domain of Qfacts.
# ─────────────────────────────────────────────
WIKIPEDIA_URLS = [
    ("https://en.wikipedia.org/wiki/List_of_stadiums_by_capacity",
     "stadiums_by_capacity"),

    ("https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)",
     "countries_by_population"),

    ("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
     "countries_by_gdp"),

    ("https://en.wikipedia.org/wiki/List_of_highest_mountains_on_Earth",
     "highest_mountains"),

    ("https://en.wikipedia.org/wiki/List_of_rivers_by_length",
     "rivers_by_length"),

    ("https://en.wikipedia.org/wiki/List_of_tallest_buildings",
     "tallest_buildings"),

    ("https://en.wikipedia.org/wiki/List_of_largest_universities_by_enrollment",
     "universities_by_enrollment"),

    ("https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_area",
     "countries_by_area"),

    ("https://en.wikipedia.org/wiki/List_of_busiest_airports_by_passenger_traffic",
     "busiest_airports"),

    ("https://en.wikipedia.org/wiki/List_of_largest_reservoirs",
     "largest_reservoirs"),
]

# ─────────────────────────────────────────────
# UNIT KEYWORDS — built once at module level
# Key   = unit string  e.g. "seats"
# Value = column header keywords that imply it
# ─────────────────────────────────────────────
UNIT_KEYWORDS = {
    "seats":    ["capacity", "seating", "stadium capacity",
                 "theater capacity", "venue capacity"],
    "people":   ["population", "attendance", "inhabitants", "residents",
                 "visitors", "tourists", "workforce", "employees",
                 "staff", "headcount", "crowd size"],
    "students": ["enrollment", "students", "graduates", "alumni",
                 "scholars", "pupils", "undergraduates"],
    "units":    ["sales volume", "production", "output",
                 "manufactured", "shipments", "deliveries"],
    "votes":    ["ballots", "votes cast", "electoral votes", "poll count"],
    "beds":     ["hospital capacity", "hospital beds",
                 "icu capacity", "ward capacity"],
    "floors":   ["stories", "levels", "floors above ground"],
    "medals":   ["gold medals", "silver medals", "bronze medals",
                 "olympic medals", "awards won"],
    "km":       ["distance", "length", "route length", "road length",
                 "track length", "pipeline length", "coastline length",
                 "border length", "range"],
    "m":        ["height", "elevation", "depth", "altitude", "width",
                 "span", "diameter", "ceiling height",
                 "water depth", "building height"],
    "mm":       ["thickness", "rainfall", "precipitation",
                 "gauge", "caliber"],
    "miles":    ["highway length", "trail length",
                 "driving distance", "nautical range"],
    "km²":      ["area", "land area", "surface area", "territory",
                 "country size", "forest area", "desert area",
                 "ocean area", "national park area"],
    "m²":       ["floor area", "building area", "room size",
                 "plot size", "office space"],
    "hectares": ["farmland", "agricultural area",
                 "plantation size", "crop area"],
    "acres":    ["land size", "estate area", "ranch size", "farm size"],
    "liters":   ["volume", "tank capacity", "fuel capacity",
                 "liquid volume", "engine displacement"],
    "m³":       ["cubic volume", "reservoir capacity",
                 "storage volume", "dam capacity"],
    "barrels":  ["oil production", "crude oil",
                 "oil reserves", "daily output (oil)"],
    "tonnes":   ["cargo", "freight", "production tonnage",
                 "mining output", "ore extracted",
                 "emission weight", "displacement (ship)"],
    "kg":       ["weight", "mass", "payload",
                 "cargo weight", "birth weight"],
    "years":    ["age", "lifespan", "duration", "reign",
                 "service years", "founded", "established",
                 "tenure", "sentence", "warranty"],
    "hours":    ["flight time", "working hours",
                 "duration (hours)", "shift length"],
    "minutes":  ["runtime", "match duration",
                 "cook time", "travel time (minutes)"],
    "seconds":  ["reaction time", "lap time",
                 "sprint time", "response time"],
    "km/h":     ["speed", "average speed", "top speed", "wind speed",
                 "train speed", "car speed",
                 "maximum speed", "cruise speed"],
    "mph":      ["speed (imperial)", "top speed (mph)", "highway speed"],
    "°C":       ["temperature", "boiling point", "melting point",
                 "average temperature", "high temperature",
                 "low temperature", "body temperature"],
    "°F":       ["temperature (fahrenheit)",
                 "oven temperature", "weather (us)"],
    "kWh":      ["energy consumption", "electricity usage",
                 "power consumption", "battery capacity", "energy output"],
    "MW":       ["power output", "plant capacity", "solar capacity",
                 "wind farm capacity", "generating capacity"],
    "GW":       ["national power capacity",
                 "grid capacity", "large power output"],
    "Hz":       ["frequency", "refresh rate",
                 "vibration frequency", "sound frequency"],
    "GHz":      ["processor speed", "clock speed",
                 "cpu frequency", "gpu frequency"],
    "GB":       ["storage capacity", "memory size",
                 "file size", "ram size", "disk size"],
    "TB":       ["data storage", "hard drive capacity",
                 "database size", "backup size"],
    "Mbps":     ["internet speed", "bandwidth",
                 "download speed", "upload speed", "data rate"],
    "USD":      ["gdp", "revenue", "income", "budget", "cost",
                 "profit", "price", "market cap", "valuation",
                 "debt", "funding", "investment", "net worth",
                 "salary", "wage", "fine", "penalty", "grant", "subsidy"],
    "EUR":      ["european revenue", "euro budget",
                 "eurozone gdp", "price (europe)"],
    "GBP":      ["uk revenue", "british budget", "price (uk)"],
    "INR":      ["indian rupee", "india gdp", "price (india)"],
    "%":        ["rate", "percentage", "growth", "literacy",
                 "unemployment rate", "inflation", "interest rate",
                 "tax rate", "efficiency", "accuracy", "humidity",
                 "probability", "approval rating", "market share",
                 "pass rate", "mortality rate", "fertility rate",
                 "poverty rate"],
    "Pa":       ["pressure", "atmospheric pressure", "fluid pressure"],
    "dB":       ["sound level", "noise level", "loudness",
                 "decibels", "audio level"],
    "V":        ["voltage", "electric potential", "battery voltage"],
    "years old":["age", "average age", "median age",
                 "retirement age", "voting age"],
}

EMPTY_CELL_VALUES = {
    '', '-', '—', '–', 'n/a', 'na', 'unknown',
    'none', 'nil', 'tbd', 'tba', '?'
}


def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'\[[\w\s]+\]', '', text)
    text = re.sub(r'[\n\t\r]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def save_to_json(data, filename):
    os.makedirs(JSON_SAVE_DIR, exist_ok=True)
    data["fetched_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["total_tables"] = len(data.get("tables", []))
    for table in data.get("tables", []):
        table["total_rows"] = len(table.get("rows", []))
    file_path = os.path.join(JSON_SAVE_DIR, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    total_rows = sum(t["total_rows"] for t in data.get("tables", []))
    print(f"  JSON → {file_path}  ({data['total_tables']} tables, {total_rows} rows)")


# ─────────────────────────────────────────────
# QUANTITY NORMALIZER
# Pass 1: detect unit from symbols in cell text
# Pass 2: infer unit from column header keywords
# Fix:    re.findall takes first number only
#         (prevents merged numbers like 150000113281)
# ─────────────────────────────────────────────
def normalize_quantity(raw_text, column_header=""):
    if not raw_text or not isinstance(raw_text, str):
        return None, None

    text = raw_text.strip()
    unit = None

    if '%' in text:
        unit = '%'
    elif any(c in text for c in ['$', '€', '£', '¥']):
        unit = 'USD'
    elif re.search(r'\bkm²\b',       text, re.IGNORECASE): unit = 'km²'
    elif re.search(r'\bm²\b',        text, re.IGNORECASE): unit = 'm²'
    elif re.search(r'\bkm/h\b',      text, re.IGNORECASE): unit = 'km/h'
    elif re.search(r'\bkwh\b',       text, re.IGNORECASE): unit = 'kWh'
    elif re.search(r'\bmph\b',       text, re.IGNORECASE): unit = 'mph'
    elif re.search(r'\bgw\b',        text, re.IGNORECASE): unit = 'GW'
    elif re.search(r'\bmw\b',        text, re.IGNORECASE): unit = 'MW'
    elif re.search(r'\bkg\b',        text, re.IGNORECASE): unit = 'kg'
    elif re.search(r'\bkm\b',        text, re.IGNORECASE): unit = 'km'
    elif re.search(r'\b°C\b',        text):                unit = '°C'
    elif re.search(r'\b°F\b',        text):                unit = '°F'
    elif re.search(r'\btonnes?\b',   text, re.IGNORECASE): unit = 'tonnes'
    elif re.search(r'\bseats?\b',    text, re.IGNORECASE): unit = 'seats'
    elif re.search(r'\bstudents?\b', text, re.IGNORECASE): unit = 'students'

    if unit is None and column_header:
        header_lower = column_header.lower().strip()
        for unit_key, keywords in UNIT_KEYWORDS.items():
            pattern = (r'\b(' + '|'.join(re.escape(kw) for kw in keywords) + r')\b')
            if re.search(pattern, header_lower, re.IGNORECASE):
                unit = unit_key
                break

    text = re.sub(r'[$€£¥]', '', text)
    text = re.sub(r'[~≈*]',  '', text)
    text = re.sub(r'\b(ca|c|approx|approximately|about|around|nearly|over)\b\.?',
                  '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[.*?\]', '', text)
    text = text.strip()

    parts = re.split(r'[–—]', text)
    if len(parts) > 1:
        text = parts[0].strip()

    text = text.replace(',', '')

    multiplier = 1.0
    if re.search(r'\bbillion\b|\bbn\b', text, re.IGNORECASE):
        multiplier = 1e9
        text = re.sub(r'\bbillion\b|\bbn\b', '', text, flags=re.IGNORECASE)
    elif re.search(r'\bmillion\b|\bmln\b', text, re.IGNORECASE):
        multiplier = 1e6
        text = re.sub(r'\bmillion\b|\bmln\b', '', text, flags=re.IGNORECASE)
    elif re.search(r'\bthousand\b', text, re.IGNORECASE):
        multiplier = 1e3
        text = re.sub(r'\bthousand\b', '', text, flags=re.IGNORECASE)

    number_matches = re.findall(r'\d+\.?\d*', text)
    if not number_matches:
        return None, None

    try:
        return float(number_matches[0]) * multiplier, unit
    except ValueError:
        return None, None


# ─────────────────────────────────────────────
# COLUMN CLASSIFIER — data-driven
# numeric_score[i] = how many cells in col i parse as float
# text_score[i]    = how many cells in col i are plain text
# Q-col = highest numeric_score
# E-col = highest text_score, excluding Q-col
# ─────────────────────────────────────────────
def classify_columns(headers, rows):
    if not headers or not rows:
        return 0, 1

    num_cols      = len(headers)
    numeric_score = [0] * num_cols
    text_score    = [0] * num_cols

    for row in rows:
        for i, cell in enumerate(row):
            if i >= num_cols:
                break
            cell_clean = cell.strip().lower()
            if cell_clean in EMPTY_CELL_VALUES:
                continue
            val, _ = normalize_quantity(cell)
            if val is not None:
                numeric_score[i] += 1
            else:
                text_score[i] += 1

    q_col      = numeric_score.index(max(numeric_score))
    e_col      = None
    best_score = -1

    for i in range(num_cols):
        if i == q_col:
            continue
        if text_score[i] > best_score:
            best_score = text_score[i]
            e_col      = i

    if e_col is None:
        e_col = 0 if q_col != 0 else 1

    return e_col, q_col


# ─────────────────────────────────────────────
# INSERT ONE QFACT
# Exact duplicate check on all 10 fields.
# Returns True if inserted, False if duplicate.
# ─────────────────────────────────────────────
def insert_qfact(qfact):
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT COUNT(*) FROM extractedtable
        WHERE entity=? AND attribute=? AND value=? AND unit=?
        AND page_title=? AND section_heading=? AND caption=?
        AND row_context=? AND surrounding_text=? AND source_url=?
    ''', (
        qfact['entity'],   qfact['attribute'],
        qfact['value'],    qfact['unit'],
        qfact['page_title'], qfact['section_heading'],
        qfact['caption'],  qfact['row_context'],
        qfact['surrounding_text'], qfact['source_url']
    ))

    if cursor.fetchone()[0] > 0:
        conn.close()
        return False

    cursor.execute('''
        INSERT INTO extractedtable (
            entity, attribute, value, unit,
            page_title, section_heading, caption,
            row_context, surrounding_text, source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        qfact['entity'],   qfact['attribute'],
        qfact['value'],    qfact['unit'],
        qfact['page_title'], qfact['section_heading'],
        qfact['caption'],  qfact['row_context'],
        qfact['surrounding_text'], qfact['source_url']
    ))

    conn.commit()
    conn.close()
    return True


# ─────────────────────────────────────────────
# EXTRACT QFACTS FROM PAGE DATA AND STORE
# ─────────────────────────────────────────────
def extract_and_store_qfacts(page_data):
    total_inserted   = 0
    total_skipped    = 0
    total_duplicates = 0

    for table in page_data.get('tables', []):
        headers          = table.get('column_headers', [])
        rows             = table.get('rows', [])
        caption          = table.get('caption', '')
        section_heading  = table.get('section_heading', '')
        page_title       = table.get('page_title', '')
        source_url       = table.get('source_url', '')
        surrounding_text = table.get('surrounding_text', '')

        if not headers or not rows:
            continue

        e_col, q_col = classify_columns(headers, rows)
        attribute    = headers[q_col]

        for row in rows:
            if len(row) <= max(e_col, q_col):
                total_skipped += 1
                continue

            entity = row[e_col].strip()
            if not entity:
                total_skipped += 1
                continue

            value, unit = normalize_quantity(row[q_col], attribute)
            if value is None:
                total_skipped += 1
                continue

            other_cells = [
                row[i] for i in range(len(row))
                if i != e_col and i != q_col and row[i].strip()
            ]
            row_context = " | ".join(other_cells)

            qfact = {
                'entity'          : entity,
                'attribute'       : attribute,
                'value'           : value,
                'unit'            : unit,
                'page_title'      : page_title,
                'section_heading' : section_heading,
                'caption'         : caption,
                'row_context'     : row_context,
                'surrounding_text': surrounding_text,
                'source_url'      : source_url
            }

            if insert_qfact(qfact):
                total_inserted += 1
            else:
                total_duplicates += 1

    print(f"  Inserted: {total_inserted} | "
          f"Skipped: {total_skipped} | "
          f"Duplicates: {total_duplicates}")
    return total_inserted


# ─────────────────────────────────────────────
# FETCH ONE WIKIPEDIA PAGE
# ─────────────────────────────────────────────
def fetch_wikipedia_page(url):
    print(f"Fetching: {url}")
    response = requests.get(url, headers=HEADERS, timeout=10)

    if response.status_code != 200:
        print(f"  ERROR {response.status_code}")
        return None

    soup       = BeautifulSoup(response.content, 'html.parser')
    title_tag  = soup.find('h1', {'id': 'firstHeading'})
    page_title = title_tag.get_text(strip=True) if title_tag else "Unknown"

    content_div  = soup.find('div', {'id': 'mw-content-text'})
    introduction = ""
    if content_div:
        first_para = content_div.find('p')
        if first_para:
            introduction = clean_text(first_para.get_text())

    tables_data = []
    all_tables  = soup.find_all('table', {'class': 'wikitable'})
    print(f"  Found {len(all_tables)} wikitables — '{page_title}'")

    for table_index, table in enumerate(all_tables):
        table_data  = {}
        caption_tag = table.find('caption')
        table_data['caption'] = clean_text(caption_tag.get_text()) if caption_tag else ""

        section_heading = ""
        for sibling in table.find_all_previous(['h2', 'h3']):
            heading_text = sibling.get_text(strip=True)
            if heading_text not in ['Contents', 'References',
                                     'See also', 'External links',
                                     'Notes', 'Bibliography']:
                section_heading = clean_text(heading_text)
                break
        table_data['section_heading'] = section_heading

        headers = []
        header_row = table.find('tr')
        if header_row:
            for th in header_row.find_all('th'):
                headers.append(clean_text(th.get_text()))
        table_data['column_headers'] = headers

        rows = []
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if not cells:
                continue
            row_data = [clean_text(cell.get_text()) for cell in cells]
            if row_data:
                rows.append(row_data)
        table_data['rows'] = rows

        surrounding_text = ""
        prev_para = table.find_previous('p')
        if prev_para:
            words = clean_text(prev_para.get_text()).split()
            surrounding_text += " ".join(words[-100:])
        next_para = table.find_next('p')
        if next_para:
            words = clean_text(next_para.get_text()).split()
            surrounding_text += " " + " ".join(words[:100])
        table_data['surrounding_text'] = surrounding_text.strip()

        table_data['page_title']  = page_title
        table_data['source_url']  = url
        table_data['table_index'] = table_index
        tables_data.append(table_data)

    return {
        'page_title'  : page_title,
        'introduction': introduction,
        'source_url'  : url,
        'tables'      : tables_data
    }


# ─────────────────────────────────────────────
# RUN — scrape all 10 URLs and populate DB
# Run this file ONCE before starting the app.
#   python3 pipeline/fetching.py
# ─────────────────────────────────────────────
if __name__ == "__main__":

    init_db()
    print("Database ready: data/qfacts.db\n")

    grand_total = 0

    for url, filename in WIKIPEDIA_URLS:
        print(f"\n{'─'*60}")
        result = fetch_wikipedia_page(url)
        if result:
            save_to_json(result, filename=filename)
            count       = extract_and_store_qfacts(result)
            grand_total += count
        else:
            print(f"  FAILED: {url}")

    # Final DB summary
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM extractedtable")
    total_in_db = cursor.fetchone()[0]
    cursor.execute("""
        SELECT page_title, COUNT(*) as cnt
        FROM extractedtable
        GROUP BY page_title
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  NEW Qfacts inserted : {grand_total}")
    print(f"  TOTAL in DB         : {total_in_db}")
    print(f"\n  Breakdown by page:")
    for row in rows:
        print(f"    {row[0]:<45} {row[1]:>5} rows")
    print(f"{'='*60}")