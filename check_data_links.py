import csv
import os
import re
import difflib
import unicodedata
from collections import defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_LINKS_DIR = os.path.join(ROOT, "2021", "data_links")
MUNICIPALITIES_CSV = os.path.join(ROOT, "municipality_names_with_page.csv")
LOG_FILE = os.path.join(ROOT, "scraped_munis.log")
OUT_DIR = os.path.join(ROOT, "2021", "summary_stats")
OUT_CSV = os.path.join(OUT_DIR, "munis_check.csv")


def normalize_name(name: str) -> str:
    # Remove accents, lowercase, replace non-alnum by underscore, collapse underscores
    if name is None:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w]+", "_", s)  # anything not alnum/_ -> _
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s


def load_municipalities(path: str):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Expect header with at least Name and Page (per provided example)
        for r in reader:
            rows.append(r)
    return rows, reader.fieldnames if 'reader' in locals() else None


def assign_numbers(rows):
    # numbering: number = (page-1)*10 + index_in_page (index_in_page starts at 1)
    by_page = defaultdict(list)
    for r in rows:
        try:
            page = int(r.get("Page", 0))
        except Exception:
            page = 0
        by_page[page].append(r)
    out = []
    for page in sorted(by_page.keys()):
        group = by_page[page]
        for idx, r in enumerate(group, start=1):
            number = (page - 1) * 10 + idx
            r["_number"] = number
            out.append(r)
    return out


def parse_log(path: str):
    """
    Parse log lines that begin with: <number>,...
    For each municipality number gather all entries (in order).
    """
    entries = defaultdict(list)
    if not os.path.exists(path):
        return entries
    # Accept multiline log messages: a line starting with a number begins a new entry,
    # lines that do not start with a number are continuations of the previous entry.
    pattern = re.compile(r"^\s*(\d+)\s*,\s*(.*)$")
    current_num = None
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            m = pattern.match(line)
            if m:
                num = int(m.group(1))
                rest = m.group(2).strip()
                entries[num].append(rest)
                current_num = num
            else:
                # continuation line: append to last entry for current_num
                if current_num is not None:
                    cont = line.strip()
                    if cont:
                        # join with a space to preserve readability
                        entries[current_num][-1] = entries[current_num][-1] + " " + cont
    return entries


def analyze_log_entries(entry_list):
    """
    Given list of log entry strings for a municipality (in chronological order),
    produce:
      - log_attempt: "started,attempt_0" if any entry contains both started and attempt_0
      - log_result: last entry excluding tokens 'started' and 'attempt_*'; return joined remainder
    """
    attempt_val = ""
    for e in entry_list:
        if "started" in e and re.search(r"attempt[_\s-]*0|attempt\s*0", e):
            attempt_val = "started,attempt_0"
            break

    # find last meaningful result
    result = ""
    for e in reversed(entry_list):
        # split by comma and strip tokens
        tokens = [t.strip() for t in e.split(",") if t.strip()]
        # remove 'started' and 'attempt_*' tokens
        tokens_clean = [t for t in tokens if not (t.lower() == "started" or re.match(r"attempt[_\s-]*\d+|attempt\s*\d+", t.lower()))]
        if not tokens_clean:
            continue
        # if first meaningful token is simple flags like 'success', 'bayern_skip', 'no_bundestagswahl' keep it
        # else join remaining tokens as message (strip leading 'Message:' if present)
        joined = ", ".join(tokens_clean)
        joined = re.sub(r"^message\s*:\s*", "", joined, flags=re.I)
        result = joined
        break

    return attempt_val, result


def ensure_out_dir(path):
    os.makedirs(path, exist_ok=True)


def _clean_field(s):
    if s is None:
        return ""
    t = str(s)
    # Remove control characters (category 'C') which may include unexpected
    # line/paragraph separators that break terminal display.
    t = "".join(ch if not unicodedata.category(ch).startswith("C") else " " for ch in t)
    # Explicitly replace common Unicode line separators
    t = t.replace("\u2028", " ").replace("\u2029", " ")
    # Normalize CR/LF and collapse whitespace
    t = re.sub(r"[\r\n]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # Replace internal commas with semicolons to improve terminal/text viewing
    t = t.replace(",", ";")
    return t


def main():
    # load municipalities
    if not os.path.exists(MUNICIPALITIES_CSV):
        raise SystemExit(f"Municipality CSV not found: {MUNICIPALITIES_CSV}")

    rows, fieldnames = load_municipalities(MUNICIPALITIES_CSV)
    numbered = assign_numbers(rows)

    # parse log
    log_entries = parse_log(LOG_FILE)

    # build normalized mapping of data_links filenames for robust lookup
    norm_map = {}
    if os.path.isdir(DATA_LINKS_DIR):
        for fname in os.listdir(DATA_LINKS_DIR):
            if not fname.endswith("_data_links.csv"):
                continue
            base = fname[: -len("_data_links.csv")]
            key = normalize_name(base)
            norm_map.setdefault(key, []).append(fname)

    # build output rows
    out_rows = []
    fuzzy_used = []
    for r in numbered:
        number = r.get("_number")
        name = r.get("Name") or r.get("municipality") or r.get("name") or ""
        norm = normalize_name(name)
        # lookup by normalized key in the prebuilt map (handles accents, case,
        # and different separator usage in file names)
        matched = norm_map.get(norm)
        used_match = None
        if matched:
            used_match = matched[0]
            data_links_flag = 1
        else:
            # Fallback strategy: try substring matches of normalized keys
            candidates = [k for k in norm_map.keys() if (norm in k) or (k in norm)]
            if candidates:
                # prefer the candidate with smallest edit distance or shortest extra
                best = sorted(candidates, key=lambda k: (abs(len(k) - len(norm)), len(k)))[0]
                used_match = norm_map[best][0]
                data_links_flag = 1
            else:
                # final fallback: difflib close matches
                close = difflib.get_close_matches(norm, list(norm_map.keys()), n=1, cutoff=0.78)
                if close:
                    used_match = norm_map[close[0]][0]
                    data_links_flag = 1
                else:
                    data_links_flag = 0

        if used_match:
            data_links_path = os.path.join(DATA_LINKS_DIR, used_match)
            # record fuzzy usages for later reporting when not exact match
            if normalize_name(used_match[:-len("_data_links.csv")]) != norm:
                fuzzy_used.append((number, name, norm, used_match))

        entries = log_entries.get(number, [])
        log_attempt = ""
        log_result = ""
        if entries:
            log_attempt, log_result = analyze_log_entries(entries)

        out_rows.append({
            "number": number,
            "municipality": _clean_field(name),
            "data_links": int(data_links_flag),
            "log_attempt": _clean_field(log_attempt),
            "log_result": _clean_field(log_result)
        })

    # write output
    ensure_out_dir(OUT_DIR)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["number", "municipality", "data_links", "log_attempt", "log_result"])
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"Wrote {len(out_rows)} rows to {OUT_CSV}")
    if fuzzy_used:
        print("Note: fuzzy filename matches were used for the following municipalities (number, name, norm, matched_file):")
        for t in fuzzy_used[:50]:
            print(t)


if __name__ == "__main__":
    main()