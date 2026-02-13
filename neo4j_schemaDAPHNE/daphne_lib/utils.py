import re
import unicodedata
from pathlib import Path
from .config import STOP_WORDS

class TextCleaner:
    @staticmethod
    def clean(text: str | None) -> str | None:
        if text is None:
            return None
        t = text.strip().replace('$', '').replace('£', '').replace('*', '')
        t = t.replace('?', '')              # supprimer marqueur d'incertitude
        t = re.sub(r'\(\s*\)', '', t)       # parenthèses vides laissées par la suppression de ?
        t = re.sub(r'\s{2,}', ' ', t)
        t = t.rstrip(";").rstrip(",").rstrip(".").strip()
        if t.upper() in STOP_WORDS or not t:
            return None
        return t

    @staticmethod
    def clean_institution(name: str) -> str | None:
        if not name:
            return None
        n = name.strip().replace('$', '').replace('£', '').replace('*', '')
        n = n.replace('?', '')              # supprimer marqueur d'incertitude
        n = re.sub(r'%[^%]*%', '', n)
        n = re.sub(r'%[\d\-\.\s:/c]{2,}', '', n)
        keywords = r"(Nation|Faculté|Université|Collège|Studium)"
        n = re.sub(r'(?<=[a-zA-ZÀ-ÿ])' + keywords, r' \1', n)
        n = re.sub(r'[%$£*]', '', n)
        n = re.sub(r'\(\s*\)', '', n)       # parenthèses vides laissées par suppression de ?
        while n and n[-1] in ').,:;':
            n = n[:-1].strip()
        n = re.sub(r'\s+', ' ', n)
        n = n.replace('=', ' ').strip()
        if not n or n.upper() in STOP_WORDS:
            return None
        return n

    @staticmethod
    def clean_person_name(raw: str) -> str | None:
        name = raw.replace('$', '').replace('£', '').replace('=', ' ').strip()
        if ',' in name:
            name = name.split(',')[0].strip()
        name = name.strip().rstrip('.').rstrip(';').strip()
        if not name or name.upper() in STOP_WORDS:
            return None
        return name

    @staticmethod
    def strip_uncertainty(text: str | None) -> tuple[str | None, bool]:
        """Détecte '?' n'importe où dans le texte, le supprime et retourne (nettoyé, est_incertain)."""
        if text is None:
            return None, False
        t = text.strip()
        if not t:
            return None, False
        if "?" not in t:
            return t, False
        # ? trouvé -> incertain
        t = t.replace("?", "")
        # Nettoyer les artefacts laissés par la suppression
        t = re.sub(r"\(\s*\)", "", t)       # parenthèses vides "(  )"
        t = t.replace(" )", ")").replace("( ", "(")  # espaces orphelins près des parenthèses
        t = re.sub(r"\s{2,}", " ", t)       # réduire espaces multiples
        t = t.strip()
        # Supprimer ponctuation traînante
        while t and t[-1] in ".,;:":
            t = t[:-1].strip()
        if not t:
            return None, True
        return t, True

    @staticmethod
    def normalize_classification(text: str) -> str:
        """Minuscules + suppression accents pour correspondance floue."""
        t = text.lower().strip()
        return unicodedata.normalize("NFD", t).encode("ascii", "ignore").decode("ascii")


def extract_dates(meta: dict) -> list[dict]:
    """Extrait les dates avec information de qualificateur (BEFORE/AFTER/NEAR/SIMPLE)."""
    results = []
    for d in meta.get("dates", []):
        entry = {"type": d.get("type", "UNKNOWN"),
                 "start_qualifier": "SIMPLE", "end_qualifier": "SIMPLE"}
        sd = d.get("startDate", {})
        ed = d.get("endDate", {})
        top_type = d.get("type", "SIMPLE")

        if sd:
            entry["start_date"] = sd.get("date")
            entry["start_qualifier"] = sd.get("type", "SIMPLE")
        elif d.get("date") is not None:
            entry["start_date"] = d.get("date")
            # Non-INTERVALLE : le type de niveau supérieur EST le qualificateur
            if top_type in ("BEFORE", "AFTER", "NEAR"):
                entry["start_qualifier"] = top_type
        if ed:
            entry["end_date"] = ed.get("date")
            entry["end_qualifier"] = ed.get("type", "SIMPLE")

        if "start_date" not in entry and "end_date" not in entry and d.get("date") is not None:
            entry["start_date"] = d["date"]
            if top_type in ("BEFORE", "AFTER", "NEAR"):
                entry["start_qualifier"] = top_type

        results.append(entry)
    return results


def safe_list(meta: dict, key: str) -> list[str]:
    vals = meta.get(key, [])
    if not isinstance(vals, list):
        return []
    # Utiliser TextCleaner
    return [TextCleaner.clean(v) for v in vals if isinstance(v, str) and TextCleaner.clean(v)]


def detect_uncertainty(meta: dict) -> dict[str, bool]:
    """Vérifie quels champs meta contiennent '?'. Retourne des drapeaux d'incertitude par champ."""
    result = {}
    for key in ("places", "institutions"):
        result[key] = any(
            isinstance(v, str) and "?" in v for v in meta.get(key, []))
    return result

def load_classification_list(filepath: Path) -> set[str]:
    """Charge un fichier de classification (une entrée par ligne, # commentaires)."""
    entries: set[str] = set()
    if not filepath.exists():
        return entries
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            entries.add(TextCleaner.normalize_classification(line))
    return entries
