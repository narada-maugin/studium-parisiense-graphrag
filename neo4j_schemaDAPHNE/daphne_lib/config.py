import os
import sys
from pathlib import Path

# Essayer de charger le fichier .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Chemins
# Se base sur l'emplacement du fichier config.py dans neo4j_schemaDAPHNE/daphne_lib/
LIB_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = LIB_DIR.parent      # neo4j_schemaDAPHNE
PROJECT_DIR = PACKAGE_DIR.parent  # studium-parisiense-graphrag

JSONL_PATH = PROJECT_DIR / "studium_parisiense_dataset.jsonl"
OUTPUT_DIR = PACKAGE_DIR / "import_csv" # Les CSV vont dans neo4j_schemaDAPHNE/import_csv/
CONFIG_DIR = PROJECT_DIR / "config"

# Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not NEO4J_PASSWORD:
    print("ATTENTION : NEO4J_PASSWORD non défini dans l'environnement.", file=sys.stderr)

STOP_WORDS = {"INCONNU", "NON SPÉCIFIÉ", "NON SPECIFIE", "?", ""}

FACTOID_TYPES = {
    "BIRTH":                  "Naissance / lieu d'origine",
    "DIOCESE_ORIGIN":         "Origine diocésaine",
    "UNIVERSITY_STUDY":       "Études universitaires",
    "ACADEMIC_GRADE":         "Grade académique",
    "COLLEGE_MEMBERSHIP":     "Appartenance à un collège",
    "SECULAR_POSITION":       "Position ecclésiastique séculière",
    "REGULAR_ORDER":          "Appartenance à un ordre régulier",
    "UNIVERSITY_TEACHING":    "Enseignement universitaire",
    "FAMILY_RELATION":        "Relation familiale",
    "STUDENT_TEACHER":        "Relation maître-élève",
    "AUTHORSHIP":             "Production textuelle",
    "ACTIVITY_PERIOD":        "Période d'activité",
    "LIFE_PERIOD":            "Période de vie",
}

ROLES = {
    "SUBJECT":        "Sujet principal du factoïde",
    "STUDENT":        "Étudiant",
    "TEACHER":        "Enseignant / Maître",
    "MEMBER":         "Membre",
    "AUTHOR":         "Auteur",
    "FAMILY_MEMBER":  "Membre de la famille",
    "RELATED_PERSON": "Personne liée",
    "HOLDER":         "Titulaire d'une position",
}
