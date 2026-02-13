#!/usr/bin/env python3
"""
Studium Parisiense — Schéma DAPHNE : mapping direct (baseline sans LLM)
========================================================================
Transforme le dataset JSONL structuré en un graphe de connaissances **basé sur les factoïdes**
suivant l'ontologie prosopographique DAPHNE.

Architecture modulaire :
- daphne_lib/config.py : Configuration
- daphne_lib/extractor.py : Logique d'extraction
- daphne_lib/writer.py : Export CSV
- daphne_lib/loader.py : Chargement Neo4j

Utilisation :
  python neo4j_schemaDAPHNE/daphne_direct_mapping.py
  python neo4j_schemaDAPHNE/daphne_direct_mapping.py --export-only
  python neo4j_schemaDAPHNE/daphne_direct_mapping.py --load-only
"""

import argparse
import sys
import json
from pathlib import Path
from collections import Counter

# Ajouter le dossier courant au path pour permettre l'import de daphne_lib
# si le script est lancé depuis la racine du projet ou depuis le dossier
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

try:
    from daphne_lib import config
    from daphne_lib.extractor import DaphneGraphExtractor
    from daphne_lib.writer import write_csvs
    from daphne_lib.loader import load_neo4j
except ImportError as e:
    print(f"Erreur d'import : {e}")
    sys.exit(1)

def export_csvs(output_dir: Path):
    print("=== Modèle Factoïde DAPHNE — Phase 1 : Export CSV ===")
    print(f"Lecture de {config.JSONL_PATH} ...")

    if not config.JSONL_PATH.exists():
        print(f"ERREUR : Fichier non trouvé : {config.JSONL_PATH}", file=sys.stderr)
        return

    extractor = DaphneGraphExtractor(config_dir=config.CONFIG_DIR)
    record_count = 0

    with open(config.JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            extractor.process_record(rec)
            record_count += 1
            if record_count % 5000 == 0:
                print(f"  ... {record_count} fiches traitées")

    print(f"\nTotal : {record_count} fiches traitées.")

    # Statistiques
    cls_counts = Counter(extractor._entity_class.values())
    n_nations = cls_counts.get("nation", 0)
    n_disciplines = cls_counts.get("discipline", 0)
    n_institutions = cls_counts.get("institution", 0)
    print(f"\nClassification des entités :")
    print(f"  Nations      -> GroupP(nation) :      {n_nations}")
    print(f"  Disciplines  -> Domain :              {n_disciplines}")
    print(f"  Institutions -> GroupP(institution) : {n_institutions}")
    print(f"  Total classifiées : {n_nations + n_disciplines + n_institutions}")

    print(f"\nComptage des Nœuds :")
    print(f"  Person :     {len(extractor.persons)}")
    print(f"  Factoid :    {len(extractor.factoids)}")
    print(f"  Total arêtes : {len(extractor.edges)}")

    edges_by_type = write_csvs(extractor, output_dir)
    
    print(f"\nDétail des arêtes :")
    for rel_type, rel_edges in sorted(edges_by_type.items()):
        print(f"  {rel_type} : {len(rel_edges)}")

    print(f"\n=== Export CSV terminé ! ===")

def main():
    parser = argparse.ArgumentParser(description="Studium Parisiense — Modèle Factoïde DAPHNE (Modulaire)")
    parser.add_argument("--export-only", action="store_true", help="Seulement exporter les CSV")
    parser.add_argument("--load-only", action="store_true", help="Seulement charger dans Neo4j")
    parser.add_argument("--output-dir", default=None, help="Dossier pour les fichiers CSV")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else config.OUTPUT_DIR

    if args.load_only:
        load_neo4j(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD, output_dir)
    elif args.export_only:
        export_csvs(output_dir)
    else:
        export_csvs(output_dir)
        load_neo4j(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD, output_dir)

if __name__ == "__main__":
    main()
