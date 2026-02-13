# Studium Parisiense — Graphe de Connaissances

Ce projet de recherche est mené au sein du **LIP6 (Sorbonne Université)** dans l'équipe Bases de Données. Il explore l'utilisation des modèles de langage (LLM) pour automatiser la création de graphes de connaissances à partir de corpus historiques complexes.

## Contexte

Un Graphe de Connaissances (GC) organise l'information en représentant des entités, leurs attributs et leurs relations sous une forme interconnectée. Ce projet s'appuie sur les LLM pour transformer du texte brut en représentations sémantiques exploitables.

## Objectifs

Le projet porte sur les données de **Studium Parisiense** (XIIe–XVIe siècle), regroupant environ 15 000 fiches prosopographiques sur les membres de l'Université de Paris.

* **Extraction** : identifier les entités et relations via des techniques de prompt engineering (few-shot, chain-of-thought).
* **Structuration** : comparer une approche guidée par ontologie et une approche sans schéma.
* **Consolidation** : fusionner les instances, gérer les ambiguïtés.
* **Exploitation** : déploiement dans Neo4j.

## Structure du projet

```
neo4j_schemaDAPHNE/          Pipeline de mapping DAPHNE (sans LLM)
  daphne_direct_mapping.py     Script principal
  daphne_lib/                  Modules (extraction, export CSV, chargement Neo4j)
  daphne_ontology_schema.json  Schéma complet de l'ontologie DAPHNE
  NOTES_ameliorations.txt      Bilan de ce qui est fait / reste à faire

uncertainty_benchmark/        Benchmark pour l'extraction d'incertitude
  uncertainty_examples.txt     59 exemples annotés (50 incertains, 9 certains)
  uncertainty_benchmark.jsonl  Gold standard (JSON structuré)
  uncertainty_prompt.txt       Prompt pour tester des LLMs

config/                       Fichiers de classification (nations, disciplines)
studium_parisiense_dataset.jsonl   Données source (2930 fiches)
```

## Stack technique

* **Langage** : Python
* **IA / NLP** : OpenAI API, HuggingFace, prompt engineering
* **Graphes** : Neo4j, Cypher
* **Modélisation** : Ontologie DAPHNE (factoïdes prosopographiques)

## Encadrement (LIP6)

* **Camelia CONSTANTIN** (Équipe BD)
* **Raphaël FOURNIER-S'NIEHOTTA** (Équipe ComplexNetworks)

---
*Projet réalisé dans le cadre d'un stage à Sorbonne Université.*
