import sys
import os
import csv
from pathlib import Path

BATCH_SIZE = 1000

def load_neo4j(uri, user, password, output_dir: Path):
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("ERREUR : package neo4j non installé. Exécutez : pip install neo4j")
        sys.exit(1)

    print(f"\n=== Modèle Factoïde DAPHNE — Phase 2 : Chargement Neo4j ===")
    print(f"Connexion à {uri} ...")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        driver.verify_connectivity()
        print("Connecté !")
    except Exception as e:
        print(f"ERREUR : Impossible de se connecter à Neo4j : {e}")
        sys.exit(1)

    with driver.session() as session:
        print("\n1. Nettoyage de la base de données ...")
        session.run("MATCH (n) DETACH DELETE n")

        print("\n2. Création des contraintes ...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Name) REQUIRE n.name_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:GroupP) REQUIRE g.group_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (pl:Place) REQUIRE pl.place_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (z:Zone) REQUIRE z.zone_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Source) REQUIRE s.source_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Factoid) REQUIRE f.factoid_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (ft:FactoidType) REQUIRE ft.factoidtype_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Role) REQUIRE r.role_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (rk:Rank) REQUIRE rk.rank_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Time) REQUIRE t.time_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Object) REQUIRE o.object_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (ot:ObjectType) REQUIRE ot.type_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Domain) REQUIRE d.domain_id IS UNIQUE",
        ]
        for c in constraints:
            session.run(c)

        print("\n3. Chargement des nœuds ...")
        _load_node_csv(session, output_dir, "nodes_person.csv", "Person", "person_id", ["shortdesc", "genre", "person_type", "status"])
        _load_node_csv(session, output_dir, "nodes_name.csv", "Name", "name_id", ["completename"])
        _load_node_csv(session, output_dir, "nodes_groupp.csv", "GroupP", "group_id", ["group_descr", "group_type"])
        _load_node_csv(session, output_dir, "nodes_place.csv", "Place", "place_id", ["place_description"])
        _load_node_csv(session, output_dir, "nodes_zone.csv", "Zone", "zone_id", ["zone_description"])
        _load_node_csv(session, output_dir, "nodes_source.csv", "Source", "source_id", ["name", "reference", "link"])
        _load_node_csv(session, output_dir, "nodes_factoid.csv", "Factoid", "factoid_id", ["factoidtype", "certainty", "duration", "notes", "description", "original_text", "problem"])
        _load_node_csv(session, output_dir, "nodes_factoidtype.csv", "FactoidType", "factoidtype_id", ["description"])
        _load_node_csv(session, output_dir, "nodes_role.csv", "Role", "role_id", ["role_description"])
        _load_node_csv(session, output_dir, "nodes_rank.csv", "Rank", "rank_id", ["rankname"])
        _load_node_csv(session, output_dir, "nodes_time.csv", "Time", "time_id", ["time_type", "begin", "finish", "begin_qualifier", "end_qualifier", "granularity"])
        _load_node_csv(session, output_dir, "nodes_object.csv", "Object", "object_id", ["object_description", "value"])
        _load_node_csv(session, output_dir, "nodes_objecttype.csv", "ObjectType", "type_id", ["type_description"])
        _load_node_csv(session, output_dir, "nodes_domain.csv", "Domain", "domain_id", ["name"])

        print("\n4. Chargement des arêtes ...")
        edge_configs = {
            "MAIN_NAME":         ("Person", "person_id", "Name", "name_id"),
            "NAMED":             ("Person", "person_id", "Name", "name_id"),
            "BELONGS_TO":        ("Person", "person_id", "Source", "source_id"),
            "HAS_TYPE":          ("Factoid", "factoid_id", "FactoidType", "factoidtype_id"),
            "REFER_TO":          ("Source", "source_id", "Factoid", "factoid_id"),
            "PARTICIPATE":       ("Factoid", "factoid_id", "Person", "person_id"),
            "TOOK_PLACE_AT":     ("Factoid", "factoid_id", "Place", "place_id"),
            "TOOK_PLACE_AT_ZONE":("Factoid", "factoid_id", "Zone", "zone_id"),
            "AT_GROUP":          ("Factoid", "factoid_id", "GroupP", "group_id"),
            "IN_DOMAIN":         ("Factoid", "factoid_id", "Domain", "domain_id"),
            "OCCURRED_AT":       ("Factoid", "factoid_id", "Time", "time_id"),
            "OF_TYPE":           ("Factoid", "factoid_id", "Object", "object_id"),
            "LINKED_TO":         ("Source", "source_id", "Source", "source_id"),
        }
        for rel_type, (from_label, from_prop, to_label, to_prop) in edge_configs.items():
            fname = f"edges_{rel_type.lower()}.csv"
            fpath = output_dir / fname
            if not fpath.exists():
                continue
            _load_edge_csv(session, output_dir, fname, rel_type, from_label, from_prop, to_label, to_prop)

        print("\n5. Résumé du graphe :")
        result = session.run("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC")
        for rec in result:
            print(f"   {rec['label']} : {rec['cnt']} nœuds")
        result = session.run("MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS cnt ORDER BY cnt DESC")
        for rec in result:
            print(f"   {rec['rel']} : {rec['cnt']} arêtes")

    driver.close()
    print("\n=== Chargement Neo4j terminé ! ===")


def _load_node_csv(session, output_dir, filename, label, key_prop, other_props):
    fpath = output_dir / filename
    if not fpath.exists():
        print(f"   PASSÉ {filename} (non trouvé)")
        return

    rows = []
    with open(fpath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    set_parts = ", ".join([f"n.{p} = row.{p}" for p in other_props])
    set_clause = f"SET {set_parts}" if set_parts else ""
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{{key_prop}: row.{key_prop}}})
    {set_clause}
    """
    
    count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        session.run(query, rows=batch)
        count += len(batch)
    print(f"   {label} : {count} nœuds chargés")


def _load_edge_csv(session, output_dir, filename, rel_type, from_label, from_prop, to_label, to_prop):
    fpath = output_dir / filename
    rows = []
    with open(fpath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        return

    skip_cols = {"type", "from_label", "from_key", "to_label", "to_key", "from_id", "to_id"}
    sample = rows[0]
    extra_props = [k for k in sample.keys() if k not in skip_cols]
    set_parts = ", ".join([f"r.{p} = row.{p}" for p in extra_props])
    set_clause = f"SET {set_parts}" if set_parts else ""

    query = f"""
    UNWIND $rows AS row
    MATCH (a:{from_label} {{{from_prop}: row.from_id}})
    MATCH (b:{to_label} {{{to_prop}: row.to_id}})
    MERGE (a)-[r:{rel_type}]->(b)
    {set_clause}
    """

    count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        session.run(query, rows=batch)
        count += len(batch)
    print(f"   {rel_type} : {count} arêtes chargées")
