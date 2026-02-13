import csv
from pathlib import Path
from collections import defaultdict

def write_csvs(extractor, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nÉcriture des CSV dans {output_dir}/ ...")

    _write_csv(output_dir, "nodes_person.csv",
                    ["person_id", "shortdesc", "genre", "person_type", "status"],
                    extractor.persons.values())
    _write_csv(output_dir, "nodes_name.csv", ["name_id", "completename"], extractor.names.values())
    _write_csv(output_dir, "nodes_groupp.csv", ["group_id", "group_descr", "group_type"], extractor.groups.values())
    _write_csv(output_dir, "nodes_place.csv", ["place_id", "place_description"], extractor.places.values())
    _write_csv(output_dir, "nodes_zone.csv", ["zone_id", "zone_description"], extractor.zones.values())
    _write_csv(output_dir, "nodes_source.csv", ["source_id", "name", "reference", "link"], extractor.sources.values())
    _write_csv(output_dir, "nodes_factoid.csv", ["factoid_id", "factoidtype", "certainty", "duration", "notes", "description", "original_text", "problem"], extractor.factoids.values())
    _write_csv(output_dir, "nodes_factoidtype.csv", ["factoidtype_id", "description"], extractor.factoid_types.values())
    _write_csv(output_dir, "nodes_role.csv", ["role_id", "role_description"], extractor.roles.values())
    _write_csv(output_dir, "nodes_rank.csv", ["rank_id", "rankname"], extractor.ranks.values())
    _write_csv(output_dir, "nodes_time.csv", ["time_id", "time_type", "begin", "finish", "begin_qualifier", "end_qualifier", "granularity"], extractor.times.values())
    _write_csv(output_dir, "nodes_object.csv", ["object_id", "object_description", "value"], extractor.objects.values())
    _write_csv(output_dir, "nodes_objecttype.csv", ["type_id", "type_description"], extractor.object_types.values())
    _write_csv(output_dir, "nodes_domain.csv", ["domain_id", "name"], extractor.domains.values())

    edges_by_type = defaultdict(list)
    for e in extractor.edges:
        edges_by_type[e["type"]].append(e)

    for rel_type, rel_edges in sorted(edges_by_type.items()):
        all_keys = set()
        for e in rel_edges:
            all_keys.update(e.keys())
        cols = sorted(all_keys)
        fname = f"edges_{rel_type.lower()}.csv"
        _write_csv(output_dir, fname, cols, rel_edges)

    return edges_by_type

def _write_csv(output_dir: Path, filename: str, fieldnames: list, rows):
    path = output_dir / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        count = 0
        for row in rows:
            writer.writerow(row)
            count += 1
    print(f"  {filename} : {count} lignes")
    return count
