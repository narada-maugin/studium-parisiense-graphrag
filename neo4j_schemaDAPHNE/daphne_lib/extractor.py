import json
from collections import defaultdict
from pathlib import Path

from .config import CONFIG_DIR, FACTOID_TYPES, ROLES
from .utils import (
    TextCleaner, 
    extract_dates, 
    safe_list, 
    detect_uncertainty, 
    load_classification_list
)

class DaphneGraphExtractor:
    """Lit les enregistrements JSONL et construit les nœuds + arêtes basés sur les factoïdes."""

    def __init__(self, config_dir: Path = CONFIG_DIR):
        # Nœuds — indexés par ID dédupliqué
        self.persons = {}
        self.names = {}
        self.groups = {}
        self.places = {}
        self.zones = {}
        self.sources = {}
        self.factoids = {}
        self.factoid_types = {}
        self.roles = {}
        self.ranks = {}
        self.times = {}
        self.objects = {}
        self.object_types = {}
        self.domains = {}

        # Déduplication Institution
        self._group_canon = {}

        # Classification : fichiers de config externes décident du routage des entités
        self._nations_set = load_classification_list(config_dir / "nations.txt")
        self._disciplines_set = load_classification_list(config_dir / "disciplines.txt")
        self._entity_class = {}   # nom_canonique -> "nation"|"discipline"|"institution"

        self.edges = []
        self._factoid_counter = 0

        # Pré-peupler les nœuds de référence
        for ft_id, ft_desc in FACTOID_TYPES.items():
            self.factoid_types[ft_id] = {
                "factoidtype_id": ft_id,
                "description": ft_desc,
            }
        for r_id, r_desc in ROLES.items():
            self.roles[r_id] = {
                "role_id": r_id,
                "role_description": r_desc,
            }

    # ---------- Générateurs d'ID ----------

    def _new_factoid_id(self) -> str:
        self._factoid_counter += 1
        return f"F_{self._factoid_counter:06d}"

    def _get_time_id(self, start: str | None, end: str | None,
                     start_qualifier: str = "SIMPLE",
                     end_qualifier: str = "SIMPLE") -> str | None:
        """Crée ou récupère un nœud Time. Retourne time_id ou None."""
        s = str(start).strip() if start else ""
        e = str(end).strip() if end else ""
        if not s and not e:
            return None

        sq = start_qualifier if start_qualifier != "SIMPLE" else ""
        eq = end_qualifier if end_qualifier != "SIMPLE" else ""

        if s and e and s != e:
            tag = f"{'_' + sq if sq else ''}_{s}{'_' + eq if eq else ''}_{e}"
            time_key = f"TI{tag}"
            self.times.setdefault(time_key, {
                "time_id": time_key,
                "time_type": "TimeInterval",
                "begin": s,
                "finish": e,
                "begin_qualifier": start_qualifier,
                "end_qualifier": end_qualifier,
                "granularity": "year",
            })
        elif s:
            tag = f"{'_' + sq if sq else ''}_{s}"
            time_key = f"I{tag}"
            self.times.setdefault(time_key, {
                "time_id": time_key,
                "time_type": "Instant",
                "begin": s,
                "finish": "",
                "begin_qualifier": start_qualifier,
                "end_qualifier": "",
                "granularity": "year",
            })
        else:
            tag = f"{'_' + eq if eq else ''}_{e}"
            time_key = f"I{tag}"
            self.times.setdefault(time_key, {
                "time_id": time_key,
                "time_type": "Instant",
                "begin": e,
                "finish": "",
                "begin_qualifier": end_qualifier,
                "end_qualifier": "",
                "granularity": "year",
            })
        return time_key

    def _add_group(self, raw_name: str) -> str | None:
        """Nettoie, déduplique, classifie et dirige vers le bon dictionnaire de nœuds."""
        cname = TextCleaner.clean_institution(raw_name)
        if not cname:
            return None
        low = cname.lower().strip()
        display = self._group_canon.setdefault(low, cname)

        # Déjà classifié ? Retourner.
        if display in self._entity_class:
            return display

        norm = TextCleaner.normalize_classification(cname)

        if norm in self._disciplines_set:
            self._entity_class[display] = "discipline"
            self.domains.setdefault(display, {
                "domain_id": display,
                "name": display,
            })
        elif norm in self._nations_set:
            self._entity_class[display] = "nation"
            self.groups.setdefault(display, {
                "group_id": display,
                "group_descr": "",
                "group_type": "nation",
            })
        else:
            self._entity_class[display] = "institution"
            self.groups.setdefault(display, {
                "group_id": display,
                "group_descr": "",
                "group_type": "institution",
            })
        return display

    def _add_person(self, name: str, genre: str = "", shortdesc: str = "",
                    person_type: str = "PhysicalPerson",
                    status: str = "") -> str:
        """Ajoute ou met à jour un nœud Personne."""
        self.persons.setdefault(name, {
            "person_id": name,
            "shortdesc": shortdesc,
            "genre": genre,
            "person_type": person_type,
            "status": status,
        })
        # Mettre à jour genre/shortdesc/status si info plus riche disponible
        if genre and not self.persons[name]["genre"]:
            self.persons[name]["genre"] = genre
        if shortdesc and not self.persons[name]["shortdesc"]:
            self.persons[name]["shortdesc"] = shortdesc
        if status and not self.persons[name]["status"]:
            self.persons[name]["status"] = status
        return name

    # ---------- Helpers de création de Factoïdes ----------

    def _make_factoid(self, fiche_id: str, ftype: str,
                      description: str = "", original_text: str = "",
                      certainty: str = "") -> str:
        """Crée un nouveau nœud Factoid et le lie à son type et sa source."""
        fid = self._new_factoid_id()
        self.factoids[fid] = {
            "factoid_id": fid,
            "factoidtype": ftype,
            "certainty": certainty,
            "duration": "",
            "notes": "",
            "description": description,
            "original_text": original_text,
            "problem": "",
        }
        # Factoid --HAS_TYPE--> FactoidType
        self.edges.append({
            "type": "HAS_TYPE",
            "from_id": fid, "from_label": "Factoid",
            "to_id": ftype, "to_label": "FactoidType",
        })
        # Source --REFER_TO--> Factoid
        source_id = f"SRC_{fiche_id}"
        self.edges.append({
            "type": "REFER_TO",
            "from_id": source_id, "from_label": "Source",
            "to_id": fid, "to_label": "Factoid",
        })
        return fid

    def _link_participant(self, factoid_id: str, person_name: str,
                          role: str = "SUBJECT", rank: str = ""):
        """Factoid --PARTICIPATE--> Person (avec rôle et rang)."""
        self.edges.append({
            "type": "PARTICIPATE",
            "from_id": factoid_id, "from_label": "Factoid",
            "to_id": person_name, "to_label": "Person",
            "role": role,
            "rank": rank,
        })

    def _link_place(self, factoid_id: str, place_name: str,
                    certainty: str = ""):
        """Factoid --TOOK_PLACE_AT--> Place."""
        self.places.setdefault(place_name, {
            "place_id": place_name,
            "place_description": place_name,
        })
        edge = {
            "type": "TOOK_PLACE_AT",
            "from_id": factoid_id, "from_label": "Factoid",
            "to_id": place_name, "to_label": "Place",
        }
        if certainty:
            edge["certainty"] = certainty
        self.edges.append(edge)

    def _link_group(self, factoid_id: str, group_name: str,
                    certainty: str = ""):
        """Route l'arête selon la classification de l'entité :
        - discipline → Factoid --IN_DOMAIN--> Domain
        - nation / institution → Factoid --AT_GROUP--> GroupP
        """
        cls = self._entity_class.get(group_name, "institution")
        if cls == "discipline":
            edge = {
                "type": "IN_DOMAIN",
                "from_id": factoid_id, "from_label": "Factoid",
                "to_id": group_name, "to_label": "Domain",
            }
        else:
            edge = {
                "type": "AT_GROUP",
                "from_id": factoid_id, "from_label": "Factoid",
                "to_id": group_name, "to_label": "GroupP",
            }
        if certainty:
            edge["certainty"] = certainty
        self.edges.append(edge)

    def _link_time(self, factoid_id: str, start: str | None, end: str | None,
                   start_qualifier: str = "SIMPLE",
                   end_qualifier: str = "SIMPLE"):
        """Factoid --OCCURRED_AT--> Time."""
        time_id = self._get_time_id(start, end, start_qualifier, end_qualifier)
        if time_id:
            self.edges.append({
                "type": "OCCURRED_AT",
                "from_id": factoid_id, "from_label": "Factoid",
                "to_id": time_id, "to_label": "Time",
            })

    def _link_time_from_dates(self, factoid_id: str, dates: list[dict]):
        """Commodité : extrait la première entrée de date et lie avec qualificateurs."""
        if not dates:
            return
        d = dates[0]
        self._link_time(
            factoid_id,
            d.get("start_date"), d.get("end_date"),
            d.get("start_qualifier", "SIMPLE"),
            d.get("end_qualifier", "SIMPLE"),
        )

    # ---------- Extraction par enregistrement (Refactoring) ----------

    def process_record(self, rec: dict):
        fiche_id = rec["_id"]
        source_id = f"SRC_{fiche_id}"

        # 1. Source & Identité
        person_name = self._process_identity(rec, source_id)
        if not person_name:
            return

        # 2. Dates (Activité & Vie)
        self._process_activity_dates(rec, fiche_id, person_name)
        self._process_life_dates(rec, fiche_id, person_name)

        # 3. Origine
        self._process_origin(rec, fiche_id, person_name)

        # 4. Curriculum
        self._process_curriculum(rec, fiche_id, person_name)

        # 5. Carrière Ecclésiastique
        self._process_ecclesiastical_career(rec, fiche_id, person_name)

        # 6. Carrière Professionnelle
        self._process_professional_career(rec, fiche_id, person_name)

        # 7. Relations
        self._process_relationships(rec, fiche_id, person_name)

        # 8. Production Textuelle
        self._process_production(rec, fiche_id, person_name)

        # 9. Bibliographie
        self._process_bibliography(rec, fiche_id)

    def _process_identity(self, rec: dict, source_id: str) -> str | None:
        """Crée Personne, Nom, et Lien vers Source."""
        identity = rec.get("identity", {})
        name_items = identity.get("name", [])
        person_name = TextCleaner.clean(name_items[0].get("value", "")) if name_items else None
        if not person_name:
            return None

        # Nœud Source
        self.sources[source_id] = {
            "source_id": source_id,
            "name": rec.get("title", ""),
            "reference": rec.get("reference", ""),
            "link": rec.get("link", ""),
        }

        gender = ""
        gender_items = identity.get("gender", [])
        if gender_items:
            gender = gender_items[0].get("value", "")

        short_desc = ""
        short_desc_items = identity.get("shortDescription", [])
        if short_desc_items:
            short_desc = short_desc_items[0].get("value", "")

        status_items = identity.get("status", [])
        id_status = ""
        if isinstance(status_items, list) and status_items:
            id_status = status_items[0].get("value", "")
        elif isinstance(status_items, str):
            id_status = status_items

        self._add_person(person_name, genre=gender, shortdesc=short_desc, status=id_status)

        # Person --BELONGS_TO--> Source
        self.edges.append({
            "type": "BELONGS_TO",
            "from_id": person_name, "from_label": "Person",
            "to_id": source_id, "to_label": "Source",
        })

        # Nœuds Name
        main_name_id = person_name
        self.names.setdefault(main_name_id, {"name_id": main_name_id, "completename": main_name_id})
        self.edges.append({
            "type": "MAIN_NAME",
            "from_id": person_name, "from_label": "Person",
            "to_id": main_name_id, "to_label": "Name",
        })

        for nv in identity.get("nameVariant", []):
            v = TextCleaner.clean(nv.get("value", ""))
            if v and v != person_name:
                self.names.setdefault(v, {"name_id": v, "completename": v})
                self.edges.append({
                    "type": "NAMED",
                    "from_id": person_name, "from_label": "Person",
                    "to_id": v, "to_label": "Name",
                })
        return person_name

    def _process_activity_dates(self, rec: dict, fiche_id: str, person_name: str):
        identity = rec.get("identity", {})
        for item in identity.get("datesOfActivity", []):
            meta = item.get("meta", {})
            for dobj in extract_dates(meta):
                ds = dobj.get("start_date")
                de = dobj.get("end_date")
                if ds or de:
                    fid = self._make_factoid(fiche_id, "ACTIVITY_PERIOD",
                                             f"Période d'activité de {person_name}")
                    self._link_participant(fid, person_name, "SUBJECT")
                    self._link_time(fid, ds, de,
                                    dobj.get("start_qualifier", "SIMPLE"),
                                    dobj.get("end_qualifier", "SIMPLE"))

    def _process_life_dates(self, rec: dict, fiche_id: str, person_name: str):
        identity = rec.get("identity", {})
        for item in identity.get("datesOfLife", []):
            meta = item.get("meta", {})
            for dobj in extract_dates(meta):
                ds = dobj.get("start_date")
                de = dobj.get("end_date")
                if ds or de:
                    fid = self._make_factoid(fiche_id, "LIFE_PERIOD",
                                             f"Période de vie de {person_name}")
                    self._link_participant(fid, person_name, "SUBJECT")
                    self._link_time(fid, ds, de,
                                    dobj.get("start_qualifier", "SIMPLE"),
                                    dobj.get("end_qualifier", "SIMPLE"))

    def _process_origin(self, rec: dict, fiche_id: str, person_name: str):
        # birthPlace
        for item in rec.get("origin", {}).get("birthPlace", []):
            meta = item.get("meta", {})
            places = safe_list(meta, "places")
            unc = detect_uncertainty(meta)
            if places:
                fid = self._make_factoid(fiche_id, "BIRTH",
                                         f"Naissance de {person_name}")
                self._link_participant(fid, person_name, "SUBJECT")
                place_cert = "uncertain" if unc["places"] else ""
                for p in places:
                    self._link_place(fid, p, certainty=place_cert)
        # diocese
        for item in rec.get("origin", {}).get("diocese", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            for inst in safe_list(meta, "institutions"):
                gname = self._add_group(inst)
                if gname:
                    self.zones.setdefault(gname, {"zone_id": gname, "zone_description": gname})
                    fid = self._make_factoid(fiche_id, "DIOCESE_ORIGIN",
                                             f"Origine diocésaine de {person_name}: {gname}")
                    self._link_participant(fid, person_name, "SUBJECT")
                    self.edges.append({
                        "type": "TOOK_PLACE_AT_ZONE",
                        "from_id": fid, "from_label": "Factoid",
                        "to_id": gname, "to_label": "Zone",
                    })

    def _process_curriculum(self, rec: dict, fiche_id: str, person_name: str):
        curriculum = rec.get("curriculum", {})
        # university
        for item in curriculum.get("university", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            dates = extract_dates(meta)
            places = safe_list(meta, "places")
            institutions = safe_list(meta, "institutions")
            if places or institutions:
                fid = self._make_factoid(fiche_id, "UNIVERSITY_STUDY",
                                         f"Études de {person_name}")
                self._link_participant(fid, person_name, "STUDENT")
                self._link_time_from_dates(fid, dates)
                place_cert = "uncertain" if unc["places"] else ""
                for p in places:
                    self._link_place(fid, p, certainty=place_cert)
                inst_cert = "uncertain" if unc["institutions"] else ""
                for inst in institutions:
                    gname = self._add_group(inst)
                    if gname:
                        self._link_group(fid, gname, certainty=inst_cert)

        # grades
        for item in curriculum.get("grades", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            grade_val = TextCleaner.clean(item.get("value", "")) or ""
            dates = extract_dates(meta)
            places = safe_list(meta, "places")
            fid = self._make_factoid(fiche_id, "ACADEMIC_GRADE",
                                     f"Grade de {person_name}: {grade_val}")
            self._link_participant(fid, person_name, "STUDENT", rank=grade_val)
            self._link_time_from_dates(fid, dates)
            place_cert = "uncertain" if unc["places"] else ""
            for p in places:
                self._link_place(fid, p, certainty=place_cert)
            if grade_val:
                self.ranks.setdefault(grade_val, {"rank_id": grade_val, "rankname": grade_val})

        # universityCollege
        for item in curriculum.get("universityCollege", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            for inst in safe_list(meta, "institutions"):
                gname = self._add_group(inst)
                if gname:
                    inst_cert = "uncertain" if unc["institutions"] else ""
                    fid = self._make_factoid(fiche_id, "COLLEGE_MEMBERSHIP",
                                             f"{person_name} membre de {gname}")
                    self._link_participant(fid, person_name, "MEMBER")
                    self._link_group(fid, gname, certainty=inst_cert)

    def _process_ecclesiastical_career(self, rec: dict, fiche_id: str, person_name: str):
        career = rec.get("ecclesiasticalCareer", {})
        # secularPosition
        for item in career.get("secularPosition", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            role_val = TextCleaner.clean(item.get("value", "")) or ""
            dates = extract_dates(meta)
            for inst in safe_list(meta, "institutions"):
                gname = self._add_group(inst)
                if gname:
                    inst_cert = "uncertain" if unc["institutions"] else ""
                    fid = self._make_factoid(fiche_id, "SECULAR_POSITION",
                                             f"{person_name}: {role_val} a {gname}")
                    self._link_participant(fid, person_name, "HOLDER")
                    self._link_group(fid, gname, certainty=inst_cert)
                    self._link_time_from_dates(fid, dates)
        # regularOrder
        for item in career.get("regularOrder", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            role_val = TextCleaner.clean(item.get("value", "")) or ""
            dates = extract_dates(meta)
            for inst in safe_list(meta, "institutions"):
                gname = self._add_group(inst)
                if gname:
                    inst_cert = "uncertain" if unc["institutions"] else ""
                    fid = self._make_factoid(fiche_id, "REGULAR_ORDER",
                                             f"{person_name}: {role_val} dans {gname}")
                    self._link_participant(fid, person_name, "MEMBER")
                    self._link_group(fid, gname, certainty=inst_cert)
                    self._link_time_from_dates(fid, dates)

    def _process_professional_career(self, rec: dict, fiche_id: str, person_name: str):
        career = rec.get("professionalCareer", {})
        # universityFunction
        for item in career.get("universityFunction", []):
            meta = item.get("meta", {})
            unc = detect_uncertainty(meta)
            function_val = TextCleaner.clean(item.get("value", "")) or ""
            dates = extract_dates(meta)
            places = safe_list(meta, "places")
            institutions = safe_list(meta, "institutions")
            if places or institutions:
                fid = self._make_factoid(fiche_id, "UNIVERSITY_TEACHING",
                                         f"{person_name}: {function_val}")
                self._link_participant(fid, person_name, "TEACHER")
                self._link_time_from_dates(fid, dates)
                place_cert = "uncertain" if unc["places"] else ""
                for p in places:
                    self._link_place(fid, p, certainty=place_cert)
                inst_cert = "uncertain" if unc["institutions"] else ""
                for inst in institutions:
                    gname = self._add_group(inst)
                    if gname:
                        self._link_group(fid, gname, certainty=inst_cert)

    def _process_relationships(self, rec: dict, fiche_id: str, person_name: str):
        rel = rec.get("relationalInsertion", {})
        # familyNetwork
        for item in rel.get("familyNetwork", []):
            meta = item.get("meta", {})
            relation_type = TextCleaner.clean(item.get("value", "")) or ""
            for raw_name in meta.get("names", []):
                cname = TextCleaner.clean_person_name(raw_name)
                if cname:
                    self._add_person(cname)
                    fid = self._make_factoid(fiche_id, "FAMILY_RELATION",
                                             f"Relation familiale: {person_name} - {cname} ({relation_type})")
                    self._link_participant(fid, person_name, "SUBJECT")
                    self._link_participant(fid, cname, "FAMILY_MEMBER")

        # studentProfessorRelationships
        for item in rel.get("studentProfessorRelationships", []):
            meta = item.get("meta", {})
            for raw_name in meta.get("names", []):
                cname = TextCleaner.clean_person_name(raw_name)
                if cname:
                    self._add_person(cname)
                    fid = self._make_factoid(fiche_id, "STUDENT_TEACHER",
                                             f"Relation maître-élève: {person_name} - {cname}")
                    self._link_participant(fid, person_name, "STUDENT")
                    self._link_participant(fid, cname, "TEACHER")

    def _process_production(self, rec: dict, fiche_id: str, person_name: str):
        tp = rec.get("textualProduction", {})
        if not isinstance(tp, dict):
            return
        for domain_name, domain_data in tp.items():
            if not isinstance(domain_data, dict):
                continue
            if domain_name:
                self.domains.setdefault(domain_name, {"domain_id": domain_name, "name": domain_name})
            for opus in domain_data.get("opus", []):
                main_title = TextCleaner.clean(opus.get("mainTitle", ""))
                if main_title:
                    self.objects.setdefault(main_title, {
                        "object_id": main_title,
                        "object_description": main_title,
                        "value": main_title,
                    })
                    self.object_types.setdefault("literary_work", {
                        "type_id": "literary_work",
                        "type_description": "Œuvre littéraire / intellectuelle",
                    })
                    fid = self._make_factoid(fiche_id, "AUTHORSHIP",
                                             f"{person_name} auteur de '{main_title}'")
                    self._link_participant(fid, person_name, "AUTHOR")
                    self.edges.append({
                        "type": "OF_TYPE",
                        "from_id": fid, "from_label": "Factoid",
                        "to_id": main_title, "to_label": "Object",
                    })

    def _process_bibliography(self, rec: dict, fiche_id: str):
        bib = rec.get("bibliography", {})
        for bib_section in ["workReferences", "bookReferences"]:
            for item in bib.get(bib_section, []):
                citation = TextCleaner.clean(item.get("value", ""))
                if citation:
                    bib_src_id = f"BIB_{hash(citation) % 10**8:08d}"
                    self.sources.setdefault(bib_src_id, {
                        "source_id": bib_src_id,
                        "name": citation,
                        "reference": "",
                        "link": "",
                    })
                    self.edges.append({
                        "type": "LINKED_TO",
                        "from_id": f"SRC_{fiche_id}", "from_label": "Source",
                        "to_id": bib_src_id, "to_label": "Source",
                        "link_type": bib_section,
                    })

