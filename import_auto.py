"""
ENTRACTE — Import automatique quotidien
Tourne chaque nuit via GitHub Actions
Récupère les nouvelles pièces depuis OpenAgenda et Paris
"""
import requests, re, unicodedata, os, time

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

cache_pieces = set()
cache_theatres = {}
cache_reps = set()

def norm(t):
    if not t: return ""
    s = unicodedata.normalize('NFKD', str(t))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^\w\s]', ' ', s.lower())
    return re.sub(r'\s+', ' ', s).strip()

MOTS_TH = ["theatre","theatr","comedie","piece de theatre","mise en scene",
           "dramaturgie","dramatique","tragedie","spectacle vivant",
           "one man show","one woman show","seul en scene","stand up","stand-up"]
MOTS_EXCLUS = ["concert","jazz","rock","musique classique","biblioth",
               "cinema","exposition","musee","lecture","conference",
               "atelier","visite","balade","danse","ballet","opera",
               "cirque","chorale","gospel","choeur","orchestre","recital","projection"]

def est_theatre(texte):
    t = norm(texte)
    if any(m in t for m in MOTS_EXCLUS): return False
    return any(m in t for m in MOTS_TH)

def genre_auto(texte):
    t = norm(texte)
    if "musical" in t or "comedie musicale" in t: return "Comédie musicale"
    if "stand up" in t or "one man" in t or "humour" in t: return "Comédie"
    if "comedie" in t: return "Comédie"
    if "drame" in t or "tragedie" in t: return "Drame"
    if "classique" in t or "moliere" in t or "shakespeare" in t: return "Classique"
    if "jeune" in t or "enfant" in t or "famille" in t: return "Jeune public"
    return "Contemporain"

def sb_get(table, filtre):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{filtre}&select=id", headers=HEADERS, timeout=10)
    return r.json() if r.status_code == 200 else []

def sb_insert(table, data):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=data, timeout=10)
    if r.status_code in (200, 201):
        res = r.json()
        return res[0] if isinstance(res, list) and res else res
    return None

def charger_cache():
    print("Chargement cache...")
    r = requests.get(f"{SUPABASE_URL}/rest/v1/pieces?select=titre&limit=10000", headers=HEADERS, timeout=20)
    if r.status_code == 200:
        for p in r.json(): cache_pieces.add(norm(p["titre"]))
    r2 = requests.get(f"{SUPABASE_URL}/rest/v1/theatres?select=id,nom&limit=10000", headers=HEADERS, timeout=20)
    if r2.status_code == 200:
        for t in r2.json(): cache_theatres[norm(t["nom"])] = t["id"]
    print(f"  {len(cache_pieces)} pièces, {len(cache_theatres)} théâtres en cache")

def get_theatre(nom, ville="France", lat=None, lng=None, adresse=None, site=None):
    cle = norm(nom)
    if cle in cache_theatres: return cache_theatres[cle]
    n = sb_insert("theatres", {"nom": nom[:200], "ville": ville,
                                "adresse": adresse, "site_web": site,
                                "latitude": lat, "longitude": lng})
    if n: cache_theatres[cle] = n["id"]; return n["id"]
    return None

def get_piece(titre, genre, desc=None, image=None, auteur=None):
    cle = norm(titre)
    if cle in cache_pieces:
        res = sb_get("pieces", f"titre=eq.{requests.utils.quote(titre[:200])}")
        return res[0]["id"] if res else None
    cache_pieces.add(cle)
    n = sb_insert("pieces", {"titre": titre[:200], "genre": genre,
                              "description": (desc or "")[:500] or None,
                              "affiche_url": image or None,
                              "auteur": (auteur or "")[:200] or None})
    return n["id"] if n else None

def creer_rep(pid, tid, date_d=None, date_f=None, prix=None, url=None):
    cle = (pid, tid)
    if cle in cache_reps: return
    cache_reps.add(cle)
    if sb_get("representations", f"piece_id=eq.{pid}&theatre_id=eq.{tid}"): return
    sb_insert("representations", {"piece_id": pid, "theatre_id": tid,
                                   "date_debut": date_d, "date_fin": date_f,
                                   "prix_min": prix, "lien_reservation": url})

def paginer(url, params={}, max_offset=5000):
    offset = 0
    while offset < max_offset:
        try:
            r = requests.get(url, params={**params, "limit": 100, "offset": offset}, timeout=20)
            if r.status_code != 200: break
            data = r.json()
            records = data.get("results", [])
            if not records: break
            yield records
            if len(records) < 100: break
            time.sleep(0.2)
        except Exception as e:
            print(f"Erreur: {e}"); break
        offset += 100

def source_paris():
    print("\n=== Que Faire à Paris ===")
    URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
    p = 0
    for records in paginer(URL, {}, max_offset=3000):
        for rec in records:
            try:
                titre = (rec.get("title") or "").strip()
                if not titre: continue
                texte = f"{titre} {rec.get('lead_text','')} {rec.get('tags','')}"
                if not est_theatre(texte): continue
                nom = (rec.get("address_name") or "").strip()
                if not nom or len(nom) < 3: continue
                ville = (rec.get("address_city") or "Paris").strip()
                geo = rec.get("lat_lon") or {}
                lat = geo.get("lat") if isinstance(geo, dict) else None
                lng = geo.get("lon") if isinstance(geo, dict) else None
                try: lat = float(lat) if lat else None
                except: lat = None
                try: lng = float(lng) if lng else None
                except: lng = None
                th_id = get_theatre(nom, ville, lat=lat, lng=lng)
                if not th_id: continue
                genre = genre_auto(texte)
                desc = (rec.get("lead_text") or "")[:500]
                image = rec.get("cover_url") or None
                p_id = get_piece(titre, genre, desc=desc, image=image)
                if not p_id: continue
                date_d = (rec.get("date_start") or "")[:10] or None
                date_f = (rec.get("date_end") or "")[:10] or None
                nbs = re.findall(r'\d+', str(rec.get("price_detail") or ""))
                prix = float(nbs[0]) if nbs else None
                creer_rep(p_id, th_id, date_d, date_f, prix, rec.get("url"))
                p += 1
                print(f"  + {titre[:50]}")
            except: pass
    print(f"Paris : {p} nouvelles pièces")

def source_openagenda():
    print("\n=== OpenAgenda France ===")
    URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/evenements-publics-openagenda/records"
    p = 0
    for records in paginer(URL, {}, max_offset=5000):
        for rec in records:
            try:
                texte = " ".join(str(v or "") for v in rec.values())
                if not est_theatre(texte): continue
                nom = (rec.get("location_name") or "").strip()
                if not nom or len(nom) < 3: continue
                ville = (rec.get("location_city") or "France").strip()
                lat = rec.get("location_latitude")
                lng = rec.get("location_longitude")
                try: lat = float(lat) if lat else None
                except: lat = None
                try: lng = float(lng) if lng else None
                except: lng = None
                th_id = get_theatre(nom, ville, lat=lat, lng=lng)
                if not th_id: continue
                titre = (rec.get("title_fr") or rec.get("title_en") or "").strip()
                if not titre: continue
                genre = genre_auto(texte)
                desc = (rec.get("description_fr") or "")[:500]
                image = rec.get("image") or None
                auteur = rec.get("contributor") or None
                p_id = get_piece(titre, genre, desc=desc, image=image, auteur=auteur)
                if not p_id: continue
                date_d = (rec.get("firstdate_begin") or "")[:10] or None
                date_f = (rec.get("lastdate_end") or "")[:10] or None
                creer_rep(p_id, th_id, date_d, date_f)
                p += 1
                print(f"  + {titre[:50]}")
            except: pass
    print(f"OpenAgenda : {p} nouvelles pièces")

# Lancement
charger_cache()
source_paris()
source_openagenda()
print(f"\n✅ Import terminé — {len(cache_pieces)} pièces, {len(cache_theatres)} théâtres")
