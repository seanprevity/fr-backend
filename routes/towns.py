from flask import Blueprint, request, jsonify, current_app
import requests
from extensions import Session
from sqlalchemy import text
import os
from dotenv import load_dotenv
from .location import get_town_full_info
import unicodedata
from urllib.parse import unquote
import sys

load_dotenv()

towns_bp = Blueprint("towns", __name__, url_prefix="/api")

@towns_bp.route("/nearest-town")
def nearest_town():
    try:
        lat = float(request.args["lat"])
        lng = float(request.args["lng"])
    except (KeyError, ValueError):
        return jsonify({"error": "Missing or invalid lat/lng"}), 400

    town = find_nearest_town(lat, lng)
    if not town: return jsonify({"error": "No town found"}), 404
    
    dept = get_town_full_info(town["name"], town["department"])
    # Update coordinates of town to match geocoded coordinates 
    official_name, lat, lng = geocode_town(town["name"], dept["department_name"], town["department"])
    town["latitude"] = lat
    town["longitude"] = lng
    if official_name: town["name"] = official_name
    return jsonify(town)

def find_nearest_town(lat, lng, max_dist=0.5):
    max_d2 = max_dist * max_dist
    sql = text("""
        SELECT id, name, department, latitude, longitude
        FROM (
            SELECT id, name, department, latitude, longitude,
                ((latitude - :lat)*(latitude - :lat)
                + (longitude - :lng)*(longitude - :lng)) AS dist2
            FROM towns
        ) AS sub
        WHERE dist2 <= :max_d2
        ORDER BY dist2
        LIMIT 1
    """)
    
    session = Session()
    try:
        row = session.execute(sql, {"lat": lat, "lng": lng, "max_d2": max_d2}).fetchone()
        return dict(row._mapping) if row else None
    finally:
        Session.remove()

def clean_town_name(name):
    if not name:
        return name

    replacements = {
        "": "œ",
        "??": "œ",
        "\u009C": "œ",  # Add unicode escaped variant too
    }

    for bad, good in replacements.items():
        name = name.replace(bad, good)

    name = unicodedata.normalize('NFKC', name)
    return name

def geocode_town(town_name, department_name, dept_code):
    current_app.logger.info(f"Geocode information for {town_name}: code: {dept_code}")
    cleaned_town_name = clean_town_name(town_name)
    address = f"{cleaned_town_name}, {department_name}"
    params = {
      "address": address,
      "components": "country:FR",
      "region": "fr",
      "key": os.getenv("GEOCODING_API_KEY")
    }
    resp = requests.get(..., params=params, timeout=5)
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        current_app.logger.error(f"Geocode failed for {address}: {data.get('status')}")
        return None, None, None

    top = None
    for r in data.get("results", []):
        if any(t in ["locality","postal_town"] for t in r.get("types", [])):
            top = r
            break

    if not top:
        # no match at that granularity → treat as failure
        current_app.logger.error(f"Geocode no locality for {address}")
        return jsonify({"error":"Geocoding failed"}), 500
    loc = top["geometry"]["location"]
    lat, lng = loc["lat"], loc["lng"]
    
    sys.stderr.write(f"[DEBUG] {top}'\n")
    sys.stderr.flush()

    # find a component typed 'locality' or 'postal_town'
    official_name = None
    for comp in top["address_components"]:
        if "locality" in comp["types"] or "postal_town" in comp["types"]:
            official_name = comp["long_name"]
            break
    # fallback to formatted_address’s first token
    if not official_name:
        official_name = top["formatted_address"].split(",")[0]

    # now update your towns table
    session = Session()
    try:
        session.execute(
            text("""
                UPDATE towns
                   SET name      = :official,
                       latitude  = :lat,
                       longitude = :lng
                 WHERE name       = :old_name
                   AND department = :dept
            """),
            { "official": official_name, "lat": lat, "lng": lng, "old_name": town_name, "dept": dept_code }
        )
        session.commit()
        current_app.logger.info(f"Towns table updated for {town_name} → {official_name} @ ({lat},{lng})")
    except Exception as e:
        session.rollback()
    finally:
        Session.remove()

    return official_name, lat, lng

@towns_bp.route("/geocode")
def geocode_searched_town():
    town = unquote(request.args.get("town", "")).strip()
    dept_code = unquote(request.args.get("department_code", "")).strip()
    dept_name = unquote(request.args.get("department_name", "")).strip()
    
    if not (town or dept_code): return jsonify({"error: Missing parameters." }), 404
    official, lat, lng = geocode_town(town, dept_name, dept_code)
    if lat is None or lng is None:
        return jsonify({"error": "Geocoding failed"}), 500
    
    return jsonify({ "name": official, "latitude":  lat, "longitude": lng }), 200

@towns_bp.route("/random", methods=["GET"])
def get_random_town():
    session = Session()
    try:
        result = session.execute(text("""
            SELECT name, department
            FROM towns
            ORDER BY RANDOM()
            LIMIT 1
        """)).fetchone()
        return jsonify({
            "name": result.name,
            "department": result.department
        })
    finally:
        Session.remove()