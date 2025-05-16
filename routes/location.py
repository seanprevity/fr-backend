from flask import Blueprint, request, jsonify, current_app
from services.openai_service import get_description
from extensions import Session
from .images import fetch_wiki_images
from sqlalchemy import text
from urllib.parse import unquote
import unicodedata
import logging
logger = logging.getLogger("france_app")

location_bp = Blueprint("location", __name__, url_prefix="/api")

def normalize_string(s):
    if not s: return s
    return unicodedata.normalize('NFKC', s)

@location_bp.route("/location")
def location_info():
    name = request.args.get("name")
    lang = request.args.get("lang", "en")
    dept_code = unquote(request.args.get("code", ""))

    logger.debug(f"Incoming /location request → name={name}, code={dept_code}, lang={lang}")

    if not name:
        logger.warning("Missing 'name' parameter")
        return jsonify({"error": "Missing 'name' parameter"}), 400

    name_normalized = normalize_string(name)
    logger.debug(f"Normalized name → {name_normalized}")

    town = get_town_full_info(name_normalized, dept_code)

    if not town:
        logger.warning(f"Town not found in DB → name={name_normalized}, code={dept_code}")
        return jsonify({"error": "Town not found"}), 404

    logger.debug(f"Town found → {town}")

    metadata = {
        **town,
        "department_name": town["department_name"],
        "department_code": dept_code,
        "region_code": town["region_code"],
        "region_name": town["region_name"],
    }

    logger.debug(f"Metadata → {metadata}")

    images_response = fetch_wiki_images(town_name=town["name"], department_name=town["department_name"])
    images_list = images_response.get("images", [])
    logger.debug(f"Wiki images → {images_list[:2]}...")

    cached = get_cached_description(town["code"], dept_code, lang)
    if cached:
        logger.debug("Returning cached description")
        return jsonify({"description": cached, "metadata": metadata, "images": images_list})

    description = get_description(name_normalized, town["department_name"], town["region_name"], lang)
    cache_description(town["code"], dept_code, lang, description)
    logger.debug("Description generated and cached")

    return jsonify({"description": description, "metadata": metadata, "images": images_list})


def get_town_full_info(town_name, dept_code):
    query = text("""
        SELECT 
            t.*, 
            d.name AS department_name, 
            r.code AS region_code, 
            r.name AS region_name
        FROM towns t
        JOIN departments d ON t.department = d.code
        JOIN regions r ON d.region = r.code
        WHERE LOWER(t.name) = LOWER(:name)
          AND t.department = :code
        LIMIT 1
    """)

    session = Session()
    try:
        logger.debug(f"Running town lookup query → name={town_name}, code={dept_code}")
        row = session.execute(query, {"name": town_name, "code": dept_code}).fetchone()

        if not row:
            logger.warning(f"No row returned for name={town_name}, code={dept_code}")
            return None

        result = dict(row._mapping)
        logger.debug(f"Query result row → {result}")
        return result
    finally:
        Session.remove()

def get_cached_description(town_code, department, lang):
    query = text("""
        SELECT description FROM descriptions
        WHERE town_code = :code AND department = :dept AND language = :lang
        LIMIT 1
    """)
    
    session = Session()
    try:
        result = session.execute(query, {
            "code": town_code,
            "dept": department,
            "lang": lang
        }).fetchone()
        return result[0] if result else None
    finally:
        Session.remove()

def cache_description(town_code, department, lang, description):
    insert = text("""
        INSERT INTO descriptions (town_code, department, language, description)
        VALUES (:code, :dept, :lang, :desc)
        ON CONFLICT (town_code, department, language) DO NOTHING
    """)
    
    session = Session()
    try:
        session.execute(insert, {
            "code": town_code,
            "dept": department,
            "lang": lang,
            "desc": description
        })
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        Session.remove()

@location_bp.route("/descriptions", methods=["DELETE"])
def delete_cached_description():
    town_code = request.args.get("town_code")
    department = request.args.get("department")

    if not (town_code and department):
        return jsonify({"error": "Missing query parameter"}), 400

    delete_q = text("""
        DELETE FROM descriptions
        WHERE town_code  = :code
        AND department = :dept
    """)
    session = Session()
    try:
        result = session.execute(delete_q, {
            "code": town_code,
            "dept": department
        })
        session.commit()
        current_app.logger.info(f"Deleted {result.rowcount} rows from descriptions for {town_code}-{department}")
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to delete cache: {e}", exc_info=True)
        return jsonify({"error": "Could not clear cache"}), 500
    finally:
        Session.remove()