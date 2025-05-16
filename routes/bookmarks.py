from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text
from extensions import Session

bookmarks_bp = Blueprint("bookmarks", __name__, url_prefix="/api")

@bookmarks_bp.route("/bookmarks", methods=["GET"])
@jwt_required()
def get_bookmarks():
    user_id = int(get_jwt_identity())
    sql = text("""SELECT 
               b.town_name, 
               b.department_code, 
               d.name AS department_name,
               r.name AS region_name
               FROM bookmarks b
               JOIN departments d ON d.code = b.department_code
               JOIN regions r ON d.region = r.code
               WHERE b.user_id = :uid""")
    
    session = Session()
    try:
        result = session.execute(sql, {"uid": user_id}).fetchall()
        bookmarks = [{"town_name": row.town_name, "department_code": row.department_code, "department_name": row.department_name, "region_name": row.region_name} for row in result]
        return jsonify({"bookmarks": bookmarks})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        Session.remove()

@bookmarks_bp.route("/bookmarks", methods=["POST"])
@jwt_required()
def add_bookmark():
    user_id = int(get_jwt_identity())
    town_name = request.json.get("town_name")
    department_code = request.json.get("code")

    if not (town_name or department_code): return jsonify({"error": "Missing town_name"}), 400

    sql = text("""
        INSERT INTO bookmarks (user_id, town_name, department_code) 
        VALUES (:uid, :town, :code) 
        ON CONFLICT DO NOTHING
        RETURNING town_name
    """)
    
    session = Session()
    try:
        result = session.execute(sql, {"uid": user_id, "town": town_name, "code": department_code})
        session.commit()
        if result.rowcount > 0:
            return jsonify({"status": "added", "town": town_name, "code": department_code}), 201
        return jsonify({"status": "already_exists"}), 200
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        Session.remove()

@bookmarks_bp.route("/bookmarks", methods=["DELETE"])
@jwt_required()
def remove_bookmark():
    user_id = int(get_jwt_identity())
    town_name = request.json.get("town_name")
    department_code = request.json.get("code")

    if not (town_name or department_code): return jsonify({"error": "Missing town_name"}), 400

    sql = text("""
        DELETE FROM bookmarks 
        WHERE user_id = :uid AND town_name = :town AND department_code = :code
        RETURNING town_name
    """)
    
    session = Session()
    try:
        result = session.execute(sql, {"uid": user_id, "town": town_name, "code": department_code})
        session.commit()
        if result.rowcount > 0:
            return jsonify({"status": "deleted", "town": town_name, "code": department_code}), 200
        return jsonify({"error": "Bookmark not found"}), 404
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        Session.remove()
        