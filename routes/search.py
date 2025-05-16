from flask import Blueprint, request, jsonify
from sqlalchemy import text
from extensions import Session 

search_bp = Blueprint("search", __name__, url_prefix="/api")

@search_bp.route("/search")
def search_towns():
    q = request.args.get("query", "").strip()
    if not q: return jsonify([]) 

    sql = text("""
      SELECT 
      t.id, 
      t.name, 
      t.latitude, 
      t.longitude, 
      t.department, 
      d.name AS department_name, 
      r.name AS region_name
      FROM towns t
      JOIN departments d ON t.department = d.code
      JOIN regions r ON d.region = r.code
      WHERE t.name ILIKE :prefix
      ORDER BY name
      LIMIT 10
    """)
    
    session = Session()
    try:
        rows = session.execute(sql, {"prefix": f"{q}%"}).fetchall()
        towns = [dict(zip(row._fields, row)) for row in rows]
        return jsonify(towns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        Session.remove()