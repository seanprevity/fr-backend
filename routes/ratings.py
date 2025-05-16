from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text
from extensions import Session

ratings_bp = Blueprint('ratings', __name__, url_prefix="/api/ratings")

@ratings_bp.route('/<town_code>/<department>', methods=['GET'])
def get_ratings(town_code, department):
    session = Session()
    try:
        result = session.execute(
            text("""
                SELECT AVG(rating) as average, COUNT(*) as count 
                FROM ratings 
                WHERE town_code = :town_code AND department = :department
            """),
            {'town_code': town_code, 'department': department}
        ).fetchone()
        
        return jsonify({
            'average': float(result.average) if result.average else 0,
            'count': result.count,
            'town_code': town_code,
            'department': department
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        Session.remove()  

@ratings_bp.route('', methods=['POST'])
@jwt_required()
def add_rating():
    session = Session()
    try:
        data = request.get_json()
        user_id = int(get_jwt_identity())
        
        # Validate all required fields
        if not all(k in data for k in ['town_code', 'department', 'rating']):
            return jsonify({'error': 'Missing required fields'}), 400

        # Validate rating is between 1-5
        if not 1 <= int(data['rating']) <= 5:
            return jsonify({'error': 'Rating must be between 1 and 5'}), 400

        session.execute(
            text("""
                INSERT INTO ratings (town_code, department, user_id, rating)
                VALUES (:town_code, :department, :user_id, :rating)
                ON CONFLICT (town_code, department, user_id) 
                DO UPDATE SET rating = EXCLUDED.rating
            """),
            {
                'town_code': data['town_code'],
                'department': data['department'],
                'user_id': user_id,
                'rating': data['rating']
            }
        )
        session.commit()
        
        return jsonify({
            'success': True,
            'town_code': data['town_code'],
            'department': data['department'],
            'rating': data['rating']
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        Session.remove()

@ratings_bp.route('/heatmap', methods=["GET"])
def get_heatmap_data():
    session = Session()
    try:
        result = session.execute(text("""
            SELECT latitude, longitude, AVG(rating) as avg_rating
            FROM ratings
            JOIN towns ON ratings.town_code = towns.code AND ratings.department = towns.department
            GROUP BY latitude, longitude
        """)).fetchall()

        points = [{"lat": r.latitude, "lng": r.longitude, "weight": r.avg_rating or 1} for r in result]

        return jsonify(points)
    finally:
        Session.remove()