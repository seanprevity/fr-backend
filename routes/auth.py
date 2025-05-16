from flask import Blueprint, request, jsonify, current_app, make_response
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token,
    set_access_cookies,
    unset_jwt_cookies,
    jwt_required,
    get_jwt_identity,
)
import traceback
from extensions import Session
from sqlalchemy import text

auth_bp = Blueprint("auth", __name__, url_prefix="/api")

@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json() or {}
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")

    if not (username and email and password):
        return jsonify({"error": "Username, email and password are required."}), 400

    if get_user_by_email(email):
        return jsonify({"error": "Email already in use."}), 409

    if get_user_by_username(username):
        return jsonify({"error": "Username already in use."}), 409

    password_hash = generate_password_hash(password)
    user = create_user(username, email, password_hash)
    current_app.logger.info(f"New user registered: {username}")
    return jsonify({"user": user}), 201

@auth_bp.route("/login", methods=["POST"])
def login():
    try:
        data = request.get_json() or {}
        current_app.logger.info(f"Received login payload: {data}")

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            current_app.logger.warning("Missing username or password")
            return jsonify({"error": "Username and password required."}), 400

        user = get_user_by_username(username)
        if not user:
            current_app.logger.warning(f"No user found for username: {username}")
            return jsonify({"error": "Invalid credentials."}), 401

        if not check_password_hash(user.get("password", ""), password):
            current_app.logger.warning("Password check failed")
            return jsonify({"error": "Invalid credentials."}), 401

        access_token = create_access_token(identity=str(user["user_id"]))
        resp = jsonify({
            "user": {
                "user_id": user["user_id"],
                "username": user["username"],
                "email": user["email"],
            }
        })
        set_access_cookies(resp, access_token)
        return resp, 200

    except Exception as e:
        current_app.logger.error("Exception during login:")
        current_app.logger.error(str(e))
        traceback.print_exc()  # for Azure Log Stream
        return jsonify({"error": "Internal server error"}), 500

def create_user(username, email, password_hash):
    sql = text("""
        INSERT INTO users (username, email, password)
        VALUES (:username, :email, :password)
        RETURNING user_id, username, email
    """)
    
    session = Session()
    try:
        row = session.execute(sql, {
            "username": username,
            "email": email,
            "password": password_hash
        }).fetchone()
        session.commit()
        return dict(row._mapping) if row else None
    except Exception as e:
        session.rollback()
        raise e
    finally:
        Session.remove()

def get_user_by_username(username):
    sql = text("SELECT * FROM users WHERE username = :username")
    
    session = Session()
    try:
        row = session.execute(sql, {"username": username}).fetchone()
        return dict(row._mapping) if row else None
    finally:
        Session.remove()

def get_user_by_email(email):
    sql = text("SELECT * FROM users WHERE email = :email")
    
    session = Session()
    try:
        row = session.execute(sql, {"email": email}).fetchone()
        return dict(row._mapping) if row else None
    finally:
        Session.remove()

@auth_bp.route('/verify-token', methods=['GET'])
@jwt_required()
def verify_token():
    return {"valid": True}, 200