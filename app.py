import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from routes.towns import towns_bp
from routes.location import location_bp
from routes.auth import auth_bp
from routes.search import search_bp
from routes.bookmarks import bookmarks_bp
from routes.ratings import ratings_bp
from dotenv import load_dotenv
from datetime import timedelta
from routes.weather import weather_bp

load_dotenv()

app = Flask(__name__)
CORS(app,
     supports_credentials=True,
     origins=["https://explorefrance.vercel.app"])

app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
app.config["JWT_ACCESS_COOKIE_PATH"] = "/"
app.config["JWT_COOKIE_SECURE"] = True     
app.config["JWT_COOKIE_SAMESITE"] = "None" 
app.config["JWT_COOKIE_CSRF_PROTECT"] = False
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=12) 
jwt = JWTManager(app)

app.register_blueprint(location_bp)
app.register_blueprint(towns_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(search_bp)
app.register_blueprint(bookmarks_bp)
app.register_blueprint(ratings_bp)
app.register_blueprint(weather_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

