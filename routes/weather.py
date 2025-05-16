from flask import Blueprint, request, jsonify
import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('WEATHER_API_KEY')

weather_bp = Blueprint("weather", __name__, url_prefix="/api")

@weather_bp.route("/weather", methods=["GET"])
def get_weather():
   lat = request.args.get("lat")
   lon = request.args.get("lon")
   
   url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={lat},{lon}&days=10"
   response = requests.get(url)
   data = response.json()
   
   forecast = []
   for day in data["forecast"]["forecastday"]:
      forecast.append({
         "date": day["date"],
         "temp_min": day["day"]["mintemp_c"],
         "temp_max": day["day"]["maxtemp_c"],
         "description": day["day"]["condition"]["text"],
         "icon": day["day"]["condition"]["icon"]
      })

   return jsonify(forecast)