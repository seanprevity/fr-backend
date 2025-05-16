import psycopg2
from psycopg2.extras import RealDictCursor
import os
import requests
from extensions import Session
from sqlalchemy import text
from dotenv import load_dotenv
import time
import unicodedata

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def clean_town_name(name):
    if not name:
        return name

    # broken character in original database - manually inputted and updated
    replacements = {
        "": "œ",    
        "??": "œ", 
    }
    for bad, good in replacements.items():
        name = name.replace(bad, good)
    name = unicodedata.normalize('NFKC', name)
    return name

def fetch_towns_missing_coords(limit=1000):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT id, name, department
        FROM towns
        WHERE latitude IS NULL OR longitude IS NULL
        LIMIT %s;
    """
    cur.execute(query, (limit,))
    towns = cur.fetchall()
    
    cur.close()
    conn.close()
    return towns

def geocode_town(town_name, department_name):
    town_name = clean_town_name(town_name)
    address = f"{town_name}, {department_name}, France"
    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={
                "address": address,
                "key": os.getenv("GEOCODING_API_KEY")
            },
            timeout=5
        )
        data = resp.json()
        if data.get("status") != "OK" or not data.get("results"):
            return None, None, None

        top = data["results"][0]
        loc = top["geometry"]["location"]
        lat, lng = loc["lat"], loc["lng"]

        official_name = None
        for comp in top["address_components"]:
            if "locality" in comp["types"] or "postal_town" in comp["types"]:
                official_name = comp["long_name"]
                break

        if not official_name:
            official_name = top["formatted_address"].split(",")[0]

        return official_name, lat, lng

    except Exception as e:
        print(f"Error geocoding {town_name}: {e}")
        return None, None, None

def get_department_name(dept_code):
    query = text("""
                 SELECT name
                 FROM departments
                 WHERE code = :code
                 LIMIT 1
                 """)
    session = Session()
    try:
        row = session.execute(query, {"code": dept_code}).fetchone()
        if row:
            return row._mapping["name"]
        return None
    finally:
        Session.remove()

def main():
    towns = fetch_towns_missing_coords(limit=10)
    print(f"Found {len(towns)} towns missing coordinates.")

    session = Session()
    updates_made = 0

    for town in towns:
        town_name = town["name"]
        department_code = town["department"]

        print(f"Updating {town_name}...")

        department_name = get_department_name(department_code)
        if not department_name:
            print(f"  ⚠️ Could not find department name for code {department_code}, skipping.")
            continue

        official_name, lat, lng = geocode_town(town_name, department_name)

        if lat and lng:
            try:
                session.execute(
                    text("""
                        UPDATE towns
                        SET name = :official,
                            latitude = :lat,
                            longitude = :lng
                        WHERE id = :id
                    """),
                    { "official": official_name, "lat": lat, "lng": lng, "id": town["id"] }
                )
                updates_made += 1
                print(f"  ✅ Updated {official_name} ({lat}, {lng})")
            except Exception as e:
                print(f"  ❌ Failed to update {town_name}: {e}")

        else:
            print(f"  ⚠️ No coordinates found for {town_name}.")

        time.sleep(0.2)  # avoid rate limits

    session.commit()
    session.close()
    print(f"\nFinished updating {updates_made} towns.")

if __name__ == "__main__":
    main()