import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import text
from extensions import Session
import unicodedata
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def clean_town_name(name):
    if not name:
        return name
    replacements = {
        "": "œ",
        "??": "œ",
        "\u009C": "œ",
    }
    for bad, good in replacements.items():
        name = name.replace(bad, good)
    return unicodedata.normalize('NFKC', name)

def fetch_all_towns():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT id, name, department
        FROM towns;
    """
    cur.execute(query)
    towns = cur.fetchall()
    
    cur.close()
    conn.close()
    return towns

def update_town_name(town_id, new_name):
    session = Session()
    try:
        session.execute(
            text("""
                UPDATE towns
                SET name = :new_name
                WHERE id = :town_id
            """),
            {"new_name": new_name, "town_id": town_id}
        )
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Failed to update town ID {town_id}: {e}")
    finally:
        Session.remove()

def main():
    towns = fetch_all_towns()
    print(f"Found {len(towns)} towns total.")

    updates = 0
    for town in towns:
        original_name = town["name"]
        cleaned_name = clean_town_name(original_name)

        if original_name != cleaned_name:
            print(f"Updating '{original_name}' to '{cleaned_name}'")
            update_town_name(town["id"], cleaned_name)
            updates += 1

    print(f"Done. Updated {updates} town names.")

if __name__ == "__main__":
    main()
