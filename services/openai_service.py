import openai
import os
from dotenv import load_dotenv
from flask import current_app

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def get_description(town_name, department_name, region_name, lang="en"):
    prompt = f"""
Provide detailed information about {town_name}, located in the {department_name} department of the {region_name} region of France. Use the following structure exactly:

DESCRIPTION:
[2-3 sentence overview detailing the town's uniqueness and what makes it intriguing.]

HISTORY:
[2-3 sentences briefly outlining the town’s background and notable past events that define the town.]

ATTRACTIONS:
[Choose between 1 and 5 attractions, based on how many truly notable sites the town has. Pick parks, scenic areas, castles, etc. anything that the town offers that is unique or interesting.  
 • Small villages or lesser-known towns: 1 key points of interest.  
 • Mid-sized towns: 2 main attractions.  
 • Major cities or historically rich locales: up to 5.]
1. [Name] – [Short description of its significance and location]
2. [Name] – [Short description of its significance and location]
(…continue numbering up to the chosen count) 
"""

    if lang == "fr":
        prompt = f"""Fournis des informations détaillées sur {town_name}, située dans le département de {department_name}, en région {region_name}, en France. Suis exactement la structure ci-dessous :

DESCRIPTION:
[Une présentation de 2 à 3 phrases décrivant ce qui rend cette ville unique, attrayante ou intrigante pour les visiteurs.]

HISTORY:
[Un résumé en 2 à 3 phrases des origines de la ville, de son histoire marquante ou d’événements notables qui la définissent.]

ATTRACTIONS:
[Indique entre 1 et 5 attractions selon l’importance réelle de la ville. Choisis uniquement les lieux vraiment remarquables : parcs, châteaux, monuments, paysages naturels, etc.
• Pour les petits villages ou villes peu connues : 1 point d’intérêt majeur.
• Pour les villes moyennes : 2 attractions principales.
• Pour les grandes villes ou lieux à forte valeur historique : jusqu’à 5 attractions possibles.]
1. [Nom] – [Brève description de son intérêt et de sa localisation]
2. [Nom] – [Brève description de son intérêt et de sa localisation]
(…poursuivre la numérotation jusqu’au nombre d’attractions retenu)
"""

    response = openai.chat.completions.create(
        model="gpt-4.1-nano",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        top_p=0.9,
        frequency_penalty=0.1
    )

    return response.choices[0].message.content.strip()
