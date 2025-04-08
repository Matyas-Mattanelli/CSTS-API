import psycopg2
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Start the API
app: FastAPI = FastAPI()

# Load configuration
with open('config.json', 'r') as handle:
    config: dict = json.load(handle)

# Define who can access the API
app.add_middleware(CORSMiddleware, allow_origins=config['origins'], allow_credentials=False, allow_methods=['GET'], allow_headers=[])

# Connect to the databasse
connection = psycopg2.connect(database=config['database'], user=config['user'], password=config['password'], host=config['host'], port=int(config['port']))
cursor = connection.cursor()

### Get data based on IDT ###

# Define the query
query_IDT: str = """
SELECT d.idt, d.name, cr.club, cr.country, d2.name AS partner, c.comp_id, c.event_id, c.type, c.age_group, c.rank, c.discipline, c.category, e.date, e.name,
cr.position, c.n_participants, cr.points, cr.final, SUM(cr.points) OVER (PARTITION BY cr.idt, d2.name, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_points,
SUM(CAST(cr.final AS INT)) OVER (PARTITION BY cr.idt, d2.name, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_finals
FROM competition_results cr JOIN
dancers d ON d.idt=cr.idt JOIN
competitions c ON cr.comp_id=c.comp_id JOIN
events e ON e.event_id=c.event_id LEFT JOIN
competition_results cr2 ON (cr2.comp_id=cr.comp_id AND cr2.couple_id=cr.couple_id AND cr.idt<>cr2.idt) LEFT JOIN
dancers d2 ON (cr2.idt=d2.idt)
WHERE cr.idt=%s
OR d.name IN
(SELECT DISTINCT name FROM dancers
WHERE idt=%s)
ORDER BY e.date
"""

# Define the path operation
@app.get(f'{config["api_path"]}/IDT/{{IDT}}')
def get_data_by_IDT(IDT: str) -> list:
    """
    Function fetching data based on the provided IDT
    """
    # Strip trailing spaces
    IDT = IDT.strip()

    # Check that IDT is numeric
    if not IDT.isnumeric():
        return []
    
    # Fetch the data
    cursor.execute(query_IDT, (IDT, IDT))
    records: list[tuple] = cursor.fetchall()

    return records

### Get data based on name ###

# Define the query
query_name: str = """
SELECT d.idt, d.name, cr.club, cr.country, d2.name AS partner, c.comp_id, c.event_id, c.type, c.age_group, c.rank, c.discipline, c.category, e.date, e.name,
cr.position, c.n_participants, cr.points, cr.final, SUM(cr.points) OVER (PARTITION BY cr.idt, d2.name, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_points,
SUM(CAST(cr.final AS INT)) OVER (PARTITION BY cr.idt, d2.name, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_finals
FROM competition_results cr JOIN
dancers d ON d.idt=cr.idt JOIN
competitions c ON cr.comp_id=c.comp_id JOIN
events e ON e.event_id=c.event_id LEFT JOIN
competition_results cr2 ON (cr2.comp_id=cr.comp_id AND cr2.couple_id=cr.couple_id AND cr.idt<>cr2.idt) LEFT JOIN
dancers d2 ON (cr2.idt=d2.idt)
WHERE d.name=%s
OR d.idt IN
(SELECT DISTINCT idt FROM dancers
WHERE d.name=%s)
ORDER BY e.date
"""

# Define the path operation
@app.get(f'{config["api_path"]}/name/{{name}}')
def get_data_by_name(name: str) -> list:
    """
    Function fetching data based on the provided IDT
    """
    # Clean the name
    name = name.strip().replace('.', ' ').replace('_', ' ').title()

    # Validate the name
    if (len(name) == 0) or name.isnumeric():
        return []
    
    # Fetch the data
    cursor.execute(query_name, (name, name))
    records: list[tuple] = cursor.fetchall()

    return records