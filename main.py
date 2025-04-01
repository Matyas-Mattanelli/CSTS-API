import psycopg2
import json
from fastapi import FastAPI

# Start the API
app: FastAPI = FastAPI()

# Load configuration
with open('config.json', 'r') as handle:
    config: dict = json.load(handle)

# Connect to the databasse
connection = psycopg2.connect(database=config['database'], user=config['user'], password=config['password'], host=config['host'], port=int(config['port']))
cursor = connection.cursor()

### Get data based on IDT ###

# Define the query
query_IDT: str = """
SELECT d.idt, d.name, cr.club, cr.country, d2.name AS partner, c.comp_id, c.event_id, c.type, c.age_group, c.rank, c.discipline, c.category, e.date, e.name,
cr.position, cr.points, cr.final, SUM(cr.points) OVER (PARTITION BY cr.idt, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_points,
SUM(CAST(cr.final AS INT)) OVER (PARTITION BY cr.idt, c.type, c.age_group, c.rank, c.discipline ORDER BY e.date) AS cumulative_finals
FROM competitionresults cr JOIN
dancers d ON d.idt=cr.idt JOIN
competitions c ON cr.comp_id=c.comp_id JOIN
events e ON e.event_id=c.event_id LEFT JOIN
competitionresults cr2 ON (cr2.comp_id=cr.comp_id AND cr2.couple_id=cr.couple_id AND cr.idt<>cr2.idt) LEFT JOIN
dancers d2 ON (cr2.idt=d2.idt)
WHERE cr.idt=%s
OR d.name IN
(SELECT DISTINCT name FROM dancers
WHERE idt=%s)
ORDER BY e.date
"""

# Define the path operation
@app.get('/IDT/{IDT}')
def get_data(IDT: str) -> list:
    """
    Function fetching data based on the provided IDT
    """
    # Check that IDT is numeric
    if not IDT.isnumeric():
        return []
    
    # Fetch the data
    cursor.execute(query_IDT, (IDT, IDT))
    records: list[tuple] = cursor.fetchall()

    return records
