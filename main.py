import psycopg2
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load configuration
with open('config.json', 'r') as handle:
    config: dict = json.load(handle)

# Start the API
app: FastAPI = FastAPI(docs_url=config["docs_url"], redoc_url=config["redoc_url"])

# Define who can access the API
app.add_middleware(CORSMiddleware, allow_origins=config['origins'], allow_credentials=False, allow_methods=['GET'], allow_headers=[])

# Connect to the databasse
connection = psycopg2.connect(database=config['database'], user=config['user'], password=config['password'], host=config['host'], port=int(config['port']))
cursor = connection.cursor()

### Get data based on IDT ###

# Define the base query
query_base: str = """
SELECT DISTINCT DATE_PART('year', e.date) AS year, DATE_PART('month', e.date) as month, e.date, d.idt, d.name, cr.club, cr.country, d2.main_name AS partner, c.comp_id, c.event_id, c.type, c.age_group, c.rank, c.discipline, c.category, e.name,
cr.position, c.n_participants, cr.points, cr.final, SUM(cr.points) OVER (PARTITION BY cr.idt, d2.main_name, c.type, c.rank, c.discipline ORDER BY e.date) AS cumulative_points,
SUM(CAST(cr.final AS INT)) OVER (PARTITION BY cr.idt, d2.main_name, c.type, c.rank, c.discipline ORDER BY e.date) AS cumulative_finals
FROM competition_results cr JOIN
dancers d ON d.idt=cr.idt JOIN
competitions c ON cr.comp_id=c.comp_id JOIN
events e ON e.event_id=c.event_id LEFT JOIN
competition_results cr2 ON (cr2.comp_id=cr.comp_id AND cr2.couple_id=cr.couple_id AND cr.idt<>cr2.idt) LEFT JOIN
dancers d2 ON (cr2.idt=d2.idt)
"""

# Define the condition
query_condition_IDT: str = """
WHERE cr.idt=%s
"""

# Define the ordering
query_order: str = """
ORDER BY e.date
"""

# Define the simple query for IDT
query_IDT: str = query_base + query_condition_IDT + query_order

# Define an advanced version of the query (also including all names associated with the IDT even though they may have a different IDT)
query_IDT_advanced: str = query_base + query_condition_IDT + """
OR d.name IN
(SELECT DISTINCT name FROM dancers
WHERE idt=%s)
""" + query_order

# Define the path operation
@app.get(f'{config["api_path"]}/IDT/{{IDT}}')
def get_data_by_IDT(IDT: str, advanced: bool = False) -> list:
    """
    Function fetching data based on the provided IDT
    """
    # Strip trailing spaces
    IDT = IDT.strip()

    # Check that IDT is numeric
    if not IDT.isnumeric():
        return []
    
    # Fetch the data
    if advanced:
        cursor.execute(query_IDT_advanced, (IDT, IDT))
    else:
        cursor.execute(query_IDT, (IDT,))
    records: list[tuple] = cursor.fetchall()

    return records

### Get data based on name ###

# Define the condition
query_condition_name: str = """
WHERE d.name=%s
"""

# Define the simple query for name
query_name: str = query_base + query_condition_name + query_order

# Define an advanced version of the query (also including all IDTs associated with the query even when they do not have the same name)
query_name_advanced: str = query_base + query_condition_name + """
OR d.idt IN
(SELECT DISTINCT idt FROM dancers
WHERE d.name=%s)
""" + query_order

# Define the path operation
@app.get(f'{config["api_path"]}/name/{{name}}')
def get_data_by_name(name: str, advanced: bool = False) -> list:
    """
    Function fetching data based on the provided IDT
    """
    # Clean the name
    name = name.strip().replace('.', ' ').replace('_', ' ').title()

    # Validate the name
    if (len(name) == 0) or name.isnumeric():
        return []
    
    # Fetch the data
    if advanced:
        cursor.execute(query_name_advanced, (name, name))
    else:
        cursor.execute(query_name, (name,))
    records: list[tuple] = cursor.fetchall()

    # Try reversing the name
    if len(records) == 0:
        name = ' '.join(name.rsplit(' ', 1)[::-1])
        if advanced:
            cursor.execute(query_name_advanced, (name, name))
        else:
            cursor.execute(query_name, (name,))
        records = cursor.fetchall()

    return records