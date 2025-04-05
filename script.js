// Get the div for showing the results
const divResults = document.getElementById("results");
const divMsg = document.getElementById("msg");
const filters = document.getElementById("filters");

// Get the input element
const input = document.getElementById("input");

// Specify the headers for the table with results
const columns = ["IDT", "Jméno", "Klub", "Stát", "Partner/ka", "ID soutěže", "ID eventu", "Typ", "Věková skupina", "Třída", "Disciplína", "Kategorie", "Datum", "Jméno eventu", "Pozice", "Počet účastníků", "Body", "Finále", "Celkem body", "Celkem finále"];
const uncheckedColumns = ["IDT", "Stát", "ID soutěže", "ID eventu", "Kategorie", "Jméno eventu"];

// Define a function bulding the filters
function buildFilters() {
    // Loop through the columns and create checkbox and label for each
    columns.forEach(col => {
        // Create checkbox
        const colClean = col.toLowerCase().replace(" ", "_").replace("/", "");
        const checkbox = document.createElement("input");
        checkbox.type = 'checkbox';
        checkbox.name = colClean;
        checkbox.value = colClean;
        checkbox.id = colClean;
        if (uncheckedColumns.indexOf(col) === -1) {
            checkbox.checked = true;
        } else {
            checkbox.checked = false;
        }
        checkbox.className = "filters";
        checkbox.onchange = () => {filterTable(col)};
        filters.appendChild(checkbox);

        // Create label
        const label = document.createElement('label');
        label.for = colClean;
        label.textContent = col;
        label.className = "filters-label";
        filters.appendChild(label);
    });
}

// Define a function filtering the table based on a given filter
function filterTable(col) {
        // Find the filter
        const filter = document.getElementById(col.toLowerCase().replace(" ", "_").replace("/", ""));
        
        // Find the corresponding cells
        const cells = document.querySelectorAll(`table tr :is(td, th):nth-child(${columns.indexOf(col)+1})`);
        
        // Hide or show the cells
        cells.forEach(cell => {
            console.log(cell.style.display);
            cell.style.display = filter.checked === true ? 'table-cell' : 'none'
        });
}

// Define a function showing or hiding the filters
const filterBtn = document.getElementById('show-filters');
function showFilters() {
    // Hide if shown
    if (filters.style.display === 'block') {
        filters.style.display = 'none';
        filterBtn.textContent = 'Show filters'

    // Show if hidden
    } else {
        filters.style.display = 'block';
        filterBtn.textContent = 'Hide filters'
    }
}

// Define a function building a table from the given data
function buildTable(data) {
    // Check if data was obtained
    if (data != null) {
        // Indicate that the results are displayed
        divMsg.textContent = `Zobrazuji výsledky pro ${input.value}`;

        // Destroy the table if it exists
        const currentTable = document.querySelector("table");
        if (currentTable != null) {
            currentTable.remove();
        }

        // Create a table to store the results
        const table = document.createElement("table");
        table.className = "school"
        const headerRow = document.createElement("tr"); // Create a header row

        // Add the headers
        columns.forEach(col => {
            const headerCol = document.createElement("th");
            headerCol.textContent = col;
            headerRow.appendChild(headerCol);
        });
        table.appendChild(headerRow);

        // Add the data for each row
        data.forEach(
            row => {
                // Create the row
                const tr = document.createElement('tr');

                // Add the cells
                row.forEach(
                    cell => {
                        const td = document.createElement('td');
                        td.textContent = cell;
                        tr.appendChild(td);
                    }
                );

                // Add the row to the table
                table.appendChild(tr);
            }
        );

        // Show the table
        divResults.appendChild(table);

        // Filter the columns
        columns.forEach(col => {filterTable(col)});
    }
}

// Define a function fetching the data and building the table upon a button click
function showResults() {
    // Get the user input
    const inputText = input.value;

    // Specify that the results are being fetched
    divMsg.textContent = `Searching for ${inputText}...`;

    // Fetch the data and build the table
    fetchData(inputText, buildTable);
}

// Define a function fetching the data 
function fetchData(input, callback) {
    // Specify the request based on whether IDT or name was specified
    let reqText;
    if (isNaN(input)) {
        reqText = `http://127.0.0.1:8000/name/${encodeURI(input)}`;
    } else {
        reqText = `http://127.0.0.1:8000/IDT/${input}`;
    };

    // Try to get the data
    fetch(reqText)
        .then(response => {
            if (!response.ok) {
                printNoResults(input);
                return null;
            } else {
                return response.json();
            }
        })
        .then(data => {
            if (data.length === 0) {
                printNoResults(input);    
            } else {
                callback(data)
            }
        })
        .catch(error => {
            printNoResults(input);
            console.error(`Failed to fetch: `, error);
        });
}

// Define a function to run in case of an error
function printNoResults(input) {
    divMsg.textContent = `Výsledky pro ${input} nebyly nalezeny`;
}

// Build the filters
buildFilters();