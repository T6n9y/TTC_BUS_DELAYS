# TTC Bus Delays

## Project Description

This project ingests TTC bus delay data from Toronto's Open Data CKAN API and stores it in a PostgreSQL database. The script fetches delay records filtered by specific date ranges (focusing on data from 2023 and 2024), parses the data into structured fields, and populates a relational table named `ttcDelays`. This provides a robust foundation for transit data analysis and data-driven decision-making aimed at improving TTC performance.

## Key Features

- **API Integration:**  
  Connects seamlessly to Toronto's Open Data CKAN API to fetch TTC bus delay records.
  
- **Data Filtering:**  
  Retrieves records for specific date ranges (e.g., 2023 and 2024), allowing you to update existing datasets with missing historical data.
  
- **Structured Database Storage:**  
  Parses the fetched data and stores it in a PostgreSQL table with clearly defined columns such as day, record ID, date, time, bound, route, station, vehicle, incident, and delay metrics.
  
- **Modular and Extensible:**  
  Clean, modular Python code that can be easily extended to include additional data processing, error handling, or analytics features.

## Technologies Used

- **Python:** Primary programming language.
- **Requests:** For making HTTP API calls.
- **Psycopg2:** For PostgreSQL database integration.
- **PostgreSQL:** Relational database for robust and scalable data storage.

## Getting Started

### Prerequisites

- Python 3.x installed on your machine.
- PostgreSQL installed and running.
- Git for version control.

### Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/T6n9y/TTC_BUS_DELAYS.git
   cd TTC_BUS_DELAYS
