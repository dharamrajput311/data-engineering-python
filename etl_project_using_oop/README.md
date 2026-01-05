# ETL Project Using Python (OOP)

This project demonstrates an end-to-end ETL pipeline built using
pure Python and Object-Oriented Programming (OOP) principles.
It simulates a real-world batch data ingestion workflow without
using high-level libraries like Pandas.

---

## Project Goal

To understand how OOP concepts are applied in real-world
data engineering pipelines by:
- Designing reusable ETL components
- Separating extraction, transformation, and loading logic
- Writing clean, maintainable, and extensible code

---

## ETL Workflow

**Source → Transform → Target**

- **Source**
  - CSV and JSON input files

- **Transform**
  - Data validation and cleansing
  - Business-rule-based transformations
  - Identification of bad records

- **Target**
  - Valid records loaded into PostgreSQL
  - Invalid records written to a separate CSV
  - Processed files archived for audit and reprocessing

---

## Features

- CSV and JSON file ingestion
- Schema validation and data transformation
- PostgreSQL data loading
- Bad record handling into CSV
- File archiving after successful processing
- Secure credential handling using environment variables
- Config-driven design using YAML

---

## Tech Stack

- Python 3.x
- PostgreSQL
- YAML (configuration management)
- Object-Oriented Programming:
  - Abstract Base Classes (ABC)
  - Inheritance
  - Polymorphism

---

## Project Structure

Refer to `project_structure.txt` for detailed folder and file layout.

---

## How to Run

1. Create and activate a virtual environment
2. Install dependencies:
	pip install -r requirements.txt
3. Configure environment variables:   
	export DB_HOST=...
	export DB_USER=...
	export DB_PASSWORD=...
4. Place input files into:
		data/input/
5. Run the pipeline:
	python main.py

---

## Future Enhancements

- Pandas-based ETL implementation for performance comparison
	
	

