CREATE TABLE IF NOT EXISTS employee_stg (
    emp_id INT,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    salary NUMERIC(12, 2),
	inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
