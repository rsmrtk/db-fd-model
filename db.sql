CREATE TABLE incomes (
                     income_id VARCHAR(36) NOT NULL PRIMARY KEY,
                     income_name VARCHAR(255),
                     income_amount NUMERIC,
                     -- ENUM(salary, transfer, others) COLUMN type
                     income_type VARCHAR(50),
                     income_date TIMESTAMP,
                     created_at TIMESTAMP
);