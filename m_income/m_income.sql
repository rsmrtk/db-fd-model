CREATE TABLE incomes (
                     income_id STRING(36) NOT NULL,
                     income_name STRING(255),
                     income_amount NUMERIC,
                     --ENUM(salary, transfer, others) COLUMN status
                     income_type STRING(50),
                     income_date TIMESTAMP,
                     created_at TIMESTAMP
) PRIMARY KEY (income_id);
