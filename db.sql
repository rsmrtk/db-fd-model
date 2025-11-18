-- CREATE TABLE incomes
-- (
--     income_id     VARCHAR(36) NOT NULL PRIMARY KEY,
--     income_name   VARCHAR(255),
--     income_amount NUMERIC,
--     -- ENUM(salary, transfer, others) COLUMN type
--     income_type   VARCHAR(50),
--     income_date   TIMESTAMP,
--     created_at    TIMESTAMP
-- );

CREATE TABLE expenses
(
    expense_id     VARCHAR(36) NOT NULL PRIMARY KEY,
    expense_name   VARCHAR(255),
    expense_amount NUMERIC,
    -- ENUM(food, restaurants, entertainment, dwelling, utilities,household purchases, transfer, others) COLUMN type
    expense_type   VARCHAR(50),
    expense_date   TIMESTAMP,
    created_at     TIMESTAMP
);