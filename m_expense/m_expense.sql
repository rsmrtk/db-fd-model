-- PostgreSQL table definition for expenses
-- Create ENUM type for expense types
CREATE TYPE expense_type_enum AS ENUM (
    'food',
    'restaurants',
    'entertainment',
    'dwelling',
    'utilities',
    'household_purchases',
    'transfer',
    'others'
);

-- Create expenses table
CREATE TABLE IF NOT EXISTS expenses
(
    expense_id     UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    expense_name   VARCHAR(255) NOT NULL,
    expense_amount DECIMAL(15, 2),
    expense_type   expense_type_enum,
    expense_date   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(expense_date);
CREATE INDEX IF NOT EXISTS idx_expenses_type ON expenses(expense_type);
CREATE INDEX IF NOT EXISTS idx_expenses_amount ON expenses(expense_amount);

-- Add comments for documentation
COMMENT ON TABLE expenses IS 'Table to store expense records';
COMMENT ON COLUMN expenses.expense_id IS 'Unique identifier for expense (UUID)';
COMMENT ON COLUMN expenses.expense_name IS 'Name/description of the expense';
COMMENT ON COLUMN expenses.expense_amount IS 'Amount of the expense';
COMMENT ON COLUMN expenses.expense_type IS 'Category/type of expense';
COMMENT ON COLUMN expenses.expense_date IS 'Date when expense occurred';
COMMENT ON COLUMN expenses.created_at IS 'Record creation timestamp';
