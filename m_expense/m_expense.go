package m_expense

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rsmrtk/db-fd-model/m_options"
	"github.com/rsmrtk/db-fd-model/sql_builder"
	"github.com/rsmrtk/smartlg/logger"
)

const (
	Package = "m_expense"
	Table   = "expenses"
	ID      = "expense_id"
	// Secondary indexes
)

type Facade struct {
	log *logger.Logger
	db  *sql.DB
	//
}

func New(o *m_options.Options) *Facade {
	// Convert pgxpool.Pool to sql.DB using stdlib
	sqlDB := stdlib.OpenDBFromPool(o.DB)
	return &Facade{
		log: o.Log,
		db:  sqlDB,
		//
	}
}

func (f *Facade) logError(functionName string, msg string, h logger.H) {
	f.log.Error(fmt.Sprintf("[%s.%s - %s] %s", Package, functionName, Table, msg), h)
}

type Data struct {
	ExpenseID     interface{}
	ExpenseName   interface{}
	ExpenseAmount sql.NullFloat64
	ExpenseType   interface{}
	ExpenseDate   sql.NullTime
	CreatedAt     sql.NullTime
}

func (data *Data) Map() map[string]any {
	out := make(map[string]any, len(allFieldsList))
	out[string(ExpenseID)] = data.ExpenseID
	out[string(ExpenseName)] = data.ExpenseName
	out[string(ExpenseAmount)] = data.ExpenseAmount
	out[string(ExpenseType)] = data.ExpenseType
	out[string(ExpenseDate)] = data.ExpenseDate
	out[string(CreatedAt)] = data.CreatedAt
	return out
}

type Field string

const (
	ExpenseID     Field = "expense_id"
	ExpenseName   Field = "expense_name"
	ExpenseAmount Field = "expense_amount"
	ExpenseType   Field = "expense_type"
	ExpenseDate   Field = "expense_date"
	CreatedAt     Field = "created_at"
)

func GetAllFields() []Field {
	return []Field{
		ExpenseID,
		ExpenseName,
		ExpenseAmount,
		ExpenseType,
		ExpenseDate,
		CreatedAt,
	}
}

var allFieldsList = GetAllFields()

func (f Field) String() string {
	return string(f)
}

var fieldsMap = map[Field]func(data *Data) interface{}{
	ExpenseID:     func(data *Data) interface{} { return &data.ExpenseID },
	ExpenseName:   func(data *Data) interface{} { return &data.ExpenseName },
	ExpenseAmount: func(data *Data) interface{} { return &data.ExpenseAmount },
	ExpenseType:   func(data *Data) interface{} { return &data.ExpenseType },
	ExpenseDate:   func(data *Data) interface{} { return &data.ExpenseDate },
	CreatedAt:     func(data *Data) interface{} { return &data.CreatedAt },
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	ptrs := make([]interface{}, len(fields))

	for i, field := range fields {
		ptrs[i] = fieldsMap[field](data)
	}
	return ptrs
}

type PrimaryKey struct {
	ExpenseID string
}

func GetColumns() []string {
	return []string{
		ExpenseID.String(),
		ExpenseName.String(),
		ExpenseAmount.String(),
		ExpenseType.String(),
		ExpenseDate.String(),
		CreatedAt.String(),
	}
}

var allStringFields = GetColumns()

func GetValues(data *Data) []interface{} {
	return []interface{}{
		data.ExpenseID,
		data.ExpenseName,
		data.ExpenseAmount,
		data.ExpenseType,
		data.ExpenseDate,
		data.CreatedAt,
	}
}

type UpdateFields map[Field]interface{}

func (uf UpdateFields) Map() map[string]any {
	out := make(map[string]any, len(uf))
	for k, v := range uf {
		out[string(k)] = v
	}
	return out
}

type Op string

const (
	OpEq    Op = "="      // Equal
	OpNe    Op = "!="     // Not Equal
	OpIn    Op = "IN"     // In
	OpLt    Op = "<"      // Less than
	OpGt    Op = ">"      // Greater than
	OpLe    Op = "<="     // Less than or equal
	OpGe    Op = ">="     // Greater than or equal
	OpIs    Op = "IS"     // Is (null)
	OpIsNot Op = "IS NOT" // Is not (null)
)

type QueryParam struct {
	Field    Field
	Operator Op
	Value    interface{}
}

func makeStringFields(fields []Field) []string {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = `"` + string(f) + `"`
	}

	return stringFields
}

// nil selects all fields
func SelectQuery(fields []Field) string {
	var stringFields []string
	if fields == nil || len(fields) == 0 {
		stringFields = makeStringFields(allFieldsList)
	} else {
		stringFields = makeStringFields(fields)
	}

	queryString := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(stringFields, ", "), Table)

	return queryString
}

func ConstructWhereClause(queryParams []QueryParam) (whereClause string, args []interface{}) {
	whereClauses := make([]string, len(queryParams))
	args = make([]interface{}, 0, len(queryParams))
	paramCounter := 1

	for i, qp := range queryParams {
		if (qp.Operator == OpIs || qp.Operator == OpIsNot) && qp.Value == nil {
			whereClauses[i] = fmt.Sprintf(`"%s" %s NULL`, qp.Field, qp.Operator)
			continue
		}

		if qp.Operator == OpIn {
			// Handle IN operator for PostgreSQL
			values := qp.Value.([]interface{})
			placeholders := make([]string, len(values))
			for j := range values {
				placeholders[j] = fmt.Sprintf("$%d", paramCounter)
				paramCounter++
				args = append(args, values[j])
			}
			whereClauses[i] = fmt.Sprintf(`"%s" IN (%s)`, qp.Field, strings.Join(placeholders, ", "))
		} else {
			whereClauses[i] = fmt.Sprintf(`"%s" %s $%d`, qp.Field, qp.Operator, paramCounter)
			paramCounter++
			args = append(args, qp.Value)
		}
	}

	return strings.Join(whereClauses, " AND "), args
}

func (f *Facade) CreateOrUpdate(
	ctx context.Context,
	data *Data,
) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (expense_id) DO UPDATE SET
			expense_name = EXCLUDED.expense_name,
			expense_amount = EXCLUDED.expense_amount,
			expense_type = EXCLUDED.expense_type,
			expense_date = EXCLUDED.expense_date,
			created_at = EXCLUDED.created_at`,
		Table, strings.Join(allStringFields, ", "))

	_, err := f.db.ExecContext(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("CreateOrUpdate", "Failed to execute", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Create(ctx context.Context, data *Data) error {
	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)`,
		Table, strings.Join(allStringFields, ", "))

	_, err := f.db.ExecContext(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("Create", "Failed to execute", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Exists(
	ctx context.Context,
	pk PrimaryKey,
) bool {
	query := fmt.Sprintf(`SELECT 1 FROM %s WHERE expense_id = $1 LIMIT 1`, Table)

	var exists int
	err := f.db.QueryRowContext(ctx, query, pk.ExpenseID).Scan(&exists)
	return err == nil
}

func (f *Facade) Get(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, args := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	rows, err := f.db.QueryContext(ctx, queryString, args...)
	if err != nil {
		f.logError("Get", "Failed to query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("Get", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (f *Facade) Find(
	ctx context.Context,
	pk PrimaryKey,
	fields []Field,
) (*Data, error) {
	queryString := SelectQuery(fields)
	queryString += " WHERE expense_id = $1 LIMIT 1"

	row := f.db.QueryRowContext(ctx, queryString, pk.ExpenseID)

	var data Data
	err := row.Scan(data.fieldPtrs(fields)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		f.logError("Find", "Failed to Scan", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) Retrieve(
	ctx context.Context,
	pk PrimaryKey,
) (*Data, error) {
	return f.Find(ctx, pk, allFieldsList)
}

func (f *Facade) CreateTx(
	ctx context.Context,
	tx *sql.Tx,
	data *Data,
) error {
	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)`,
		Table, strings.Join(allStringFields, ", "))

	_, err := tx.ExecContext(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("CreateTx", "Failed to execute", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}
	return nil
}

func (f *Facade) UpdateTx(
	ctx context.Context,
	tx *sql.Tx,
	pk PrimaryKey,
	data UpdateFields,
) error {
	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+1)
	paramCounter := 1

	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf(`"%s" = $%d`, field.String(), paramCounter))
		args = append(args, value)
		paramCounter++
	}
	args = append(args, pk.ExpenseID)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE expense_id = $%d",
		Table, strings.Join(setClauses, ", "), paramCounter)

	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		f.logError("UpdateTx", "Failed to execute", logger.H{
			"error":      err,
			"primaryKey": pk,
			"data":       data,
		})
		return err
	}
	return nil
}

func (f *Facade) FindTx(
	ctx context.Context,
	tx *sql.Tx,
	pk PrimaryKey,
	fields []Field,
) (*Data, error) {
	queryString := SelectQuery(fields)
	queryString += " WHERE expense_id = $1 LIMIT 1"

	row := tx.QueryRowContext(ctx, queryString, pk.ExpenseID)

	var data Data
	err := row.Scan(data.fieldPtrs(fields)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		f.logError("FindTx", "Failed to Scan", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) GetByBuilder(ctx context.Context, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.StringPostgres()
	queryArgs := builder.ArgsPostgres()
	fields := builder.Fields()

	rows, err := f.db.QueryContext(ctx, queryStr, queryArgs...)
	if err != nil {
		f.logError("GetByBuilder", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByBuilder", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (f *Facade) GetByBuilderTx(ctx context.Context, tx *sql.Tx, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.StringPostgres()
	queryArgs := builder.ArgsPostgres()
	fields := builder.Fields()

	rows, err := tx.QueryContext(ctx, queryStr, queryArgs...)
	if err != nil {
		f.logError("GetByBuilderTx", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByBuilderTx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (f *Facade) GetByBuilderIter(ctx context.Context, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.StringPostgres()
	queryArgs := builder.ArgsPostgres()
	fields := builder.Fields()

	rows, err := f.db.QueryContext(ctx, queryStr, queryArgs...)
	if err != nil {
		f.logError("GetByBuilderIter", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByBuilderIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	return nil
}

func (f *Facade) GetByBuilderTxIter(ctx context.Context, tx *sql.Tx, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.StringPostgres()
	queryArgs := builder.ArgsPostgres()
	fields := builder.Fields()

	rows, err := tx.QueryContext(ctx, queryStr, queryArgs...)
	if err != nil {
		f.logError("GetByBuilderTxIter", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByBuilderTxIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	return nil
}

func (f *Facade) GetTx(
	ctx context.Context,
	tx *sql.Tx,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	q := SelectQuery(fields)
	whereClause, args := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		q += " WHERE " + whereClause
	}

	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		f.logError("GetTx", "Failed to query", logger.H{
			"error":       err,
			"queryParams": queryParams,
			"fields":      fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var d Data
		if err := rows.Scan(d.fieldPtrs(fields)...); err != nil {
			f.logError("GetTx", "Failed to Scan", logger.H{
				"error":       err,
				"queryParams": queryParams,
				"fields":      fields,
			})
			return nil, err
		}
		res = append(res, &d)
	}

	return res, nil
}

func (f *Facade) ExistTx(
	ctx context.Context,
	tx *sql.Tx,
	pk PrimaryKey,
) bool {
	query := fmt.Sprintf(`SELECT 1 FROM %s WHERE expense_id = $1 LIMIT 1`, Table)

	var exists int
	err := tx.QueryRowContext(ctx, query, pk.ExpenseID).Scan(&exists)
	return err == nil
}

func (c *Facade) InitBuilder() *sql_builder.Builder[Field] {
	b := sql_builder.New[Field]("")
	b.Select(allFieldsList...).From(Table)
	return b
}

func (f *Facade) Update(
	ctx context.Context,
	pk PrimaryKey,
	data UpdateFields,
) error {
	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+1)
	paramCounter := 1

	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf(`"%s" = $%d`, field.String(), paramCounter))
		args = append(args, value)
		paramCounter++
	}
	args = append(args, pk.ExpenseID)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE expense_id = $%d",
		Table, strings.Join(setClauses, ", "), paramCounter)

	_, err := f.db.ExecContext(ctx, query, args...)
	if err != nil {
		f.logError("Update", "Failed to execute", logger.H{
			"error": err,
			"data":  data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (f *Facade) UpdateByParams(
	ctx context.Context,
	queryParams []QueryParam,
	data UpdateFields,
) error {
	// Construct SQL query
	whereClauses, whereArgs := ConstructWhereClause(queryParams)
	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+len(whereArgs))
	paramCounter := 1

	// Construct SET clause
	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf(`"%s" = $%d`, field.String(), paramCounter))
		args = append(args, value)
		paramCounter++
	}

	// Add WHERE args
	for _, arg := range whereArgs {
		args = append(args, arg)
	}

	// Update WHERE clause placeholders
	updatedWhereClauses := whereClauses
	for i := 1; i <= len(whereArgs); i++ {
		old := fmt.Sprintf("$%d", i)
		new := fmt.Sprintf("$%d", paramCounter)
		updatedWhereClauses = strings.Replace(updatedWhereClauses, old, new, 1)
		paramCounter++
	}

	queryString := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		Table, strings.Join(setClauses, ", "), updatedWhereClauses)

	_, err := f.db.ExecContext(ctx, queryString, args...)
	if err != nil {
		f.logError("UpdateByParams", "Failed to execute", logger.H{
			"error":        err,
			"query_params": queryParams,
			"data":         data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (f *Facade) Delete(
	ctx context.Context,
	pk PrimaryKey,
) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE expense_id = $1", Table)

	_, err := f.db.ExecContext(ctx, query, pk.ExpenseID)
	if err != nil {
		f.logError("Delete", "Failed to execute", logger.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (f *Facade) GetIter(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
	callback func(*Data),
) error {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, args := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	rows, err := f.db.QueryContext(ctx, queryString, args...)
	if err != nil {
		f.logError("GetIter", "Failed to query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}
		callback(&data)
	}

	return nil
}

func (f *Facade) GetByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	if len(primaryKeys) == 0 {
		return []*Data{}, nil
	}

	// Create placeholders for IN clause
	placeholders := make([]string, len(primaryKeys))
	args := make([]interface{}, len(primaryKeys))
	for i, pk := range primaryKeys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = pk.ExpenseID
	}

	queryString := SelectQuery(fields)
	queryString += fmt.Sprintf(" WHERE expense_id IN (%s)", strings.Join(placeholders, ", "))

	rows, err := f.db.QueryContext(ctx, queryString, args...)
	if err != nil {
		f.logError("GetByPrimaryKeys", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeys", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeys(ctx, primaryKeys, allFieldsList)
}

func (f *Facade) GetByPrimaryKeysTx(
	ctx context.Context,
	tx *sql.Tx,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	if len(primaryKeys) == 0 {
		return []*Data{}, nil
	}

	// Create placeholders for IN clause
	placeholders := make([]string, len(primaryKeys))
	args := make([]interface{}, len(primaryKeys))
	for i, pk := range primaryKeys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = pk.ExpenseID
	}

	queryString := SelectQuery(fields)
	queryString += fmt.Sprintf(" WHERE expense_id IN (%s)", strings.Join(placeholders, ", "))

	rows, err := tx.QueryContext(ctx, queryString, args...)
	if err != nil {
		f.logError("GetByPrimaryKeysTx", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeysTx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeysTx(
	ctx context.Context,
	tx *sql.Tx,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeysTx(ctx, tx, primaryKeys, allFieldsList)
}

func (f *Facade) GetByPrimaryKeysIter(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	fields []Field,
	callback func(*Data),
) error {
	if len(primaryKeys) == 0 {
		return nil
	}

	// Create placeholders for IN clause
	placeholders := make([]string, len(primaryKeys))
	args := make([]interface{}, len(primaryKeys))
	for i, pk := range primaryKeys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = pk.ExpenseID
	}

	queryString := SelectQuery(fields)
	queryString += fmt.Sprintf(" WHERE expense_id IN (%s)", strings.Join(placeholders, ", "))

	rows, err := f.db.QueryContext(ctx, queryString, args...)
	if err != nil {
		f.logError("GetByPrimaryKeysIter", "Failed to query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeysIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	return nil
}

func (f *Facade) ListByPrimaryKeysIter(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	callback func(*Data),
) error {
	return f.GetByPrimaryKeysIter(ctx, primaryKeys, allFieldsList, callback)
}

func (f *Facade) List(
	ctx context.Context,
	queryParams []QueryParam,
) ([]*Data, error) {
	return f.Get(ctx, queryParams, allFieldsList)
}

func (f *Facade) ListIter(
	ctx context.Context,
	queryParams []QueryParam,
	callback func(*Data),
) error {
	return f.GetIter(ctx, queryParams, allFieldsList, callback)
}

type OperationRead struct {
	f         *Facade
	fields    []Field
	strFields []string
	stmt      string
	args      []interface{}
	tx        *sql.Tx
	qp        []QueryParam
	pk        *PrimaryKey
	pks       []PrimaryKey
	qb        *sql_builder.Builder[Field]
}

func (op *OperationRead) Exists(
	ctx context.Context,
) bool {
	if op.pk != nil {
		query := fmt.Sprintf(`SELECT 1 FROM %s WHERE expense_id = $1 LIMIT 1`, Table)
		var exists int
		if op.tx != nil {
			err := op.tx.QueryRowContext(ctx, query, op.pk.ExpenseID).Scan(&exists)
			return err == nil
		}
		err := op.f.db.QueryRowContext(ctx, query, op.pk.ExpenseID).Scan(&exists)
		return err == nil
	}
	return false
}

func (op *OperationRead) Columns(fields ...Field) *OperationRead {
	if len(fields) == 0 {
		return op
	}
	op.fields = fields
	return op
}

func (op *OperationRead) Params(queryParams []QueryParam) *OperationRead {
	op.qp = queryParams
	return op
}

func (op *OperationRead) Tx(tx *sql.Tx) *OperationRead {
	op.tx = tx
	return op
}

func (op *OperationRead) ByKeys(primaryKeys []PrimaryKey) *OperationRead {
	op.pks = primaryKeys
	return op
}

func convertColumns(fields []Field) []string {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}
	return stringFields
}

func (op *OperationRead) stringColumns() {
	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}
	op.strFields = convertColumns(op.fields)
}

func (op *OperationRead) Select(fields ...Field) *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(fields))
	op.fields = fields
	return op.qb
}

func (op *OperationRead) SelectAll() *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(nil))
	return op.qb
}

func (op *OperationRead) SelectCount(columns ...Field) *sql_builder.Builder[Field] {
	startQuery := strings.Builder{}
	startQuery.WriteString("SELECT COUNT(")
	if len(columns) == 0 {
		startQuery.WriteString("*")
	} else {
		startQuery.WriteString(strings.Join(makeStringFields(columns), ", "))
	}
	startQuery.WriteString(") FROM ")
	startQuery.WriteString(Table)
	op.qb = sql_builder.New[Field](startQuery.String())
	startQuery.Reset()
	return op.qb
}

func (op *OperationRead) GetCount(ctx context.Context) (int64, error) {
	if op.qb == nil {
		op.SelectCount()
	}

	var queryStr string
	var queryArgs []interface{}
	if op.qb != nil {
		queryStr = op.qb.StringPostgres()
		queryArgs = op.qb.ArgsPostgres()
	}

	var count int64
	var err error
	if op.tx != nil {
		err = op.tx.QueryRowContext(ctx, queryStr, queryArgs...).Scan(&count)
	} else {
		err = op.f.db.QueryRowContext(ctx, queryStr, queryArgs...).Scan(&count)
	}

	if err != nil {
		op.f.logError("GetCount", "Failed to Scan", logger.H{
			"error": err,
			"query": queryStr,
			"args":  queryArgs,
		})
		return 0, err
	}

	return count, nil
}

func (op *OperationRead) Rows(ctx context.Context) ([]*Data, error) {
	op.stringColumns()

	var rows *sql.Rows
	var err error

	if op.qb != nil {
		queryStr := op.qb.StringPostgres()
		queryArgs := op.qb.ArgsPostgres()
		if op.tx != nil {
			rows, err = op.tx.QueryContext(ctx, queryStr, queryArgs...)
		} else {
			rows, err = op.f.db.QueryContext(ctx, queryStr, queryArgs...)
		}
	} else if op.qp != nil {
		queryString := SelectQuery(op.fields)
		whereClauses, args := ConstructWhereClause(op.qp)
		if len(op.qp) > 0 {
			queryString += " WHERE " + whereClauses
		}
		if op.tx != nil {
			rows, err = op.tx.QueryContext(ctx, queryString, args...)
		} else {
			rows, err = op.f.db.QueryContext(ctx, queryString, args...)
		}
	} else if op.pks != nil {
		data, err := op.f.GetByPrimaryKeys(ctx, op.pks, op.fields)
		return data, err
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(op.fields)...); err != nil {
			return nil, err
		}
		res = append(res, &data)
	}

	return res, nil
}

func (op *OperationRead) SingleRow(
	ctx context.Context,
) (*Data, error) {
	if op.pk == nil {
		return nil, fmt.Errorf("primary key not set")
	}

	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}

	queryString := SelectQuery(op.fields)
	queryString += " WHERE expense_id = $1 LIMIT 1"

	var row *sql.Row
	if op.tx != nil {
		row = op.tx.QueryRowContext(ctx, queryString, op.pk.ExpenseID)
	} else {
		row = op.f.db.QueryRowContext(ctx, queryString, op.pk.ExpenseID)
	}

	var data Data
	err := row.Scan(data.fieldPtrs(op.fields)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		op.f.logError("SingleRow", "Failed to Scan", logger.H{
			"error":  err,
			"fields": op.fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) Read() *OperationRead {
	return &OperationRead{
		f: f,
	}
}
