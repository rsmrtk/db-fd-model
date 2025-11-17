package m_income

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rsmrtk/db-fd-model/m_options"
	"github.com/rsmrtk/db-fd-model/sql_builder"
	"github.com/rsmrtk/smartlg/logger"
)

const (
	Package = "m_income"
	Table   = "incomes"
	ID      = "income_id"
	// Secondary indexes
)

type Facade struct {
	log *logger.Logger
	db  *pgxpool.Pool
	//
}

func New(o *m_options.Options) *Facade {
	return &Facade{
		log: o.Log,
		db:  o.DB,
		//
	}
}

func (f *Facade) logError(functionName string, msg string, h logger.H) {
	f.log.Error(fmt.Sprintf("[%s.%s - %s] %s", Package, functionName, Table, msg), h)
}

type Data struct {
	IncomeID     string
	IncomeName   *string
	IncomeAmount *float64
	IncomeType   *string
	IncomeDate   *time.Time
	CreatedAt    *time.Time
}

func (data *Data) Map() map[string]any {
	out := make(map[string]any, len(allFieldsList))
	out[string(IncomeID)] = data.IncomeID
	out[string(IncomeName)] = data.IncomeName
	out[string(IncomeAmount)] = data.IncomeAmount
	out[string(IncomeType)] = data.IncomeType
	out[string(IncomeDate)] = data.IncomeDate
	out[string(CreatedAt)] = data.CreatedAt
	return out
}

type Field string

const (
	IncomeID     Field = "income_id"
	IncomeName   Field = "income_name"
	IncomeAmount Field = "income_amount"
	IncomeType   Field = "income_type"
	IncomeDate   Field = "income_date"
	CreatedAt    Field = "created_at"
)

type EnumType string

const (
	EnumTypeSalary   EnumType = "salary"
	EnumTypeTransfer EnumType = "transfer"
	EnumTypeOthers   EnumType = "others"
)

func (e EnumType) String() string {
	return string(e)
}

func (e *EnumType) IsValid() bool {
	switch *e {
	case EnumTypeSalary, EnumTypeTransfer, EnumTypeOthers:
		return true
	}
	return false
}

func GetAllFields() []Field {
	return []Field{
		IncomeID,
		IncomeName,
		IncomeAmount,
		IncomeType,
		IncomeDate,
		CreatedAt,
	}
}

var allFieldsList = GetAllFields()

func (f Field) String() string {
	return string(f)
}

var fieldsMap = map[Field]func(data *Data) interface{}{
	IncomeID:     func(data *Data) interface{} { return &data.IncomeID },
	IncomeName:   func(data *Data) interface{} { return &data.IncomeName },
	IncomeAmount: func(data *Data) interface{} { return &data.IncomeAmount },
	IncomeType:   func(data *Data) interface{} { return &data.IncomeType },
	IncomeDate:   func(data *Data) interface{} { return &data.IncomeDate },
	CreatedAt:    func(data *Data) interface{} { return &data.CreatedAt },
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	ptrs := make([]interface{}, len(fields))

	for i, field := range fields {
		ptrs[i] = fieldsMap[field](data)
	}
	return ptrs
}

type PrimaryKey struct {
	IncomeID string
}

func GetColumns() []string {
	return []string{
		IncomeID.String(),
		IncomeName.String(),
		IncomeAmount.String(),
		IncomeType.String(),
		IncomeDate.String(),
		CreatedAt.String(),
	}
}

var allStringFields = GetColumns()

func GetValues(data *Data) []interface{} {
	return []interface{}{
		data.IncomeID,
		data.IncomeName,
		data.IncomeAmount,
		data.IncomeType,
		data.IncomeDate,
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
		stringFields[i] = string(f)
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

func ConstructWhereClause(queryParams []QueryParam) (whereClause string, params map[string]interface{}) {
	whereClauses := make([]string, len(queryParams))
	params = make(map[string]interface{}, len(queryParams))
	builder := strings.Builder{}
	for i, qp := range queryParams {
		if (qp.Operator == OpIs || qp.Operator == OpIsNot) && qp.Value == nil {
			whereClauses[i] = fmt.Sprintf("%s %s NULL", qp.Field, qp.Operator)
			continue
		}
		builder.WriteString("param")
		builder.WriteString(strconv.Itoa(i))
		paramName := builder.String()
		builder.Reset()

		// Construct param - PostgreSQL uses $N syntax
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(i + 1))
		param := builder.String()
		builder.Reset()

		if qp.Operator == "IN" {
			// PostgreSQL uses = ANY($N) instead of IN UNNEST(@param)
			builder.WriteString("= ANY(")
			builder.WriteString(param)
			builder.WriteString(")")
			param = builder.String()
			builder.Reset()
		}

		// Construct whereClause
		builder.WriteString(string(qp.Field))
		builder.WriteString(" ")
		builder.WriteString(string(qp.Operator))
		builder.WriteString(" ")
		builder.WriteString(param)
		whereClause := builder.String()
		whereClauses[i] = whereClause
		params[paramName] = qp.Value
		builder.Reset()
	}

	return strings.Join(whereClauses, " AND "), params
}

func (f *Facade) CreateOrUpdate(
	ctx context.Context,
	data *Data,
) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (%s) DO UPDATE SET
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s
	`, Table,
		strings.Join(allStringFields, ", "),
		IncomeID,
		IncomeName, IncomeName,
		IncomeAmount, IncomeAmount,
		IncomeType, IncomeType,
		IncomeDate, IncomeDate,
		CreatedAt, CreatedAt,
	)

	_, err := f.db.Exec(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("CreateOrUpdate", "Failed to Exec", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Create(ctx context.Context, data *Data) error {
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)",
		Table, strings.Join(allStringFields, ", "))

	_, err := f.db.Exec(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("Create", "Failed to Exec", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Exists(
	ctx context.Context,
	incomeID string,
) bool {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", ID, Table, ID)
	var id string
	err := f.db.QueryRow(ctx, query, incomeID).Scan(&id)
	return err == nil
}

func (f *Facade) ExistsRtx(
	ctx context.Context,
	tx pgx.Tx,
	incomeID string,
) bool {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", ID, Table, ID)
	var id string
	err := tx.QueryRow(ctx, query, incomeID).Scan(&id)
	return err == nil
}

func (f *Facade) Get(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert map params to ordered slice
	args := make([]interface{}, len(params))
	for i := 0; i < len(params); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args[i] = params[paramName]
	}

	rows, err := f.db.Query(ctx, queryString, args...)
	if err != nil {
		f.logError("Get", "Failed to Query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetRtx(
	ctx context.Context,
	rtx pgx.Tx,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert map params to ordered slice
	args := make([]interface{}, len(params))
	for i := 0; i < len(params); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args[i] = params[paramName]
	}

	rows, err := rtx.Query(ctx, queryString, args...)
	if err != nil {
		f.logError("GetRtx", "Failed to Query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetRtx", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) Find(
	ctx context.Context,
	incomeID string,
	fields []Field,
) (*Data, error) {
	stringFields := makeStringFields(fields)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(stringFields, ", "), Table, IncomeID)

	var data Data
	err := f.db.QueryRow(ctx, query, incomeID).Scan(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("Find", "Failed to QueryRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) FindRtx(
	ctx context.Context,
	rtx pgx.Tx,
	incomeID string,
	fields []Field,
) (*Data, error) {
	stringFields := makeStringFields(fields)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(stringFields, ", "), Table, IncomeID)

	var data Data
	err := rtx.QueryRow(ctx, query, incomeID).Scan(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("FindRtx", "Failed to QueryRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) Retrieve(
	ctx context.Context,
	incomeID string,
) (*Data, error) {
	return f.Find(ctx, incomeID, allFieldsList)
}

func (f *Facade) RetrieveRtx(
	ctx context.Context,
	rtx pgx.Tx,
	incomeID string,
) (*Data, error) {
	return f.FindRtx(ctx, rtx, incomeID, allFieldsList)
}

func (f *Facade) CreateTx(
	ctx context.Context,
	tx pgx.Tx,
	data *Data,
) error {
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)",
		Table, strings.Join(allStringFields, ", "))

	_, err := tx.Exec(ctx, query, GetValues(data)...)
	if err != nil {
		f.logError("CreateTx", "Failed to Exec", logger.H{
			"error": err, "data": data,
		})
		return err
	}
	return nil
}

func (f *Facade) UpdateTx(
	ctx context.Context,
	tx pgx.Tx,
	incomeID string,
	data UpdateFields,
) error {
	if len(data) == 0 {
		return nil
	}

	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+1)
	paramIdx := 1

	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, paramIdx))
		args = append(args, value)
		paramIdx++
	}
	args = append(args, incomeID)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d",
		Table, strings.Join(setClauses, ", "), IncomeID, paramIdx)

	_, err := tx.Exec(ctx, query, args...)
	if err != nil {
		f.logError("UpdateTx", "Failed to Exec", logger.H{
			"error": err, "primaryKeys": map[string]interface{}{
				"income_id": incomeID,
			},
			"data": data,
		})
		return err
	}
	return nil
}

func (f *Facade) FindTx(
	ctx context.Context,
	tx pgx.Tx,
	incomeID string,
	fields []Field,
) (*Data, error) {
	stringFields := makeStringFields(fields)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(stringFields, ", "), Table, IncomeID)

	var data Data
	err := tx.QueryRow(ctx, query, incomeID).Scan(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("FindTx", "Failed to QueryRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}

	return &data, nil
}

func (c *Facade) GetByBuilder(ctx context.Context, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := c.db.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilder", "Failed to Query", logger.H{
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
			c.logError("GetByBuilder", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderRtx(ctx context.Context, rtx pgx.Tx, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := rtx.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilderRtx", "Failed to Query", logger.H{
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
			c.logError("GetByBuilderRtx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderTx(ctx context.Context, tx pgx.Tx, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := tx.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilderTx", "Failed to Query", logger.H{
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
			c.logError("GetByBuilderTx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderIter(ctx context.Context, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := c.db.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilderIter", "Failed to Query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderRtxIter(ctx context.Context, rtx pgx.Tx, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := rtx.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilderRtxIter", "Failed to Query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderRtxIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderTxIter(ctx context.Context, tx pgx.Tx, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	rows, err := tx.Query(ctx, queryStr, queryParams...)
	if err != nil {
		c.logError("GetByBuilderTxIter", "Failed to Query", logger.H{
			"error":  err,
			"fields": fields,
		})
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderTxIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		callback(&data)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (f *Facade) GetTx(
	ctx context.Context,
	tx pgx.Tx,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	q := SelectQuery(fields)
	whereClause, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		q += " WHERE " + whereClause
	}

	// Convert map params to ordered slice
	args := make([]interface{}, len(params))
	for i := 0; i < len(params); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args[i] = params[paramName]
	}

	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		f.logError("GetTx", "Failed to Query", logger.H{
			"error":       err,
			"queryParams": queryParams,
			"fields":      fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetTx", "Failed to Scan", logger.H{
				"error":       err,
				"queryParams": queryParams,
				"fields":      fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ExistTx(
	ctx context.Context,
	tx pgx.Tx,
	incomeID string,
) bool {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", ID, Table, ID)
	var id string
	err := tx.QueryRow(ctx, query, incomeID).Scan(&id)
	return err == nil
}

func (c *Facade) InitBuilder() *sql_builder.Builder[Field] {
	b := sql_builder.New[Field]("")
	b.Select(allFieldsList...).From(Table)
	return b
}

func (f *Facade) Update(
	ctx context.Context,
	incomeID string,
	data UpdateFields,
) error {
	if len(data) == 0 {
		return nil
	}

	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+1)
	paramIdx := 1

	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, paramIdx))
		args = append(args, value)
		paramIdx++
	}
	args = append(args, incomeID)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d",
		Table, strings.Join(setClauses, ", "), IncomeID, paramIdx)

	_, err := f.db.Exec(ctx, query, args...)
	if err != nil {
		f.logError("Update", "Failed to Exec", logger.H{
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
	if len(data) == 0 {
		return nil
	}

	// Construct WHERE clause
	whereClauses, whereParams := ConstructWhereClause(queryParams)

	// Build SET clause
	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+len(whereParams))
	paramIdx := 1

	// Add SET parameters
	for field, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, paramIdx))
		args = append(args, value)
		paramIdx++
	}

	// Add WHERE parameters
	for i := 0; i < len(whereParams); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args = append(args, whereParams[paramName])
	}

	// Adjust WHERE clause to use correct parameter numbers
	adjustedWhere := whereClauses
	for i := len(data); i > 0; i-- {
		adjustedWhere = strings.ReplaceAll(adjustedWhere,
			fmt.Sprintf("$%d", i),
			fmt.Sprintf("$%d", i+len(data)))
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		Table, strings.Join(setClauses, ", "), adjustedWhere)

	_, err := f.db.Exec(ctx, query, args...)
	if err != nil {
		f.logError("UpdateByParams", "Failed to Exec", logger.H{
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
	incomeID string,
) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", Table, IncomeID)

	_, err := f.db.Exec(ctx, query, incomeID)
	if err != nil {
		f.logError("Delete", "Failed to Exec", logger.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (f *Facade) GetRtxIter(
	ctx context.Context,
	rtx pgx.Tx,
	queryParams []QueryParam,
	fields []Field,
	callback func(*Data),
) error {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert map params to ordered slice
	args := make([]interface{}, len(params))
	for i := 0; i < len(params); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args[i] = params[paramName]
	}

	rows, err := rtx.Query(ctx, queryString, args...)
	if err != nil {
		f.logError("GetRtxIter", "Failed to Query", logger.H{
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
			f.logError("GetRtxIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}
		callback(&data)
	}

	if err := rows.Err(); err != nil {
		return err
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
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert map params to ordered slice
	args := make([]interface{}, len(params))
	for i := 0; i < len(params); i++ {
		paramName := fmt.Sprintf("param%d", i)
		args[i] = params[paramName]
	}

	rows, err := f.db.Query(ctx, queryString, args...)
	if err != nil {
		f.logError("GetIter", "Failed to Query", logger.H{
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

	if err := rows.Err(); err != nil {
		return err
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

	stringFields := makeStringFields(fields)
	incomeIDs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		incomeIDs[i] = pk.IncomeID
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ANY($1)",
		strings.Join(stringFields, ", "), Table, IncomeID)

	rows, err := f.db.Query(ctx, query, incomeIDs)
	if err != nil {
		f.logError("GetByPrimaryKeys", "Failed to Query", logger.H{
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeys(ctx, primaryKeys, allFieldsList)
}

func (f *Facade) GetByPrimaryKeysRtx(
	ctx context.Context,
	rtx pgx.Tx,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	if len(primaryKeys) == 0 {
		return []*Data{}, nil
	}

	stringFields := makeStringFields(fields)
	incomeIDs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		incomeIDs[i] = pk.IncomeID
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ANY($1)",
		strings.Join(stringFields, ", "), Table, IncomeID)

	rows, err := rtx.Query(ctx, query, incomeIDs)
	if err != nil {
		f.logError("GetByPrimaryKeysRtx", "Failed to Query", logger.H{
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
			f.logError("GetByPrimaryKeysRtx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetByPrimaryKeysTx(
	ctx context.Context,
	tx pgx.Tx,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	if len(primaryKeys) == 0 {
		return []*Data{}, nil
	}

	stringFields := makeStringFields(fields)
	incomeIDs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		incomeIDs[i] = pk.IncomeID
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ANY($1)",
		strings.Join(stringFields, ", "), Table, IncomeID)

	rows, err := tx.Query(ctx, query, incomeIDs)
	if err != nil {
		f.logError("GetByPrimaryKeysTx", "Failed to Query", logger.H{
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeysRtx(
	ctx context.Context,
	rtx pgx.Tx,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeysRtx(ctx, rtx, primaryKeys, allFieldsList)
}

func (f *Facade) ListByPrimaryKeysTx(
	ctx context.Context,
	tx pgx.Tx,
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

	stringFields := makeStringFields(fields)
	incomeIDs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		incomeIDs[i] = pk.IncomeID
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ANY($1)",
		strings.Join(stringFields, ", "), Table, IncomeID)

	rows, err := f.db.Query(ctx, query, incomeIDs)
	if err != nil {
		f.logError("GetByPrimaryKeysIter", "Failed to Query", logger.H{
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

	if err := rows.Err(); err != nil {
		return err
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

func (f *Facade) ListRtx(
	ctx context.Context,
	rtx pgx.Tx,
	queryParams []QueryParam,
) ([]*Data, error) {
	return f.GetRtx(ctx, rtx, queryParams, allFieldsList)
}

func (f *Facade) ListIter(
	ctx context.Context,
	queryParams []QueryParam,
	callback func(*Data),
) error {
	return f.GetIter(ctx, queryParams, allFieldsList, callback)
}

func (f *Facade) ListRtxIter(
	ctx context.Context,
	rtx pgx.Tx,
	queryParams []QueryParam,
	callback func(*Data),
) error {
	return f.GetRtxIter(ctx, rtx, queryParams, allFieldsList, callback)
}

type readtype string

const (
	byKeys    readtype = "byKeys"
	byRange   readtype = "byRange"
	byQuery   readtype = "byQuery"
	byParams  readtype = "byParams"
	byIndex   readtype = "byIndex"
	byIndexes readtype = "byIndexes"
	byBuilder readtype = "byBuilder"
	byCounter readtype = "byCounter"
)

type OperationRead struct {
	f         *Facade
	fields    []Field
	strFields []string
	query     string
	args      []interface{}
	readtype  readtype
	rtx       pgx.Tx
	tx        pgx.Tx
	params    []interface{}
	qp        []QueryParam
	qb        *sql_builder.Builder[Field]
}

func (op *OperationRead) Exists(
	ctx context.Context,
	incomeID string,
) bool {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", ID, Table, ID)
	var id string
	var err error
	if op.rtx != nil {
		err = op.rtx.QueryRow(ctx, query, incomeID).Scan(&id)
	} else {
		err = op.f.db.QueryRow(ctx, query, incomeID).Scan(&id)
	}
	return err == nil
}

func (op *OperationRead) Columns(fields ...Field) *OperationRead {
	if len(fields) == 0 {
		return op
	}
	op.fields = fields
	return op
}

func (op *OperationRead) Params(queryParams []QueryParam) *OperationRead {
	op.readtype = byParams
	op.qp = queryParams
	return op
}

func (op *OperationRead) Query(query string, args ...interface{}) *OperationRead {
	op.readtype = byQuery
	op.query = query
	op.args = args
	return op
}

func (op *OperationRead) Rtx(rtx pgx.Tx) *OperationRead {
	op.rtx = rtx
	return op
}

func (op *OperationRead) Tx(tx pgx.Tx) *OperationRead {
	op.tx = tx
	return op
}

func (op *OperationRead) ByKeys(primaryKeys []PrimaryKey) *OperationRead {
	incomeIDs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		incomeIDs[i] = pk.IncomeID
	}
	op.readtype = byKeys
	op.args = []interface{}{incomeIDs}
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

func (op *OperationRead) primaryColumns() {
	if len(op.fields) == 0 {
		op.fields = []Field{
			IncomeID,
		}
	}
	op.strFields = convertColumns(op.fields)
}

func (op *OperationRead) Select(fields ...Field) *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(fields))
	op.readtype = byBuilder
	op.fields = fields
	return op.qb
}

func (op *OperationRead) SelectAll() *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(nil))
	op.readtype = byBuilder
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
	op.readtype = byCounter
	startQuery.Reset()
	return op.qb
}

func (op *OperationRead) GetCount(ctx context.Context) (int64, error) {
	if op.qb == nil {
		op.SelectCount()
	}

	queryStr := op.qb.String()
	queryParams := op.qb.Params()

	var count int64
	var err error
	if op.rtx != nil {
		err = op.rtx.QueryRow(ctx, queryStr, queryParams...).Scan(&count)
	} else {
		err = op.f.db.QueryRow(ctx, queryStr, queryParams...).Scan(&count)
	}

	if err != nil {
		op.f.logError("GetCount", "Failed to Scan", logger.H{
			"error": err,
			"query": queryStr,
			"param": queryParams,
		})
		return 0, err
	}

	return count, nil
}

func (op *OperationRead) Rows(ctx context.Context) ([]*Data, error) {
	op.stringColumns()

	var rows pgx.Rows
	var err error

	switch op.readtype {
	case byKeys:
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ANY($1)",
			strings.Join(op.strFields, ", "), Table, IncomeID)
		if op.rtx != nil {
			rows, err = op.rtx.Query(ctx, query, op.args...)
		} else {
			rows, err = op.f.db.Query(ctx, query, op.args...)
		}
	case byQuery:
		if op.rtx != nil {
			rows, err = op.rtx.Query(ctx, op.query, op.args...)
		} else {
			rows, err = op.f.db.Query(ctx, op.query, op.args...)
		}
	case byBuilder:
		queryStr := op.qb.String()
		queryParams := op.qb.Params()
		if op.rtx != nil {
			rows, err = op.rtx.Query(ctx, queryStr, queryParams...)
		} else {
			rows, err = op.f.db.Query(ctx, queryStr, queryParams...)
		}
	case byParams:
		queryString := SelectQuery(op.fields)
		whereClauses, params := ConstructWhereClause(op.qp)
		if op.qp != nil && len(op.qp) > 0 {
			queryString += " WHERE " + whereClauses
		}
		args := make([]interface{}, len(params))
		for i := 0; i < len(params); i++ {
			paramName := fmt.Sprintf("param%d", i)
			args[i] = params[paramName]
		}
		if op.rtx != nil {
			rows, err = op.rtx.Query(ctx, queryString, args...)
		} else {
			rows, err = op.f.db.Query(ctx, queryString, args...)
		}
	case byCounter:
		return nil, fmt.Errorf("OperationRead Rows: byCounter is not supported. Use GetCount instead")
	default:
		return nil, fmt.Errorf("unsupported read type: %s", op.readtype)
	}

	if err != nil {
		op.f.logError("OperationRead Rows", "Failed to Query", logger.H{
			"error":        err,
			"query_params": op.params,
			"fields":       op.fields,
		})
		return nil, err
	}
	defer rows.Close()

	var res []*Data
	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(op.fields)...); err != nil {
			op.f.logError("OperationRead Rows", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": op.params,
				"fields":       op.fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (op *OperationRead) DoIter(ctx context.Context, callback func(*Data)) error {
	rows, err := op.Rows(ctx)
	if err != nil {
		return err
	}

	for _, row := range rows {
		callback(row)
	}

	return nil
}

func (op *OperationRead) SingleRow(
	ctx context.Context,
	incomeID string,
) (*Data, error) {
	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}

	stringFields := convertColumns(op.fields)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(stringFields, ", "), Table, IncomeID)

	var data Data
	var err error
	if op.rtx != nil {
		err = op.rtx.QueryRow(ctx, query, incomeID).Scan(data.fieldPtrs(op.fields)...)
	} else {
		err = op.f.db.QueryRow(ctx, query, incomeID).Scan(data.fieldPtrs(op.fields)...)
	}

	if err != nil {
		op.f.logError("SingleRow", "Failed to QueryRow", logger.H{
			"error":     err,
			"fields":    op.fields,
			"income_id": incomeID,
		})
		return nil, err
	}

	return &data, nil
}

type OperationWrite struct {
	f       *Facade
	creates []*Data
	updates []updateOp
	deletes []string
	puts    []*Data
}

type updateOp struct {
	incomeID string
	data     UpdateFields
}

func (op *OperationWrite) Update(
	incomeID string,
	data UpdateFields,
) *OperationWrite {
	op.updates = append(op.updates, updateOp{
		incomeID: incomeID,
		data:     data,
	})
	return op
}

func (op *OperationWrite) Create(data *Data) *OperationWrite {
	op.creates = append(op.creates, data)
	return op
}

func (op *OperationWrite) Put(data *Data) *OperationWrite {
	op.puts = append(op.puts, data)
	return op
}

func (op *OperationWrite) Apply(ctx context.Context) error {
	tx, err := op.f.db.Begin(ctx)
	if err != nil {
		op.f.logError("OperationWrite Apply", "Failed to Begin transaction", logger.H{
			"error": err,
		})
		return err
	}
	defer tx.Rollback(ctx)

	// Execute creates
	for _, data := range op.creates {
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6)",
			Table, strings.Join(allStringFields, ", "))
		_, err := tx.Exec(ctx, query, GetValues(data)...)
		if err != nil {
			op.f.logError("OperationWrite Apply", "Failed to insert", logger.H{
				"error": err,
				"data":  data,
			})
			return err
		}
	}

	// Execute puts (upserts)
	for _, data := range op.puts {
		query := fmt.Sprintf(`
			INSERT INTO %s (%s)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (%s) DO UPDATE SET
				%s = EXCLUDED.%s,
				%s = EXCLUDED.%s,
				%s = EXCLUDED.%s,
				%s = EXCLUDED.%s,
				%s = EXCLUDED.%s
		`, Table,
			strings.Join(allStringFields, ", "),
			IncomeID,
			IncomeName, IncomeName,
			IncomeAmount, IncomeAmount,
			IncomeType, IncomeType,
			IncomeDate, IncomeDate,
			CreatedAt, CreatedAt,
		)
		_, err := tx.Exec(ctx, query, GetValues(data)...)
		if err != nil {
			op.f.logError("OperationWrite Apply", "Failed to upsert", logger.H{
				"error": err,
				"data":  data,
			})
			return err
		}
	}

	// Execute updates
	for _, update := range op.updates {
		if len(update.data) == 0 {
			continue
		}

		setClauses := make([]string, 0, len(update.data))
		args := make([]interface{}, 0, len(update.data)+1)
		paramIdx := 1

		for field, value := range update.data {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, paramIdx))
			args = append(args, value)
			paramIdx++
		}
		args = append(args, update.incomeID)

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d",
			Table, strings.Join(setClauses, ", "), IncomeID, paramIdx)

		_, err := tx.Exec(ctx, query, args...)
		if err != nil {
			op.f.logError("OperationWrite Apply", "Failed to update", logger.H{
				"error":     err,
				"income_id": update.incomeID,
				"data":      update.data,
			})
			return err
		}
	}

	// Execute deletes
	for _, incomeID := range op.deletes {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", Table, IncomeID)
		_, err := tx.Exec(ctx, query, incomeID)
		if err != nil {
			op.f.logError("OperationWrite Apply", "Failed to delete", logger.H{
				"error":     err,
				"income_id": incomeID,
			})
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		op.f.logError("OperationWrite Apply", "Failed to Commit transaction", logger.H{
			"error": err,
		})
		return err
	}

	return nil
}

func (op *OperationWrite) Delete(
	incomeID string,
) *OperationWrite {
	op.deletes = append(op.deletes, incomeID)
	return op
}

func (f *Facade) Read() *OperationRead {
	return &OperationRead{
		f:        f,
		readtype: byParams,
	}
}

func (f *Facade) Write() *OperationWrite {
	return &OperationWrite{
		f: f,
	}
}
