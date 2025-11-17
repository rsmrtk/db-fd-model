package m_income

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
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
	db  *spanner.Client
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
	IncomeName   spanner.NullString
	IncomeAmount spanner.NullNumeric
	IncomeType   spanner.NullString
	IncomeDate   spanner.NullTime
	CreatedAt    spanner.NullTime
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
		stringFields[i] = "`" + string(f) + "`"
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

		// Construct param
		builder.WriteString("@")
		builder.WriteString(paramName)
		param := builder.String()
		builder.Reset()

		if qp.Operator == "IN" {
			builder.WriteString("UNNEST(")
			builder.WriteString(param)
			builder.WriteString(")")
			param = builder.String()
			builder.Reset()
		}

		// Construct whereClause
		builder.WriteString("`")
		builder.WriteString(string(qp.Field))
		builder.WriteString("`")
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

func (f *Facade) CreateMut(data *Data) *spanner.Mutation {
	return spanner.Insert(Table, GetColumns(), GetValues(data))
}

func (f *Facade) CreateOrUpdateMut(data *Data) *spanner.Mutation {
	return spanner.InsertOrUpdate(Table, GetColumns(), GetValues(data))
}

func (f *Facade) CreateOrUpdate(
	ctx context.Context,
	data *Data,
) error {
	mutation := f.CreateOrUpdateMut(data)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("CreateOrUpdate", "Failed to Apply", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Create(ctx context.Context, data *Data) error {
	mutation := f.CreateMut(data)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("Create", "Failed to Apply", logger.H{
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
	_, err := f.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		[]string{string(ID)},
	)
	return err == nil
}

func (f *Facade) ExistsRtx(
	ctx context.Context,
	tx *spanner.ReadOnlyTransaction,
	incomeID string,
) bool {
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		[]string{string(ID)},
	)
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

	stmt := spanner.Statement{
		SQL:    queryString,
		Params: params,
	}

	iter := f.db.Single().Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("Get", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	stmt := spanner.Statement{
		SQL:    queryString,
		Params: params,
	}
	iter := rtx.Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("Get", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) Find(
	ctx context.Context,
	incomeID string,
	fields []Field,
) (*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	row, err := f.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		stringFields,
	)
	if err != nil {
		f.logError("Find", "Failed to ReadRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("Find", "Failed to Scan", logger.H{
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
	rtx *spanner.ReadOnlyTransaction,
	incomeID string,
	fields []Field,
) (*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	row, err := rtx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		stringFields,
	)
	if err != nil {
		f.logError("Find", "Failed to ReadRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("Find", "Failed to Scan", logger.H{
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
	rtx *spanner.ReadOnlyTransaction,
	incomeID string,
) (*Data, error) {
	return f.FindRtx(ctx, rtx, incomeID, allFieldsList)
}

func (f *Facade) CreateTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	data *Data,
) error {
	mut := spanner.Insert(Table, GetColumns(), GetValues(data))

	if err := tx.BufferWrite([]*spanner.Mutation{mut}); err != nil {
		f.logError("CreateTx", "Failed to BufferWrite", logger.H{
			"error": err, "data": data,
		})
		return err
	}
	return nil
}

func (f *Facade) UpdateTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	incomeID string,
	data UpdateFields,
) error {
	mut := f.UpdateMut(
		incomeID,
		data,
	)

	if err := tx.BufferWrite([]*spanner.Mutation{mut}); err != nil {
		f.logError("UpdateTx", "Failed to BufferWrite", logger.H{
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
	tx *spanner.ReadWriteTransaction,
	incomeID string,
	fields []Field,
) (*Data, error) {
	strCols := make([]string, len(fields))
	for i, fld := range fields {
		strCols[i] = string(fld)
	}

	row, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		strCols,
	)
	if err != nil {
		f.logError("FindTx", "Failed to ReadRow", logger.H{
			"error":     err,
			"income_id": incomeID,
			"fields":    fields,
		})
		return nil, err
	}
	var d Data
	if err := row.Columns(d.fieldPtrs(fields)...); err != nil {
		f.logError("FindTx", "Failed to Scan", logger.H{
			"error":  err,
			"fields": fields,
		})
		return nil, err
	}
	return &d, nil
}

func (c *Facade) GetByBuilder(ctx context.Context, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	iter := c.db.Single().Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilder", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderRtx(ctx context.Context, rtx *spanner.ReadOnlyTransaction, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	iter := rtx.Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderRtx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderTx(ctx context.Context, tx *spanner.ReadWriteTransaction, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	iter := tx.Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderTx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
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

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := c.db.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderRtxIter(ctx context.Context, rtx *spanner.ReadOnlyTransaction, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := rtx.Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderRtxIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderTxIter(ctx context.Context, tx *spanner.ReadWriteTransaction, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := tx.Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderTxIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) GetTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	q := SelectQuery(fields)
	whereClause, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		q += " WHERE " + whereClause
	}
	stmt := spanner.Statement{SQL: q, Params: params}

	iter := tx.Query(ctx, stmt)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var d Data
		if err := row.Columns(d.fieldPtrs(fields)...); err != nil {
			f.logError("GetTx", "Failed to Scan", logger.H{
				"error":       err,
				"queryParams": queryParams,
				"fields":      fields,
			})
			return err
		}
		res = append(res, &d)
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ExistTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	incomeID string,
) bool {
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		[]string{string(ID)},
	)
	return err == nil
}

func (c *Facade) InitBuilder() *sql_builder.Builder[Field] {
	b := sql_builder.New[Field]("")
	b.Select(allFieldsList...).From(Table)
	return b
}

func (f *Facade) UpdateMut(
	incomeID string,
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
		IncomeID.String(): incomeID,
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.UpdateMap(Table, mutationData)
}

func (f *Facade) Update(
	ctx context.Context,
	incomeID string,
	data UpdateFields,
) error {
	mutation := f.UpdateMut(
		incomeID,
		data,
	)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("Update", "Failed to Apply", logger.H{
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
	whereClauses, params := ConstructWhereClause(queryParams)
	builder := strings.Builder{}
	setClauses := make([]string, 0, len(data))
	// Construct SET clause
	for field, value := range data {
		builder.WriteString(string(field))
		paramName := builder.String()
		builder.Reset()

		// Construct param
		builder.WriteString("@")
		builder.WriteString(paramName)
		param := builder.String()
		builder.Reset()

		// Construct SET clause
		builder.WriteString("`")
		builder.WriteString(string(field))
		builder.WriteString("` = ")
		builder.WriteString(param)
		setClause := builder.String()
		setClauses = append(setClauses, setClause)
		params[paramName] = value
	}

	queryString := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		Table, strings.Join(setClauses, ", "), whereClauses)

	stmt := spanner.Statement{
		SQL:    queryString,
		Params: params,
	}

	if _, err := f.db.PartitionedUpdate(ctx, stmt); err != nil {
		f.logError("UpdateByParams", "Failed to PartitionedUpdate", logger.H{
			"error":        err,
			"query_params": queryParams,
			"data":         data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (f *Facade) DeleteMut(
	incomeID string,
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		incomeID,
	})
}

func (f *Facade) Delete(
	ctx context.Context,
	incomeID string,
) error {
	mutation := f.DeleteMut(
		incomeID,
	)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("Delete", "Failed to Apply", logger.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (f *Facade) GetRtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
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

	if err := rtx.Query(ctx, spanner.Statement{
		SQL:    queryString,
		Params: params,
	}).Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetRtxIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
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

	if err := f.db.Single().Query(ctx, spanner.Statement{
		SQL:    queryString,
		Params: params,
	}).Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) GetByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			pk.IncomeID,
		}
	}

	iter := f.db.Single().Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeys", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
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
	rtx *spanner.ReadOnlyTransaction,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			pk.IncomeID,
		}
	}

	iter := rtx.Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeys", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetByPrimaryKeysTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, fld := range fields {
		stringFields[i] = string(fld)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			pk.IncomeID,
		}
	}

	iter := tx.Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeysTx", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}
		res = append(res, &data)
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeysRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeysRtx(ctx, rtx, primaryKeys, allFieldsList)
}

func (f *Facade) ListByPrimaryKeysTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
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
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			pk.IncomeID,
		}
	}

	if err := f.db.Single().Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeysIter", "Failed to Scan", logger.H{
				"error":  err,
				"fields": fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
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
	rtx *spanner.ReadOnlyTransaction,
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
	rtx *spanner.ReadOnlyTransaction,
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
	stmt      spanner.Statement
	readtype  readtype
	rtx       *spanner.ReadOnlyTransaction
	tx        *spanner.ReadWriteTransaction
	spk       []spanner.Key
	keyrange  spanner.KeyRange
	params    []interface{}
	qp        []QueryParam
	singleKey spanner.Key
	keyList   []spanner.Key
	indxName  string
	qb        *sql_builder.Builder[Field]
}

func (op *OperationRead) Exists(
	ctx context.Context,
	incomeID string,
) bool {
	tx := op.rtx
	if tx == nil {
		tx = op.f.db.Single()
	}
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			incomeID,
		},
		[]string{string(ID)},
	)
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

func (op *OperationRead) Query(stmt spanner.Statement) *OperationRead {
	op.readtype = byQuery
	op.stmt = stmt
	return op
}

func (op *OperationRead) Rtx(rtx *spanner.ReadOnlyTransaction) *OperationRead {
	op.rtx = rtx
	return op
}

func (op *OperationRead) Tx(tx *spanner.ReadWriteTransaction) *OperationRead {
	op.tx = tx
	return op
}

func (op *OperationRead) ByKeys(primaryKeys []PrimaryKey) *OperationRead {
	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			pk.IncomeID,
		}
	}
	op.readtype = byKeys
	op.spk = spk
	return op
}

// If you do not provide columns via Columns method, by default it will select only columns which are primary keys
func (op *OperationRead) ByIndexKeyList(indexName string, keys []spanner.Key) *OperationRead {
	op.readtype = byIndexes
	op.indxName = indexName
	op.keyList = keys
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
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	iter := op.rtx.Query(ctx, spanner.Statement{
		SQL:    op.qb.String(),
		Params: op.qb.Params(),
	})
	defer iter.Stop()

	var count int64
	if err := iter.Do(func(row *spanner.Row) error {
		if err := row.Columns(&count); err != nil {
			op.f.logError("GetCount", "Failed to Scan", logger.H{
				"error": err,
				"query": op.qb.String(),
				"param": op.qb.Params(),
			})
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return count, nil
}

func (op *OperationRead) Iterator(ctx context.Context) *spanner.RowIterator {
	defer op.qb.Reset()
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	switch op.readtype {
	case byKeys:
		op.params = []interface{}{op.spk}
		op.stringColumns()
		return op.rtx.Read(ctx, Table, spanner.KeySetFromKeys(op.spk...), op.strFields)
	case byRange:
		op.params = []interface{}{op.keyrange}
		op.stringColumns()
		return op.rtx.Read(ctx, Table, op.keyrange, op.strFields)
	case byQuery:
		op.params = []interface{}{op.stmt}
		op.stringColumns()
		return op.rtx.Query(ctx, op.stmt)
	case byIndex:
		op.params = []interface{}{op.singleKey}
		op.stringColumns()
		return op.rtx.ReadUsingIndex(ctx, Table, op.indxName, op.singleKey, op.strFields)
	case byIndexes:
		op.params = []interface{}{op.keyList}
		op.primaryColumns()
		return op.rtx.ReadUsingIndex(ctx, Table, op.indxName, spanner.KeySetFromKeys(op.keyList...), op.strFields)
	case byBuilder:
		op.stringColumns()
		op.params = []interface{}{op.qb.Params()}
		return op.rtx.Query(ctx, spanner.Statement{
			SQL:    op.qb.String(),
			Params: op.qb.Params(),
		})
	case byParams:
		op.stringColumns()
		queryString := SelectQuery(op.fields)
		whereClauses, params := ConstructWhereClause(op.qp)
		if op.qp != nil && len(op.qp) > 0 {
			queryString += " WHERE " + whereClauses
		}
		op.params = []interface{}{op.qp}
		return op.rtx.Query(ctx, spanner.Statement{
			SQL:    queryString,
			Params: params,
		})
	case byCounter:
		panic("OperationRead Iterator: byCounter is not supported. Use GetCount instead")
	default:
		return nil
	}
}

func (op *OperationRead) DoIter(ctx context.Context, callback func(*Data)) error {
	withLog := func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(op.fields)...); err != nil {
			op.f.logError("OperationRead DoIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": op.params,
				"fields":       op.fields,
			})
			return err
		}

		callback(&data)

		return nil
	}

	if err := op.Iterator(ctx).Do(withLog); err != nil {
		return err
	}

	return nil
}

func (op *OperationRead) Rows(ctx context.Context) ([]*Data, error) {
	iter := op.Iterator(ctx)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(op.fields)...); err != nil {
			op.f.logError("OperationRead Rows", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": op.params,
				"fields":       op.fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (op *OperationRead) SingleRow(
	ctx context.Context,
	incomeID string,
) (*Data, error) {
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}
	row, err := op.rtx.ReadRow(ctx, Table,
		spanner.Key{
			incomeID,
		},
		convertColumns(op.fields),
	)
	if err != nil {
		op.f.logError("ReadRow", "Failed to ReadRow", logger.H{
			"error":     err,
			"fields":    op.fields,
			"income_id": incomeID,
		})
		return nil, err
	}

	var data Data
	err = row.Columns(data.fieldPtrs(op.fields)...)
	if err != nil {
		op.f.logError("ReadRow", "Failed to Scan", logger.H{
			"error":  err,
			"fields": op.fields,
		})
		return nil, err
	}

	return &data, nil
}

type Muts struct {
}

var mutations = &Muts{}

func (op *Muts) Create(data *Data) *spanner.Mutation {
	return spanner.Insert(Table, allStringFields, GetValues(data))
}

// Put is an alias for spanner.InsertOrUpdate
func (op Muts) Put(data *Data) *spanner.Mutation {
	return spanner.InsertOrUpdate(Table, allStringFields, GetValues(data))
}

func (op *Muts) Delete(
	incomeID string,
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		incomeID,
	})
}

func (op *Muts) Update(
	incomeID string,
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
		IncomeID.String(): incomeID,
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.UpdateMap(Table, mutationData)
}

type OperationWrite struct {
	f    *Facade
	muts []*spanner.Mutation
}

func (op *OperationWrite) apply(ctx context.Context, muts []*spanner.Mutation) error {
	if _, err := op.f.db.Apply(ctx, muts); err != nil {
		op.f.logError("OperationWrite Apply", "Failed to Apply", logger.H{
			"error": err,
			"muts":  muts,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (op *OperationWrite) Update(
	incomeID string,
	data UpdateFields,
) *OperationWrite {
	op.muts = append(
		op.muts,
		mutations.Update(
			incomeID,
			data,
		),
	)

	return op
}

func (op *OperationWrite) Create(data *Data) *OperationWrite {
	op.muts = append(op.muts, mutations.Create(data))
	return op
}

// Put is an alias for spanner.InsertOrUpdate
func (op *OperationWrite) Put(data *Data) *OperationWrite {
	op.muts = append(op.muts, mutations.Put(data))
	return op
}

func (op *OperationWrite) Apply(ctx context.Context) error {
	return op.apply(ctx, op.muts)
}

func (op *OperationWrite) Delete(
	incomeID string,
) *OperationWrite {
	op.muts = append(op.muts, mutations.Delete(
		incomeID,
	))

	return op
}

func (op *OperationWrite) GetMuts() []*spanner.Mutation {
	return op.muts
}

func Mut() *Muts {
	return mutations
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

func (f *Facade) Muts() *Muts {
	return mutations
}
