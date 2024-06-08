package mysql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-yaaf/yaaf-common/database"
)

// region Query helper Methods -----------------------------------------------------------------------------------------

// Build SQL statement with sql arguments based on the query data
func (s *mSqlDatabaseQuery) buildStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id, data FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build SQL count statement with sql arguments based on the query data
// supported aggregations: count, sum, avg, min, max
func (s *mSqlDatabaseQuery) buildCountStatement(field, function string, keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("(data->>'%s')::FLOAT", field)
	}
	SQL = fmt.Sprintf(`SELECT %s(%s) as aggr FROM "%s" %s`, function, aggr, tblName, where)
	return
}

// Build SQL statement with sql arguments based on the query data
func (s *mSqlDatabaseQuery) buildIdStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build SQL statement with sql arguments based on the query data
func (s *mSqlDatabaseQuery) buildCriteria() (where string, args []any) {
	parts := make([]string, 0, 0)
	varIndex := 1

	// Initialize match all (AND) conditions
	for _, list := range s.allFilters {
		for _, fq := range list {
			part, partArgs := s.buildFilter(fq, varIndex)
			if len(part) > 0 {
				parts = append(parts, part)
				if partArgs != nil {
					args = append(args, partArgs...)
					varIndex += len(partArgs)
				}
			}
		}
	}

	// Initialize match any (OR) conditions
	for _, list := range s.anyFilters {
		orParts := make([]string, 0, 0)
		for _, fq := range list {
			part, partArgs := s.buildFilter(fq, varIndex)
			if len(part) > 0 {
				orParts = append(orParts, part)
				if partArgs != nil {
					args = append(args, partArgs...)
					varIndex += len(partArgs)
				}
			}
		}

		// If range is defined, add it to the filters
		if len(s.rangeField) > 0 {
			rangeFilter := []database.QueryFilter{database.F(s.rangeField).Between(s.rangeFrom, s.rangeTo)}
			s.allFilters = append(s.allFilters, rangeFilter)
		}

		if len(orParts) > 0 {
			orConditions := fmt.Sprintf("(%s)", strings.Join(orParts, " OR "))
			parts = append(parts, orConditions)
		}
	}

	if len(parts) > 0 {
		where = fmt.Sprintf("WHERE %s", strings.Join(parts, " AND "))
	}

	return
}

// Build order clause based on the query data
func (s *mSqlDatabaseQuery) buildOrder() string {

	l := len(s.ascOrders) + len(s.descOrders)
	if l == 0 {
		return ""
	}
	fields := make([]string, 0, l)
	for _, field := range s.ascOrders {
		if field == "id" {
			fields = append(fields, fmt.Sprintf("id ASC"))
		} else {
			fields = append(fields, fmt.Sprintf("data->>'%s' ASC", field))
		}
	}
	for _, field := range s.descOrders {
		if field == "id" {
			fields = append(fields, fmt.Sprintf("id DESC"))
		} else {
			fields = append(fields, fmt.Sprintf("data->>'%s' DESC", field))
		}
	}

	order := fmt.Sprintf("ORDER BY %s", strings.Join(fields, " , "))
	return order
}

// Build limit clause for pagination
func (s *mSqlDatabaseQuery) buildLimit() string {
	// Calculate limit and offset from page number and page size (limit)
	var offset int
	if s.limit > 0 {
		if s.page < 2 {
			offset = 0
			return fmt.Sprintf(`LIMIT %d`, s.limit)
		} else {
			offset = (s.page - 1) * s.limit
			return fmt.Sprintf(`LIMIT %d OFFSET %d`, s.limit, offset)
		}
	} else {
		return ""
	}
}

// Build query filter
func (s *mSqlDatabaseQuery) buildFilter(qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// Ignore empty values
	if len(qf.GetValues()) == 0 {
		return "", nil
	}

	// Determine the field name and extract operator
	fieldName := qf.GetField()
	if qf.GetField() != "id" {
		//fieldName = fmt.Sprintf("data->>'%s'", qf.GetField())
		fieldName = s.getCastField(qf)
	}

	switch qf.GetOperator() {
	case database.Eq:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), qf.GetValues()
	case database.Neq:
		return fmt.Sprintf("(%s != $%d)", fieldName, varIndex), qf.GetValues()
	case database.Gt:
		return fmt.Sprintf("(%s > $%d)", fieldName, varIndex), qf.GetValues()
	case database.Gte:
		return fmt.Sprintf("(%s >= $%d)", fieldName, varIndex), qf.GetValues()
	case database.Lt:
		return fmt.Sprintf("(%s < $%d)", fieldName, varIndex), qf.GetValues()
	case database.Lte:
		return fmt.Sprintf("(%s <= $%d)", fieldName, varIndex), qf.GetValues()
	case database.Like:
		return s.buildFilterLike(fieldName, qf, varIndex)
	case database.In:
		return s.buildFilterIn(fieldName, qf, varIndex)
	case database.NotIn:
		return s.buildFilterNotIn(fieldName, qf, varIndex)
	case database.Between:
		return fmt.Sprintf("(%s BETWEEN $%d AND $%d)", fieldName, varIndex, varIndex+1), qf.GetValues()
	case database.Contains:
		return fmt.Sprintf("((%s)::jsonb @> $%d)", fieldName, varIndex), qf.GetValues()
	default:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), qf.GetValues()
	}
}

// Build LIKE query filter
func (s *mSqlDatabaseQuery) buildFilterLike(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	args = make([]any, 0)
	parts := make([]string, 0)

	for _, value := range qf.GetValues() {
		str := parseWildcards(fmt.Sprintf("%v", value))
		parts = append(parts, fmt.Sprintf("(lower(%s) LIKE lower($%d))", fieldName, varIndex))
		args = append(args, str)
		varIndex++
	}
	sqlPart = fmt.Sprintf("(%s)", strings.Join(parts, " OR "))
	return
}

// Handle special characters: * ?
func parseWildcards(value string) string {
	if strings.Contains(value, "*") {
		return strings.Replace(value, "*", "%", -1)
	} else if strings.Contains(value, "%") {
		return value
	} else {
		return fmt.Sprintf("%%%s%%", value)
	}
}

// Build IN query filter
func (s *mSqlDatabaseQuery) buildFilterIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// If value is of type array, convert each item to an array
	list := make([]any, 0)

	for _, val := range qf.GetValues() {
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			items := s.convertAnyArray(val)
			for _, item := range items {
				list = append(list, item)
			}
		} else {
			list = append(list, val)
		}
	}

	return fmt.Sprintf("(%s = ANY($%d))", fieldName, varIndex), []any{list}
}

// Build NOT IN query filter
func (s *mSqlDatabaseQuery) buildFilterNotIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// If value is of type array, convert each item to an array
	list := make([]any, 0)

	for _, val := range qf.GetValues() {
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			items := s.convertAnyArray(val)
			for _, item := range items {
				list = append(list, item)
			}
		} else {
			list = append(list, val)
		}
	}
	return fmt.Sprintf("NOT (%s = ANY ($%d))", fieldName, varIndex), []any{list}
}

// Build the cast
func (s *mSqlDatabaseQuery) getCastField(qf database.QueryFilter) (result string) {
	result = fmt.Sprintf("data->>'%s'", qf.GetField())

	values := qf.GetValues()
	if len(values) == 0 {
		return
	}

	switch v := values[0].(type) {
	case int:
		result = fmt.Sprintf("%v", v)
		return fmt.Sprintf("(data->>'%s')::BIGINT", qf.GetField())
	case uint:
		return fmt.Sprintf("(data->>'%s')::BIGINT", qf.GetField())
	case float32:
		return fmt.Sprintf("(data->>'%s')::FLOAT", qf.GetField())
	case float64:
		return fmt.Sprintf("(data->>'%s')::FLOAT", qf.GetField())
	default:
		return fmt.Sprintf("data->>'%s'", qf.GetField())
	}
}

// endregion

func (s *mSqlDatabaseQuery) convertAnyArray(value any) (result []any) {

	switch v := value.(type) {
	case []any:
		for _, item := range v {
			result = append(result, item)
		}
	case []int:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint:
		for _, item := range v {
			result = append(result, item)
		}
	case []int32:
		for _, item := range v {
			result = append(result, item)
		}
	case []int64:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint32:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint64:
		for _, item := range v {
			result = append(result, item)
		}
	case []string:
		for _, item := range v {
			result = append(result, item)
		}
	case []float32:
		for _, item := range v {
			result = append(result, item)
		}
	case []float64:
		for _, item := range v {
			result = append(result, item)
		}
	case []bool:
		for _, item := range v {
			result = append(result, item)
		}
	default:
		fmt.Println(v)
		result = append(result, value)
	}
	return result
}
