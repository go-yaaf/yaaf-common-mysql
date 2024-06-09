package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

// region Database basic CRUD methods ----------------------------------------------------------------------------------

// Get a single entity by ID
//
// param: factory - Entity factory
// param: entityID - Entity id
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Entity, error
func (dbs *MySqlDatabase) Get(factory EntityFactory, entityID string, keys ...string) (result Entity, err error) {

	var (
		rows *sql.Rows
		fe   error
	)

	result = factory()

	defer func() {
		if fe != nil {
			if result != nil {
				result = nil
			}
		}
	}()

	if entityID == "" {
		return nil, fmt.Errorf("empty entity id passed to Get operation")
	}

	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = $1`, tableName(result.TABLE(), keys...))

	if rows, err = dbs.pgDb.Query(SQL, entityID); err != nil {
		return nil, err
	}

	// Connection is released to pool only after rows is closed.
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return nil, fmt.Errorf("no row fetched for id: %s", entityID)
	}

	jsonDoc := JsonDoc{}
	if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
		return nil, err
	}

	if err = Unmarshal([]byte(jsonDoc.Data), &result); err != nil {
		return nil, err
	}

	return
}

// Exists Check if entity exists by ID
//
// param: factory - Entity factory
// param: entityID - Entity id
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: bool, error
func (dbs *MySqlDatabase) Exists(factory EntityFactory, entityID string, keys ...string) (result bool, err error) {

	SQL := fmt.Sprintf(`SELECT id FROM "%s" WHERE id = $1`, tableName(factory().TABLE(), keys...))

	if rows, err := dbs.pgDb.Query(SQL, entityID); err != nil {
		return false, err
	} else {
		result = rows.Next()
		_ = rows.Close()
		return result, nil
	}
}

// List Get list of entities by IDs
//
// param: factory - Entity factory
// param: entityIDs - List of Entity IDs
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: []Entity, error
func (dbs *MySqlDatabase) List(factory EntityFactory, entityIDs []string, keys ...string) (list []Entity, err error) {

	var (
		rows *sql.Rows
	)

	list = make([]Entity, 0)

	// For empty list of ids, return empty list
	if len(entityIDs) == 0 {
		return list, nil
	}

	table := tableName(factory().TABLE(), keys...)
	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = ANY($1)`, table)
	if rows, err = dbs.pgDb.Query(SQL, entityIDs); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		jsonDoc := JsonDoc{}
		if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
			return
		} else {
			entity := factory()
			if err = Unmarshal([]byte(jsonDoc.Data), &entity); err == nil {
				list = append(list, entity)
			}
		}
	}
	return
}

// Insert new entity
//
// param: entity - The entity to insert
// return: Inserted Entity, error
func (dbs *MySqlDatabase) Insert(entity Entity) (added Entity, err error) {
	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlInsert, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	if affected, err := result.RowsAffected(); err != nil {
		return nil, err
	} else if affected == 0 {
		err = fmt.Errorf("no row affected when inserting new entity")
	}
	added = entity

	// Publish the change
	dbs.publishChange(AddEntity, added)
	return
}

// Update existing entity
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *MySqlDatabase) Update(entity Entity) (updated Entity, err error) {

	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpdate, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return nil, fmt.Errorf("no row affected when executing update operation")
	}
	updated = entity

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return
}

// Upsert Update entity or insert it if it does not exist
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *MySqlDatabase) Upsert(entity Entity) (updated Entity, err error) {
	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpsert, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return nil, fmt.Errorf("no row affected when executing upsert operation")
	}
	updated = entity

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return
}

// Delete entity
//
// param: factory - Entity factory
// param: entityID - Entity ID to delete
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *MySqlDatabase) Delete(factory EntityFactory, entityID string, keys ...string) (err error) {
	var (
		affected int64
		result   sql.Result
	)
	entity := factory()

	// Get entity
	deleted, er := dbs.Get(factory, entityID, keys...)
	if er != nil {
		return er
	}

	tblName := tableName(entity.TABLE(), keys...)
	SQL := fmt.Sprintf(sqlDelete, tblName)
	if result, err = dbs.pgDb.Exec(SQL, entityID); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return fmt.Errorf("no row affected when executing delete operation")
	}

	// Publish the change
	dbs.publishChange(DeleteEntity, deleted)
	return
}

//endregion

// region Database bulk CRUD methods -----------------------------------------------------------------------------------

// BulkInsert Insert multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to insert
// return: Number of inserted entities, error
func (dbs *MySqlDatabase) BulkInsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	// Get the table
	table := tableName(entities[0].TABLE(), entities[0].KEY())
	valueStrings := make([]string, 0, len(entities))
	valueArgs := make([]any, 0, len(entities)*2)
	i := 0
	for _, entity := range entities {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, entity.ID())
		bytes, _ := Marshal(entity)
		valueArgs = append(valueArgs, string(bytes))
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, data) VALUES %s`, table, strings.Join(valueStrings, ","))

	var (
		result sql.Result
	)
	if result, err = dbs.pgDb.Exec(SQL, valueArgs...); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return affected, fmt.Errorf("no row affected when executing bulk insert operation")
	}

	// Publish the change
	for _, entity := range entities {
		dbs.publishChange(AddEntity, entity)
	}
	return
}

// BulkUpdate Update multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to update
// return: Number of updated entities, error
func (dbs *MySqlDatabase) BulkUpdate(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var (
		tx *sql.Tx
	)

	// Start transaction
	if tx, err = dbs.pgDb.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpdate, table)
		data, _ := Marshal(entity)
		if _, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return
	} else {
		affected = int64(len(entities))
	}

	// Publish the changes
	for _, entity := range entities {
		dbs.publishChange(UpdateEntity, entity)
	}
	return
}

// BulkUpsert Upsert multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to upsert
// return: Number of updated entities, error
func (dbs *MySqlDatabase) BulkUpsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var (
		tx *sql.Tx
	)

	// Start transaction
	if tx, err = dbs.pgDb.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpsert, table)
		data, _ := Marshal(entity)
		if _, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return
	} else {
		affected = int64(len(entities))
	}

	// Publish the changes
	for _, entity := range entities {
		dbs.publishChange(UpdateEntity, entity)
	}
	return
}

// BulkDelete Delete multiple entities from the database in a single transaction (all must be of the same type)
//
// param: factory - Entity factory
// param: entityIDs - List of entities IDs to delete
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Number of deleted entities, error
func (dbs *MySqlDatabase) BulkDelete(factory EntityFactory, entityIDs []string, keys ...string) (affected int64, err error) {
	var (
		result sql.Result
		entity = factory()
	)

	if len(entityIDs) == 0 {
		return 0, nil
	}

	tblName := tableName(entity.TABLE(), keys...)

	// Get the list of deleted entities (for notification)
	deleted, e := dbs.List(factory, entityIDs, keys...)
	if e != nil {
		return 0, e
	}

	SQL := fmt.Sprintf(sqlBulkDelete, tblName)

	if result, err = dbs.pgDb.Exec(SQL, entityIDs); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return 0, fmt.Errorf("no row affected when executing delete operation")
	}

	// Publish the change to the cache
	for _, ent := range deleted {
		dbs.publishChange(DeleteEntity, ent)
	}
	return
}

//endregion

// region Database set field methods -----------------------------------------------------------------------------------

// SetField Update a single field of the document in a single transaction
//
// param: factory - Entity factory
// param: entityID - The entity ID to update the field
// param: field - The field name to update
// param: value - The field value to update
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *MySqlDatabase) SetField(factory EntityFactory, entityID string, field string, value any, keys ...string) (err error) {

	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	SQL := fmt.Sprintf(`UPDATE "%s" SET data = jsonb_set(data, '{%s}', $1, false) WHERE id = $2`, tblName, field)

	args := make([]any, 0)
	args = append(args, value)
	args = append(args, entityID)

	if _, err = dbs.pgDb.Exec(SQL, args...); err != nil {
		return
	}

	// Get the updated entity and publish the change
	if updated, fer := dbs.Get(factory, entityID, keys...); fer == nil {
		dbs.publishChange(UpdateEntity, updated)
	}
	return
}

// SetFields Update some fields of the document in a single transaction
//
// param: factory - Entity factory
// param: entityID - The entity ID to update the field
// param: fields - A map of field-value pairs to update
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *MySqlDatabase) SetFields(factory EntityFactory, entityID string, fields map[string]any, keys ...string) (err error) {

	for f, v := range fields {
		if er := dbs.SetField(factory, entityID, f, v, keys...); er != nil {
			return er
		}
	}
	return nil
}

// BulkSetFields Update specific field of multiple entities in a single transaction (eliminates the need to fetch - change - update)
//
// param: factory - Entity factory
// param: field - The field name to update
// param: values - The map of entity Id to field value
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Number of updated entities, error
func (dbs *MySqlDatabase) BulkSetFields(factory EntityFactory, field string, values map[string]any, keys ...string) (affected int64, error error) {

	if len(values) == 0 {
		return 0, nil
	}

	// Determine the type of the field
	sqlType := dbs.getSqlType(values)

	// Create temp table to map entity to field id
	tmpTable := fmt.Sprintf("ch%d", time.Now().UnixMilli())
	createTmp := fmt.Sprintf("create TEMP table %s (id character varying PRIMARY KEY NOT NULL, val %s)", tmpTable, sqlType)
	if _, err := dbs.pgDb.Exec(createTmp); err != nil {
		return 0, err
	}

	// Bulk Insert values
	valueStrings := make([]string, 0, len(values))
	valueArgs := make([]any, 0, len(values)*2)
	i := 0
	for id, val := range values {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, id)
		valueArgs = append(valueArgs, val)
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, val) VALUES %s`, tmpTable, strings.Join(valueStrings, ","))
	if _, err := dbs.pgDb.Exec(SQL, valueArgs...); err != nil {
		return 0, err
	}

	// Create bulk update statement
	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	SQL = fmt.Sprintf("UPDATE %s SET data['%s'] = to_jsonb(%s.val) FROM %s WHERE %s.id = %s.id", tblName, field, tmpTable, tmpTable, tmpTable, tblName)

	// Drop the temp table
	defer func() {
		DROP := fmt.Sprintf("DROP TABLE %s", tmpTable)
		_, _ = dbs.pgDb.Exec(DROP)
	}()

	// Execute update
	if result, err := dbs.pgDb.Exec(SQL); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

// Get the SQL type of the value
func (dbs *MySqlDatabase) getSqlType(values map[string]any) string {

	typeName := "string"
	for _, v := range values {
		typeName = fmt.Sprintf("%T", v)
		break
	}
	if strings.HasPrefix(typeName, "string") {
		return "character varying"
	}
	if strings.HasPrefix(typeName, "float") {
		return "double precision"
	}
	if strings.HasPrefix(typeName, "bool") {
		return "boolean"
	}

	// For all other types (numbers, timestamp, enums) return bigint
	return "bigint"
}

//endregion

// region Database Query methods ---------------------------------------------------------------------------------------

// Query Helper method to construct query
//
// param: factory - Entity factory
// return: Query object
func (dbs *MySqlDatabase) Query(factory EntityFactory) database.IQuery {
	return &mSqlDatabaseQuery{
		db:      dbs,
		factory: factory,
	}
}

//endregion

// region Database DDL methods -----------------------------------------------------------------------------------------

// ExecuteDDL create table and indexes
//
// param: ddl - The ddl parameter is a map of strings (table names) to array of strings (list of fields to index)
// return: error
func (dbs *MySqlDatabase) ExecuteDDL(ddl map[string][]string) (err error) {
	for table, fields := range ddl {

		SQL := fmt.Sprintf(ddlCreateTable, table)
		if _, err = dbs.pgDb.Exec(SQL); err != nil {
			logger.Error("%s error: %s", SQL, err.Error())
			return
		}
		for _, field := range fields {
			SQL = fmt.Sprintf(ddlCreateIndex, table, field, table, field)
			if _, err = dbs.pgDb.Exec(SQL); err != nil {
				logger.Error("%s error: %s", SQL, err.Error())
				return
			}
		}
	}
	return nil
}

// ExecuteSQL Execute SQL command
//
// param: sql - The SQL command to execute
// param: args - Statement arguments
// return: Number of affected records, error
func (dbs *MySqlDatabase) ExecuteSQL(sql string, args ...any) (int64, error) {
	if result, err := dbs.pgDb.Exec(sql, args...); err != nil {
		logger.Error("%s error: %s", sql, err.Error())
		return 0, err
	} else {
		if a, er := result.RowsAffected(); er != nil {
			return 0, er
		} else {
			return a, nil
		}
	}
}

// ExecuteQuery Execute native SQL query
func (dbs *MySqlDatabase) ExecuteQuery(sql string, args ...any) ([]Json, error) {

	rows, err := dbs.pgDb.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}
	// Prepare a slice of interface{}'s to represent each column, and a second slice to contain pointers to each item in the columns slice
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	result := make([]Json, 0)

	// Iterate over the rows
	for rows.Next() {
		// Scan the result into the column pointers
		if er := rows.Scan(valuePtrs...); er != nil {
			return nil, fmt.Errorf("failed to scan row: %v", er)
		}
		// Create a map and populate it with the row data
		entry := Json{}
		for i, col := range columns {
			var v any
			val := values[i]

			// If the column is null, set the value to nil
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}

			entry[col] = v
		}
		result = append(result, entry)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %v", err)
	}

	return result, nil
}

// DropTable Drop table and indexes
//
// param: table - Table name to drop
// return: error
func (dbs *MySqlDatabase) DropTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlDropTable, table)
	if _, err = dbs.pgDb.Exec(SQL); err != nil {
		logger.Error("%s error: %s", SQL, err.Error())
	}
	return
}

// PurgeTable Fast delete table content (truncate)
//
// param: table - Table name to purge
// return: error
func (dbs *MySqlDatabase) PurgeTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlPurgeTable, table)
	if _, err = dbs.pgDb.Exec(SQL); err != nil {
		logger.Error("%s error: %s", SQL, err.Error())
	}
	return
}

//endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// publishChange Publish entity change to the message bus:
//
//	Topic: 		ENTITY_<accountId> or ENTITY_system
//	Payload:		The entity
//	OpCode:		1=Add, 2=Update, 3=Delete
//	Addressee:		The entity table name
//	SessionId:		The shard key
//
// param: action - The action on the entity
// param: entity - The changed entity
func (dbs *MySqlDatabase) publishChange(action EntityAction, entity Entity) {

	if dbs.bus == nil || entity == nil {
		return
	}

	// Set topic in the format of: ENTITY-{Table}-{Key}
	topic := fmt.Sprintf("%s-%s-%s", messaging.EntityMessageTopic, entity.TABLE(), entity.KEY())
	addressee := reflect.TypeOf(entity).String()
	idx := strings.LastIndex(addressee, ".")
	addressee = addressee[idx+1:]

	if dbs.bus != nil {
		msg := messaging.EntityMessage{
			BaseMessage: messaging.BaseMessage{
				MsgTopic:     topic,
				MsgOpCode:    int(action),
				MsgAddressee: addressee,
				MsgSessionId: entity.ID(),
			},
			MsgPayload: entity,
		}
		if err := dbs.bus.Publish(&msg); err != nil {
			logger.Warn("error publishing change: %s", err.Error())
		}
	}
}

// endregion

// region Datastore  methods -------------------------------------------------------------------------------------------

// IndexExists tests if index exists
func (dbs *MySqlDatabase) IndexExists(indexName string) (exists bool) {
	// TODO: Add implementation
	return false
}

// CreateIndex creates an index (without mapping)
func (dbs *MySqlDatabase) CreateIndex(indexName string) (name string, err error) {
	// TODO: Add implementation
	return indexName, fmt.Errorf("not implemented")
}

// CreateEntityIndex creates an index of entity and add entity field mapping
func (dbs *MySqlDatabase) CreateEntityIndex(factory EntityFactory, key string) (name string, err error) {
	// TODO: Add implementation
	return key, fmt.Errorf("not implemented")
}

// ListIndices returns a list of all indices matching the pattern
func (dbs *MySqlDatabase) ListIndices(pattern string) (map[string]int, error) {
	// TODO: Add implementation
	return nil, fmt.Errorf("not implemented")
}

// DropIndex drops an index
func (dbs *MySqlDatabase) DropIndex(indexName string) (ack bool, err error) {
	// TODO: Add implementation
	return false, fmt.Errorf("not implemented")
}

// endregion
