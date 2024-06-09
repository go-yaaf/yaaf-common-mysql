package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-mysql/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySqlDatabase(t *testing.T) {
	skipCI(t)

	uri := "mysql://user:password@host:3306/dbname"

	fmt.Println(uri)

	db, err := mysql.NewMySqlDatabase(uri)
	require.NoError(t, err)

	err = db.Ping(2, 5)
	require.NoError(t, err)
}

func TestMySqlDatabaseOverSsh(t *testing.T) {
	skipCI(t)

	uri := "mysql://user:password@host:3306/dbname?ssh_user=ssh_usr&ssh_pwd=ssh_pwd&ssh_host=ssh_host&ssh_port=ssh_port"

	fmt.Println(uri)

	db, err := mysql.NewMySqlDatabase(uri)
	require.NoError(t, err)

	err = db.Ping(2, 5)
	require.NoError(t, err)

	query := fmt.Sprintf("select * from table_name limit %d", 100)
	list, er := db.ExecuteQuery(query)
	require.NoError(t, er)

	for _, row := range list {
		fmt.Println(row)
	}

	err = db.Close()
	require.NoError(t, err)
}
