package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-mysql/mysql"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecuteQuery(t *testing.T) {
	skipCI(t)

	uri := "<db_uri>"
	db, err := mysql.NewMySqlDatabase(uri)
	require.NoError(t, err)

	err = db.Ping(2, 5)
	require.NoError(t, err)

	runQuery(db)

	_ = db.Close()
}

func runQuery(db database.IDatabase) {

	cp := 1729507296
	SQL := `
			SELECT * FROM aya_flight_diary f
			left join aya_flight_diary_cstm c on c.id_c = f.id
			left join aya_flight_diary_accounts_c a on f.id = a.aya_flight_diary_accountsaya_flight_diary_idb
			left join aya_flight_diary_flight_captain_c p on f.id = p.aya_flight_diary_flight_captain_aya_flight_diary_idb
			left join aya_flight_diary_aya_airplanes_c z on f.id = z.aya_flight_diary_aya_airplanesaya_flight_diary_idb
			WHERE f.date_modified > FROM_UNIXTIME(%d) AND f.deleted is false AND a.deleted is false AND p.deleted is false AND z.deleted is false
		`

	query := fmt.Sprintf(SQL, cp)

	list, er := db.ExecuteQuery("", query)
	if er != nil {
		panic(er)
	}

	for _, item := range list {
		fmt.Println(item)
	}
}
