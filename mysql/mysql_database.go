package mysql

import (
	"database/sql"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"

	_ "github.com/go-sql-driver/mysql"
)

// region Configuration helpers ----------------------------------------------------------------------------------------

// SSHConfig holds the SSH configuration
type SSHConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	KeyFile  string
}

// DBConfig holds the MySQL database configuration
type DBConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	DBName   string
	AppName  string
	Driver   string
}

// ConnectionString returns DNS connection
func (c *DBConfig) ConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.Username, c.Password, c.Host, c.Port, c.DBName)
}

//endregion

// region Database store definitions -----------------------------------------------------------------------------------

type MySqlDatabase struct {
	pgDb   *sql.DB               // The sql connection
	bus    messaging.IMessageBus // Message bus for change notifications
	uri    string                // DB connection URI
	ssh    *ssh.Client           // SSH client (in case of connection over SSH)
	tunnel net.Listener          // SSH tunnel (in case of connection over SSH)
}

const (
	sqlInsert      = `INSERT INTO "%s" (id, data) VALUES ($1, $2)`
	sqlUpdate      = `UPDATE "%s" SET data = $2 WHERE id = $1`
	sqlUpsert      = `INSERT INTO "%s" (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2`
	sqlDelete      = `DELETE FROM "%s" WHERE id = $1`
	sqlBulkDelete  = `DELETE FROM "%s" WHERE id = ANY($1)`
	ddlDropTable   = `DROP TABLE IF EXISTS "%s" CASCADE`
	ddlCreateTable = `CREATE TABLE IF NOT EXISTS "%s" (id character varying PRIMARY KEY NOT NULL, data jsonb NOT NULL default '{}')`
	ddlCreateIndex = `CREATE INDEX IF NOT EXISTS %s_%s_idx ON "%s" USING BTREE ((data->>'%s'))`
	ddlPurgeTable  = `TRUNCATE "%s" RESTART IDENTITY CASCADE`
)

// endregion

// region Factory method for Database store ----------------------------------------------------------------------------

// NewMySqlStore factory method for datastore
//
// param: URI - represents the database connection string in the format of: mysql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewMySqlStore(URI string) (database.IDatastore, error) {
	if db, sshCli, tunnel, err := openConnection(URI); err != nil {
		return nil, err
	} else {
		dbs := &MySqlDatabase{
			pgDb:   db,
			uri:    URI,
			ssh:    sshCli,
			tunnel: tunnel,
		}
		return dbs, nil
	}
}

// NewMySqlDatabase factory method for database
//
// param: URI - represents the database connection string in the format of: mysql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewMySqlDatabase(URI string) (database.IDatabase, error) {
	if db, sshCli, tunnel, err := openConnection(URI); err != nil {
		return nil, err
	} else {
		dbs := &MySqlDatabase{
			pgDb:   db,
			uri:    URI,
			ssh:    sshCli,
			tunnel: tunnel,
		}
		return dbs, nil
	}
}

// NewMySqlDatabaseWithMessageBus factory method for database with injected message bus
//
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewMySqlDatabaseWithMessageBus(URI string, bus messaging.IMessageBus) (database.IDatabase, error) {
	if db, sshCli, tunnel, err := openConnection(URI); err != nil {
		return nil, err
	} else {
		dbs := &MySqlDatabase{
			pgDb:   db,
			uri:    URI,
			ssh:    sshCli,
			tunnel: tunnel,
			bus:    bus,
		}
		return dbs, nil
	}
}

// Ping Test database connectivity
//
// param: retries - how many retries are required (max 10)
// param: intervalInSeconds - time interval (in seconds) between retries (max 60)
func (dbs *MySqlDatabase) Ping(retries uint, intervalInSeconds uint) error {

	if retries > 10 {
		retries = 10
	}

	if intervalInSeconds > 60 {
		intervalInSeconds = 60
	}

	for try := 1; try <= int(retries); try++ {
		err := dbs.pgDb.Ping()
		if err == nil {
			return nil
		}

		// In case of failure, sleep and try again after 10 seconds
		logger.Debug("ping to database failed try %d of 5", try)

		// time.Second
		duration := time.Second * time.Duration(intervalInSeconds)
		time.Sleep(duration)
	}
	return fmt.Errorf("could not establish database connection")
}

// Close DB and free resources
func (dbs *MySqlDatabase) Close() error {

	// Close SSH tunnel
	if dbs.tunnel != nil {
		_ = dbs.tunnel.Close()
	}

	// Close SSH connection
	if dbs.ssh != nil {
		_ = dbs.ssh.Close()
	}

	// Close database connection
	if dbs.pgDb != nil {
		_ = dbs.pgDb.Close()
	}
	return nil
}

// CloneDatabase Returns a clone (copy) of the database instance
func (dbs *MySqlDatabase) CloneDatabase() (database.IDatabase, error) {
	return NewMySqlDatabaseWithMessageBus(dbs.uri, dbs.bus)
}

// CloneDatastore Returns a clone (copy) of the database instance
func (dbs *MySqlDatabase) CloneDatastore() (database.IDatastore, error) {
	return NewMySqlStore(dbs.uri)
}

// Resolve table name from entity class name and shard keys
func tableName(table string, keys ...string) (tblName string) {

	tblName = table

	if len(keys) == 0 {
		return tblName
	}

	// replace accountId placeholder with the first key
	tblName = strings.Replace(tblName, "{{accountId}}", "{{0}}", -1)

	for idx, key := range keys {
		placeHolder := fmt.Sprintf("{{%d}}", idx)
		tblName = strings.Replace(tblName, placeHolder, key, -1)
	}

	// Replace templates: {{year}}
	tblName = strings.Replace(tblName, "{{year}}", time.Now().Format("2006"), -1)

	// Replace templates: {{month}}
	tblName = strings.Replace(tblName, "{{month}}", time.Now().Format("01"), -1)

	// TODO: Replace templates: {{week}}

	return
}

//endregion

// region Connectivity Methods -----------------------------------------------------------------------------------------

// parseConnectionString Convert URI style connection to DB connection string in the format of:
// postgres://user:password@host:port/database_name
// to MYSQL connection string: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
// return: driver name, connection string, error
func parseConnectionString(dbUri string) (*DBConfig, *SSHConfig, error) {
	uri, err := url.Parse(strings.TrimSpace(dbUri))
	if err != nil {
		return nil, nil, fmt.Errorf("URI: %s parsing failed: %s", dbUri, err.Error())
	}

	dbCfg := &DBConfig{}
	dbCfg.Username = uri.User.Username()
	dbCfg.Password, _ = uri.User.Password()
	dbCfg.Driver = strings.ToLower(uri.Scheme)
	if dbCfg.Driver != "mysql" {
		return nil, nil, fmt.Errorf("schema for postgresql database must be: mysql")
	}

	dbCfg.DBName = strings.TrimPrefix(uri.Path, "/") // Remove slash
	if host, port, er := net.SplitHostPort(uri.Host); er != nil {
		return nil, nil, fmt.Errorf("URI: %s host:port parsing failed: %s", uri, er.Error())
	} else {
		dbCfg.Host = host
		dbCfg.Port, _ = strconv.Atoi(port)
	}

	// Get the app name
	params := uri.Query()
	if _, ok := params["application_name"]; ok {
		dbCfg.AppName = params["application_name"][0]
	} else if _, ok := params["ApplicationName"]; ok {
		dbCfg.AppName = params["ApplicationName"][0]
	} else {
		executablePath := os.Args[0]                  // Gets the path of the currently running executable
		dbCfg.AppName = filepath.Base(executablePath) // Extracts the executable name from the path
	}

	// Check for connection over SSH
	sshCfg := &SSHConfig{}
	if _, ok := params["ssh_host"]; ok {
		sshCfg.Host = params["ssh_host"][0]
	} else {
		return dbCfg, nil, nil
	}
	if _, ok := params["ssh_port"]; ok {
		sshPort := params["ssh_port"][0]
		sshCfg.Port, _ = strconv.Atoi(sshPort)
	}
	if _, ok := params["ssh_user"]; ok {
		sshCfg.Username = params["ssh_user"][0]
	}
	if _, ok := params["ssh_pwd"]; ok {
		sshCfg.Password = params["ssh_pwd"][0]
	}
	return dbCfg, sshCfg, nil
}

// openConnection open Database connection	with / without SSH
func openConnection(URI string) (*sql.DB, *ssh.Client, net.Listener, error) {

	// Get configurations
	dbCfg, sshCfg, err := parseConnectionString(URI)
	if err != nil {
		return nil, nil, nil, err
	}
	if sshCfg != nil {
		return openConnectionOverSSH(dbCfg, sshCfg)
	}

	// Open standard connection
	cli, er := sql.Open(dbCfg.Driver, dbCfg.ConnectionString())
	if er != nil {
		return nil, nil, nil, er
	}

	// Ping the DB to test the connection
	er = cli.Ping()
	if er != nil {
		return nil, nil, nil, er
	}
	return cli, nil, nil, er
}

func openConnectionOverSSH(dbCfg *DBConfig, sshCfg *SSHConfig) (*sql.DB, *ssh.Client, net.Listener, error) {

	// Establish SSH connection
	sshClient, err := connectSSH(sshCfg)
	if err != nil {
		log.Fatalf("Failed to establish SSH connection: %v", err)
	}

	// Create an SSH tunnel
	tunnel, err := createSSHTunnel(sshClient, dbCfg)
	if err != nil {
		log.Fatalf("Failed to create SSH tunnel: %v", err)
	}

	// Connect to the MySQL database
	localAddr := tunnel.Addr().String()
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbCfg.Username, dbCfg.Password, localAddr, dbCfg.DBName)
	if dbs, er := sql.Open(dbCfg.Driver, dsn); er != nil {
		return nil, nil, nil, fmt.Errorf("failed to connect to MySQL: %v", err)
	} else {
		return dbs, sshClient, tunnel, nil
	}
}

// connectSSH establishes an SSH connection
func connectSSH(config *SSHConfig) (*ssh.Client, error) {
	var auth []ssh.AuthMethod
	if config.Password != "" {
		auth = append(auth, ssh.Password(config.Password))
	} else {
		file, err := os.Open(config.KeyFile)
		if err != nil {
			return nil, err
		}
		defer func() { _ = file.Close() }()

		key, err := io.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key: %v", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %v", err)
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	clientConfig := &ssh.ClientConfig{
		User:            config.Username,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	address := fmt.Sprintf("%s:%d", config.Host, config.Port)
	return ssh.Dial("tcp", address, clientConfig)
}

// createSSHTunnel creates an SSH tunnel for the MySQL connection
func createSSHTunnel(client *ssh.Client, dbConfig *DBConfig) (net.Listener, error) {
	localEndpoint := fmt.Sprintf("127.0.0.1:0")
	remoteEndpoint := fmt.Sprintf("%s:%d", dbConfig.Host, dbConfig.Port)

	listener, err := net.Listen("tcp", localEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create local listener: %v", err)
	}

	go func() {
		for {
			localConn, err := listener.Accept()
			if err != nil {
				log.Fatalf("failed to accept local connection: %v", err)
			}

			remoteConn, err := client.Dial("tcp", remoteEndpoint)
			if err != nil {
				log.Fatalf("failed to connect to remote endpoint: %v", err)
			}

			go func() {
				defer func() { _ = localConn.Close() }()
				defer func() { _ = remoteConn.Close() }()
				copyConn(localConn, remoteConn)
			}()
		}
	}()

	return listener, nil
}

// copyConn copies data between two connections
func copyConn(src, dst net.Conn) {
	go func() {
		defer func() { _ = src.Close() }()
		defer func() { _ = dst.Close() }()
		_, _ = io.Copy(src, dst)
	}()
	_, _ = io.Copy(dst, src)
}

// connectDB connects to the MySQL database using the SSH tunnel
func connectDB(config DBConfig, localAddr string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", config.Username, config.Password, localAddr, config.DBName)
	return sql.Open("mysql", dsn)
}

//endregion
