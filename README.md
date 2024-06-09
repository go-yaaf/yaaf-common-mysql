# yaaf-common-mysql

[![Build](https://github.com/go-yaaf/yaaf-common-mysql/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-mysql/actions/workflows/build.yml)

MySQL object database implementation of `yaaf-common` IDatabase interface

IMPORTANT: This is a partial implementation only used for data transfer only! not ready for production!!

## About
This library is the MySQL concrete implementation of the ORM layer defined in the `IDatabase` interface in `yaaf-common` library.
This implementation refers to the object database concepts only, the underlying database includes table per domain model entity,
each table has only two fields: `id` (of type string) and `data` (of type jsonb).
Domain model entities are stored as Json documents (which are indexed by json keys) hence the database is used like a document storage (similar to Elasticsearch, MongoDB, Couchbase and more)

This library is built around the [Go-MySQL-driver](https://github.com/go-sql-driver/mysql/) library

#### Adding dependency

```bash
$ go get -v -t github.com/go-yaaf/yaaf-common-mysql
```

#### Connection URI
The connection URI for this library should be in the following format:
```bash
mysql://[user]:[password]@[host]:[port]/[db_name]
```

If connection is over SSH tunnel, the connection URI should be in the following format:
```bash
mysql://[user]:[password]@[host]:[port]/[db_name]?ssh_user=[ssh_usr]&ssh_pwd=[ssh_pwd]&ssh_host=[ssh_host]&ssh_port=[ssh_port]
```

