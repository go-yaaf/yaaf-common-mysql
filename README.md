# GO-YAAF MySQL Database Middleware

![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-mysql/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-mysql/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-mysql/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-mysql?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-mysql)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-mysql)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-mysql?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-mysql)
![License](https://img.shields.io/dub/l/vibe-d.svg)

## Overview

`yaaf-common-mysql` is a library that provides a MySQL database adapter for the `yaaf-common` framework. It simplifies database operations by offering a convenient interface for interacting with a MySQL database, including support for SSH tunneling.
This implementation refers to the object database concepts only, the underlying database includes table per domain model entity,
each table has only two fields: `id` (of type string) and `data` (of type jsonb).
Domain model entities are stored as Json documents (which are indexed by json keys) hence the database is used like a document storage (similar to Elasticsearch, MongoDB, Couchbase and more)

This library is built around the [Go-MySQL-driver](https://github.com/go-sql-driver/mysql/) library


## Features

*   **CRUD Operations:** Easily create, read, update, and delete entities.
*   **Bulk Operations:** Perform bulk inserts, updates, and deletes for efficient data management.
*   **SSH Tunneling:** Securely connect to a MySQL database through an SSH tunnel.
*   **Connection Pooling:** Utilizes connection pooling for efficient database connections.
*   **Entity-Based:** Works with entities defined in `yaaf-common`.

## Installation

To use `yaaf-common-mysql` in your project, you can use `go get`:

```bash
go get github.com/go-yaaf/yaaf-common-mysql
```

## Configuration

The database connection is configured using a URI. The format for the URI is:

```
mysql://<user>:<password>@<host>:<port>/<database>
```

For a connection over an SSH tunnel, you can include SSH parameters in the URI:

```
mysql://<user>:<password>@<host>:<port>/<database>?ssh_host=<ssh_host>&ssh_port=<ssh_port>&ssh_user=<ssh_user>&ssh_pwd=<ssh_password>
```

## Usage

### Creating a Database Instance

To get started, create a new `IDatabase` instance using the `NewMySqlDatabase` factory method:

```go
import (
    "github.com/go-yaaf/yaaf-common/database"
    "github.com/go-yaaf/yaaf-common-mysql/mysql"
)

func main() {
    uri := "mysql://user:password@localhost:3306/mydatabase"
    db, err := mysql.NewMySqlDatabase(uri)
    if err != nil {
        // Handle error
    }
    defer db.Close()

    // Use the database instance
}
```

### CRUD Operations

The library provides standard CRUD operations for your entities.

#### Defining an Entity

First, define your entity by embedding `BaseEntity` from `yaaf-common`:

```go
import "github.com/go-yaaf/yaaf-common/entity"

type Hero struct {
    entity.BaseEntity
    Name  string `json:"name"`
    Power string `json:"power"`
}

func (h *Hero) TABLE() string { return "heroes" }
func (h *Hero) KEY() []string { return []string{} }

func NewHero(name, power string) *Hero {
    return &Hero{
        BaseEntity: entity.NewBaseEntity(),
        Name:       name,
        Power:      power,
    }
}
```

#### Insert

To insert a new entity into the database:

```go
hero := NewHero("Superman", "Flight")
if _, err := db.Insert(hero); err != nil {
    // Handle error
}
```

#### Get

To retrieve an entity by its ID:

```go
retrievedHero, err := db.Get(func() entity.Entity { return &Hero{} }, hero.ID())
if err != nil {
    // Handle error
}
```

#### Update

To update an existing entity:

```go
heroToUpdate := retrievedHero.(*Hero)
heroToUpdate.Power = "Super Strength"
if _, err := db.Update(heroToUpdate); err != nil {
    // Handle error
}
```

#### Delete

To delete an entity by its ID:

```go
if err := db.Delete(func() entity.Entity { return &Hero{} }, hero.ID()); err != nil {
    // Handle error
}
```

### Bulk Operations

The library also supports bulk operations for inserting, updating, and deleting multiple entities at once.

#### Bulk Insert

```go
heroes := []entity.Entity{
    NewHero("Batman", "Rich"),
    NewHero("Wonder Woman", "Lasso of Truth"),
}
if _, err := db.BulkInsert(heroes); err != nil {
    // Handle error
}
```

#### Bulk Update

```go
// Assume heroes have been retrieved and modified
if _, err := db.BulkUpdate(heroes); err != nil {
    // Handle error
}
```

#### Bulk Delete

```go
heroIDs := []string{"id1", "id2", "id3"}
if _, err := db.BulkDelete(func() entity.Entity { return &Hero{} }, heroIDs); err != nil {
    // Handle error
}
```

### Querying

The library provides a query builder for constructing more complex queries.

```go
query := db.Query(func() entity.Entity { return &Hero{} })
results, err := query.Where("power", "=", "Flight").Find()
if err != nil {
    // Handle error
}
```

### DDL Operations

You can execute Data Definition Language (DDL) statements to manage your database schema.

```go
ddl := map[string][]string{
    "heroes": {"name", "power"},
}
if err := db.ExecuteDDL(ddl); err != nil {
    // Handle error
}
```
