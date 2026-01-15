# migrate

Bare bones Go migration library and command.

## Why

* Migrate anything not just SQL by implementing the abstract driver interface.
* Plain files are streamed to migration driver.
* Executabe files are run and stream STDOUT to migration driver.

## Drivers

### Postgres

```
migrate 'postgres://localhost/example' _testdata
```

## Migration Table

Migration history is stored in a table managed by the driver. The default location varies by driver:

| Driver   | Default Location                  |
|----------|-----------------------------------|
| Postgres | `migrate.schema_migrations`       |
| DuckDB   | `migrate.schema_migrations`       |
| SQLite   | `migrate_schema_migrations`       |

### Custom Schema/Table

For isolation (e.g., modular monoliths), configure the schema and table name programmatically:

```go
m, _ := migrate.New("postgres", dsn,
    migrate.WithSchema("orders"),
    migrate.WithTableName("migrations"),
)
// Postgres/DuckDB: orders.migrations
// SQLite: orders_migrations
```

Or via CLI flags:

```bash
migrate -schema orders -table migrations 'postgres://...' ./migrations
```

## TODO

* Git style hook directory for pre/post migration scripts.
