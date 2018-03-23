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

## TODO

* Git style hook directory for pre/post migration scripts.
