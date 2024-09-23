# OData Adapter For Apache Datafusion

## About
This is an [OData](https://www.odata.org/) API adapter for [Apache Datafusion](https://github.com/apache/arrow-datafusion) SQL engine.

OData protocol is positioned as "The SQL of REST", but is a somewhat legacy protocol used by some older systems. We wouldn't recommend using it as an integration protocol for some new project, but this adapter is useful if you *have to* integrate your Datafusion app with some existing OData-focused system.

## Quick Start
Start example:
```sh
RUST_LOG=debug cargo run --example simple_service
```

Query using [xh](https://github.com/ducaale/xh):

Service root:
```sh
xh GET 'http://localhost:50051/'
```

Metadata:
```sh
xh GET 'http://localhost:50051/$metadata'
```

Query collection:
```sh
xh GET 'http://localhost:50051/tickers.spy/?$select=offset,from_symbol,to_symbol,close&$top=5'
```

## Status
This code is super raw and experimental. Very far from prod-ready. Use at your own risk.

- [x] Only support small subset of `OData 3.0`
- [x] Only supports `atom` format in responses
- [x] Service root resource
- [x] `$metadata` resource
- [x] Collection resource
  - [x] `$select`
  - [x] `$orderby`
  - [x] `$skip`
  - [x] `$top`
  - [x] `$filter`
  - [ ] pagination
  - [x] real object IDs
- [x] Collection entry by ID (`service/collection(id)`)
  - [x] Numeric IDs
  - [ ] Other ID types
- [ ] Parameters
- [ ] Nested collections
- [ ] Functions
- [ ] ...
