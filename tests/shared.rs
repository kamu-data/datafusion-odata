use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::{arrow::datatypes::SchemaRef, prelude::*, sql::TableReference};
use datafusion_odata::{
    collection::{CollectionAddr, QueryParams},
    context::*,
    error::ODataError,
};

pub async fn fixture(collection_elem: &str) -> Arc<ODataContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "covid19.canada",
        "examples/data/covid.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    ctx.register_parquet(
        "tickers.spy",
        "examples/data/tickers.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    Arc::new(ODataContext::new(
        ctx,
        "http://example.com/odata".to_string(),
        Some(CollectionAddr::decode(collection_elem).unwrap()),
    ))
}

///////////////////////////////////////////////////////////////////////////////

pub struct ODataContext {
    query_ctx: SessionContext,
    service_base_url: String,
    addr: Option<CollectionAddr>,
}

impl ODataContext {
    fn new(
        query_ctx: SessionContext,
        service_base_url: String,
        addr: Option<CollectionAddr>,
    ) -> Self {
        Self {
            query_ctx,
            service_base_url,
            addr,
        }
    }
}

#[async_trait::async_trait]
impl ServiceContext for ODataContext {
    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    async fn list_collections(&self) -> Result<Vec<Arc<dyn CollectionContext>>, ODataError> {
        let catalog_name = self.query_ctx.catalog_names().into_iter().next().unwrap();
        let catalog = self.query_ctx.catalog(&catalog_name).unwrap();

        let schema_name = catalog.schema_names().into_iter().next().unwrap();
        let schema = catalog.schema(&schema_name).unwrap();

        let mut table_names = schema.table_names();
        table_names.sort();

        let mut collections: Vec<Arc<dyn CollectionContext>> = Vec::new();
        for table_name in table_names {
            collections.push(Arc::new(ODataContext {
                query_ctx: self.query_ctx.clone(),
                service_base_url: self.service_base_url.clone(),
                addr: Some(CollectionAddr {
                    name: table_name,
                    key: None,
                }),
            }));
        }

        Ok(collections)
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Error
    }
}

#[async_trait::async_trait]
impl CollectionContext for ODataContext {
    fn addr(&self) -> Result<&CollectionAddr, ODataError> {
        Ok(self.addr.as_ref().unwrap())
    }

    fn service_base_url(&self) -> Result<String, ODataError> {
        Ok(self.service_base_url.clone())
    }

    fn collection_base_url(&self) -> Result<String, ODataError> {
        let service_base_url = &self.service_base_url;
        let collection_name = self.collection_name()?;
        Ok(format!("{service_base_url}{collection_name}"))
    }

    fn collection_name(&self) -> Result<String, ODataError> {
        Ok(self.addr()?.name.clone())
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
            .unwrap()
            .into()
    }

    async fn schema(&self) -> Result<SchemaRef, ODataError> {
        Ok(self
            .query_ctx
            .table_provider(TableReference::bare(self.collection_name()?))
            .await
            .map_err(|e| {
                ODataError::handle_no_table_as_collection_not_found(
                    self.collection_name().unwrap(),
                    e,
                )
            })?
            .schema())
    }

    async fn query(&self, query: QueryParams) -> Result<DataFrame, ODataError> {
        let df = self
            .query_ctx
            .table(TableReference::bare(self.collection_name()?))
            .await
            .map_err(|e| {
                ODataError::handle_no_table_as_collection_not_found(
                    self.collection_name().unwrap(),
                    e,
                )
            })?;

        query
            .apply(
                df,
                self.addr()?,
                "offset",
                &self.key_column_alias(),
                100,
                usize::MAX,
            )
            .map_err(ODataError::internal)
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Error
    }
}
