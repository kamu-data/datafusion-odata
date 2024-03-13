use datafusion::{arrow::datatypes::SchemaRef, dataframe::DataFrame};

use crate::collection::QueryParams;

///////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_NAMESPACE: &str = "default";

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ServiceContext: Send + Sync {
    fn service_base_url(&self) -> String;

    async fn list_collections(&self) -> Vec<(String, SchemaRef)>;
}

#[async_trait::async_trait]
pub trait CollectionContext: ServiceContext {
    async fn query(&self, query: QueryParams) -> datafusion::error::Result<DataFrame>;

    fn collection_name(&self) -> String;

    fn collection_namespace(&self) -> String {
        DEFAULT_NAMESPACE.to_string()
    }

    fn collection_base_url(&self) -> String;
}