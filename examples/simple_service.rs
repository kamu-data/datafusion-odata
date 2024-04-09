use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::{prelude::*, sql::TableReference};

use datafusion_odata::collection::*;
use datafusion_odata::context::*;
use datafusion_odata::handlers::*;

///////////////////////////////////////////////////////////////////////////////

const DEFAULT_MAX_ROWS: usize = 100;

///////////////////////////////////////////////////////////////////////////////
// Real handlers
// Wrap the library-provided handlers in order to extract load balancer hostname from HTTP request.
///////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler(
    axum::extract::State(query_ctx): axum::extract::State<SessionContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
) -> axum::response::Response<String> {
    let ctx = Arc::new(ODataContext::new_service(query_ctx, host));
    datafusion_odata::handlers::odata_service_handler(axum::Extension(ctx)).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler(
    axum::extract::State(query_ctx): axum::extract::State<SessionContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
) -> axum::response::Response<String> {
    let ctx = ODataContext::new_service(query_ctx, host);
    datafusion_odata::handlers::odata_metadata_handler(axum::Extension(Arc::new(ctx))).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler(
    axum::extract::State(query_ctx): axum::extract::State<SessionContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
    axum::extract::Path(collection_path_element): axum::extract::Path<String>,
    query: axum::extract::Query<QueryParamsRaw>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response<String> {
    let Some(addr) = CollectionAddr::decode(&collection_path_element) else {
        return axum::response::Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body("".into())
            .unwrap();
    };

    let ctx = Arc::new(ODataContext::new_collection(query_ctx, host, addr));
    datafusion_odata::handlers::odata_collection_handler(axum::Extension(ctx), query, headers).await
}

///////////////////////////////////////////////////////////////////////////////
// Service and Collection context object.
// Provides our URL layout to the library.
// Knows how to map Datafusion tables to collection names and vice versa.
///////////////////////////////////////////////////////////////////////////////

pub struct ODataContext {
    query_ctx: SessionContext,
    service_base_url: String,
    addr: Option<CollectionAddr>,
}

impl ODataContext {
    fn new_service(query_ctx: SessionContext, host: axum::headers::Host) -> Self {
        let scheme = std::env::var("SCHEME").unwrap_or("http".to_string());
        Self {
            query_ctx,
            service_base_url: format!("{scheme}://{host}/"),
            addr: None,
        }
    }

    fn new_collection(
        query_ctx: SessionContext,
        host: axum::headers::Host,
        addr: CollectionAddr,
    ) -> Self {
        let mut this = Self::new_service(query_ctx, host);
        this.addr = Some(addr);
        this
    }
}

#[async_trait::async_trait]
impl ServiceContext for ODataContext {
    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    async fn list_collections(&self) -> Vec<Arc<dyn CollectionContext>> {
        let cnames = self.query_ctx.catalog_names();
        assert_eq!(
            cnames.len(),
            1,
            "Multiple catalogs not supported: {:?}",
            cnames
        );
        let catalog_name = cnames.first().unwrap();
        let catalog = self.query_ctx.catalog(catalog_name).unwrap();

        let snames = catalog.schema_names();
        assert_eq!(
            snames.len(),
            1,
            "Multiple schemas not supported: {:?}",
            snames
        );
        let schema_name = snames.first().unwrap();
        let schema = catalog.schema(schema_name).unwrap();

        let mut collections: Vec<Arc<dyn CollectionContext>> = Vec::new();
        for table_name in schema.table_names() {
            collections.push(Arc::new(ODataContext {
                query_ctx: self.query_ctx.clone(),
                service_base_url: self.service_base_url.clone(),
                addr: Some(CollectionAddr {
                    name: table_name,
                    key: None,
                }),
            }));
        }

        collections
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Error
    }
}

#[async_trait::async_trait]
impl CollectionContext for ODataContext {
    fn addr(&self) -> &CollectionAddr {
        self.addr.as_ref().unwrap()
    }

    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    fn collection_base_url(&self) -> String {
        let service_base_url = &self.service_base_url;
        let collection_name = self.collection_name();
        format!("{service_base_url}{collection_name}")
    }

    fn collection_name(&self) -> String {
        self.addr.as_ref().unwrap().name.clone()
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn schema(&self) -> SchemaRef {
        self.query_ctx
            .table_provider(TableReference::bare(self.collection_name()))
            .await
            .unwrap()
            .schema()
    }

    async fn query(&self, query: QueryParams) -> datafusion::error::Result<DataFrame> {
        let df = self
            .query_ctx
            .table(TableReference::bare(self.collection_name()))
            .await?;

        query.apply(
            df,
            self.addr(),
            "offset",
            &self.key_column_alias(),
            DEFAULT_MAX_ROWS,
            usize::MAX,
        )
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Error
    }
}

///////////////////////////////////////////////////////////////////////////////
// Mock handlers (to simplify hacking responses)
///////////////////////////////////////////////////////////////////////////////

async fn mock_odata_service_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/service.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(body)
        .unwrap()
}

async fn mock_odata_metadata_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/metadata.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(body)
        .unwrap()
}

async fn mock_odata_collection_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/collection.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("Initializing");

    ///////////////////////////

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

    ///////////////////////////

    let app = axum::Router::new()
        // Mock
        .route("/mock", axum::routing::get(mock_odata_service_handler))
        .route("/mock/", axum::routing::get(mock_odata_service_handler))
        .route(
            "/mock/$metadata",
            axum::routing::get(mock_odata_metadata_handler),
        )
        .route(
            "/mock/:collection",
            axum::routing::get(mock_odata_collection_handler),
        )
        // Real
        .route("/", axum::routing::get(odata_service_handler))
        .route("/$metadata", axum::routing::get(odata_metadata_handler))
        .route("/:collection", axum::routing::get(odata_collection_handler))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(vec![http::Method::GET, http::Method::POST])
                .allow_headers(tower_http::cors::Any),
        )
        .with_state(ctx);

    tracing::info!("Runninng on http://localhost:3000/");
    let server = axum::Server::bind(&([0, 0, 0, 0], 3000).into()).serve(app.into_make_service());

    if let Err(err) = server.await {
        eprintln!("server error: {}", err);
    }
}
