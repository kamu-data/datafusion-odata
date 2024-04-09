use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::{arrow::datatypes::SchemaRef, prelude::*, sql::TableReference};
use datafusion_odata::{
    collection::{CollectionAddr, QueryParams, QueryParamsRaw},
    context::*,
};
use indoc::indoc;

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_service() {
    let ctx = fixture("tickers.spy").await;
    let resp = datafusion_odata::handlers::odata_service_handler(axum::Extension(ctx)).await;
    assert_eq!(
        *resp.body(),
        indoc!(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <service xml:base="http://example.com/odata"
             xmlns="http://www.w3.org/2007/app"
             xmlns:atom="http://www.w3.org/2005/Atom">
            <workspace>
            <atom:title>default</atom:title>
            <collection href="covid19.canada">
            <atom:title>covid19.canada</atom:title>
            </collection>
            <collection href="tickers.spy">
            <atom:title>tickers.spy</atom:title>
            </collection>
            </workspace>
            </service>
            "#
        )
        .replace('\n', "")
    );
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_metadata() {
    let ctx = fixture("tickers.spy").await;
    let resp = datafusion_odata::handlers::odata_metadata_handler(axum::Extension(ctx)).await;
    assert_eq!(
        *resp.body(),
        indoc!(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
            <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="3.0" m:MaxDataServiceVersion="3.0">
            <Schema Namespace="default" xmlns="http://schemas.microsoft.com/ado/2009/11/edm">
            <EntityType Name="covid19.canada">
            <Key><PropertyRef Name="covid19.canada"/></Key>
            <Property Name="offset" Type="Edm.Int64" Nullable="false"/>
            <Property Name="op" Type="Edm.Int32" Nullable="false"/>
            <Property Name="system_time" Type="Edm.DateTime" Nullable="false"/>
            <Property Name="reported_date" Type="Edm.DateTime" Nullable="false"/>
            <Property Name="province" Type="Edm.String" Nullable="false"/>
            <Property Name="total_daily" Type="Edm.Int64" Nullable="false"/>
            </EntityType>
            <EntityType Name="tickers.spy">
            <Key><PropertyRef Name="tickers.spy"/></Key>
            <Property Name="offset" Type="Edm.Int64" Nullable="true"/>
            <Property Name="op" Type="Edm.Int32" Nullable="false"/>
            <Property Name="system_time" Type="Edm.DateTime" Nullable="false"/>
            <Property Name="event_time" Type="Edm.DateTime" Nullable="true"/>
            <Property Name="from_symbol" Type="Edm.String" Nullable="false"/>
            <Property Name="to_symbol" Type="Edm.String" Nullable="false"/>
            <Property Name="open" Type="Edm.Double" Nullable="true"/>
            <Property Name="high" Type="Edm.Double" Nullable="true"/>
            <Property Name="low" Type="Edm.Double" Nullable="true"/>
            <Property Name="close" Type="Edm.Double" Nullable="true"/>
            <Property Name="volume" Type="Edm.Double" Nullable="true"/>
            </EntityType>
            <EntityContainer Name="default" m:IsDefaultEntityContainer="true">
            <EntitySet Name="covid19.canada" EntityType="default.covid19.canada"/>
            <EntitySet Name="tickers.spy" EntityType="default.tickers.spy"/>
            </EntityContainer>
            </Schema>
            </edmx:DataServices>
            </edmx:Edmx>
            "#
        )
        .replace('\n', "")
    );
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_collection() {
    let ctx = fixture("tickers.spy").await;
    let resp = datafusion_odata::handlers::odata_collection_handler(
        axum::Extension(ctx),
        axum::extract::Query(QueryParamsRaw {
            select: Some("offset,close".to_string()),
            order_by: Some("offset asc".to_string()),
            skip: None,
            top: Some(2),
        }),
        axum::http::HeaderMap::new(),
    )
    .await;
    assert_eq!(
        *resp.body(),
        indoc!(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <feed
             xml:base="http://example.com/odata/"
             xmlns="http://www.w3.org/2005/Atom"
             xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
             xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
            <id>http://example.com/odatatickers.spy</id>
            <title type="text">tickers.spy</title>
            <updated>2023-01-01T00:00:00.000Z</updated>
            <link rel="self" title="tickers.spy" href="tickers.spy"/>
            <entry>
            <id>http://example.com/odatatickers.spy(0)</id>
            <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.tickers.spy"/>
            <link rel="edit" title="tickers.spy" href="tickers.spy(0)"/>
            <title/>
            <updated>2023-01-01T00:00:00.000Z</updated>
            <author><name/></author>
            <content type="application/xml">
            <m:properties>
            <d:offset m:type="Edm.Int64">0</d:offset>
            <d:close m:type="Edm.Double">135.5625</d:close>
            </m:properties>
            </content>
            </entry>
            <entry>
            <id>http://example.com/odatatickers.spy(1)</id>
            <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.tickers.spy"/>
            <link rel="edit" title="tickers.spy" href="tickers.spy(1)"/>
            <title/>
            <updated>2023-01-01T00:00:00.000Z</updated>
            <author><name/></author>
            <content type="application/xml">
            <m:properties>
            <d:offset m:type="Edm.Int64">1</d:offset>
            <d:close m:type="Edm.Double">134.5937</d:close>
            </m:properties>
            </content>
            </entry>
            </feed>
            "#
        )
        .replace('\n', "")
    );
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_collection_entity_by_id() {
    let ctx = fixture("tickers.spy(1)").await;
    let resp = datafusion_odata::handlers::odata_collection_handler(
        axum::Extension(ctx),
        axum::extract::Query(QueryParamsRaw {
            select: Some("offset,close".to_string()),
            order_by: None,
            skip: None,
            top: None,
        }),
        axum::http::HeaderMap::new(),
    )
    .await;
    assert_eq!(
        *resp.body(),
        indoc!(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <entry
             xml:base="http://example.com/odata/"
             xmlns="http://www.w3.org/2005/Atom"
             xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
             xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
            <id>http://example.com/odatatickers.spy(1)</id>
            <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.tickers.spy"/>
            <link rel="edit" title="tickers.spy" href="tickers.spy(1)"/>
            <title/>
            <updated>2023-01-01T00:00:00.000Z</updated>
            <author><name/></author>
            <content type="application/xml">
            <m:properties>
            <d:offset m:type="Edm.Int64">1</d:offset>
            <d:close m:type="Edm.Double">134.5937</d:close>
            </m:properties>
            </content>
            </entry>
            "#
        )
        .replace('\n', "")
    );
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_collection_entity_by_id_not_found() {
    let ctx = fixture("tickers.spy(999999)").await;
    let resp = datafusion_odata::handlers::odata_collection_handler(
        axum::Extension(ctx),
        axum::extract::Query(QueryParamsRaw {
            select: Some("offset,close".to_string()),
            order_by: None,
            skip: None,
            top: None,
        }),
        axum::http::HeaderMap::new(),
    )
    .await;
    assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
}

///////////////////////////////////////////////////////////////////////////////

async fn fixture(collection_elem: &str) -> Arc<ODataContext> {
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

    async fn list_collections(&self) -> Vec<Arc<dyn CollectionContext>> {
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
        self.addr().name.clone()
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
            .unwrap()
            .into()
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
            100,
            usize::MAX,
        )
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Error
    }
}
