mod shared;

use datafusion_odata::collection::QueryParamsRaw;
use indoc::indoc;

use shared::fixture;

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
            filter: None,
        }),
        axum::http::HeaderMap::new(),
    )
    .await
    .unwrap();
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
            filter: None,
        }),
        axum::http::HeaderMap::new(),
    )
    .await
    .unwrap();
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
            filter: None,
        }),
        axum::http::HeaderMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_collection_with_filter() {
    let ctx = fixture("tickers.spy").await;
    let resp = datafusion_odata::handlers::odata_collection_handler(
        axum::Extension(ctx),
        axum::extract::Query(QueryParamsRaw {
            select: Some("offset,close".to_string()),
            order_by: Some("offset asc".to_string()),
            skip: None,
            top: None,
            filter: Some("offset eq 0".to_string()),
        }),
        axum::http::HeaderMap::new(),
    )
    .await
    .unwrap();
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
            </feed>
            "#
        )
        .replace('\n', "")
    );
}
