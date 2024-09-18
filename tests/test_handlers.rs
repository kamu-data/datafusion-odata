mod shared;

use indoc::indoc;

use shared::fixture;

///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_service() {
    let ctx = fixture("tickers.spy").await;
    let resp = datafusion_odata::handlers::odata_service_handler(axum::Extension(ctx))
        .await
        .unwrap();
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
    let resp = datafusion_odata::handlers::odata_metadata_handler(axum::Extension(ctx))
        .await
        .unwrap();
    assert_eq!(
        *resp.body(),
        indoc!(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
            <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="3.0" m:MaxDataServiceVersion="3.0">
            <Schema Namespace="default" xmlns="http://schemas.microsoft.com/ado/2009/11/edm">
            <EntityType Name="covid19.canada">
            <Key><PropertyRef Name="offset"/></Key>
            <Property Name="offset" Type="Edm.Int64" Nullable="false"/>
            <Property Name="op" Type="Edm.Int32" Nullable="false"/>
            <Property Name="system_time" Type="Edm.DateTime" Nullable="false"/>
            <Property Name="reported_date" Type="Edm.DateTime" Nullable="false"/>
            <Property Name="province" Type="Edm.String" Nullable="false"/>
            <Property Name="total_daily" Type="Edm.Int64" Nullable="false"/>
            </EntityType>
            <EntityType Name="tickers.spy">
            <Key><PropertyRef Name="offset"/></Key>
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


