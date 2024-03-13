use std::sync::Arc;

use chrono::Utc;

use crate::{collection::*, context::*, metadata::*, service::*};

///////////////////////////////////////////////////////////////////////////////

pub const MEDIA_TYPE_ATOM: &str = "application/atom+xml;type=feed;charset=utf-8";
pub const MEDIA_TYPE_XML: &str = "application/xml;charset=utf-8";

const DEFAULT_COLLECTION_RESPONSE_SIZE: usize = 512_000;

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler(
    axum::Extension(odata_ctx): axum::Extension<Arc<dyn ServiceContext>>,
) -> axum::response::Response<String> {
    let mut collections = Vec::new();

    for (collection_name, _) in odata_ctx.list_collections().await {
        collections.push(Collection {
            href: collection_name.clone(),
            title: collection_name,
        })
    }

    let service = Service::new(
        odata_ctx.service_base_url(),
        Workspace {
            title: DEFAULT_NAMESPACE.to_string(),
            collections,
        },
    );

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("service", &service))
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler(
    axum::Extension(odata_ctx): axum::Extension<Arc<dyn ServiceContext>>,
) -> axum::response::Response<String> {
    let mut entity_types = Vec::new();
    let mut entity_container = EntityContainer {
        name: DEFAULT_NAMESPACE.to_string(),
        is_default: true,
        entity_set: Vec::new(),
    };

    for (collection_name, schema) in odata_ctx.list_collections().await {
        let mut properties = Vec::new();

        for field in schema.fields() {
            let p = Property::primitive(
                field.name(),
                to_edm_type(field.data_type()),
                field.is_nullable(),
            );

            properties.push(p);
        }

        entity_types.push(EntityType {
            name: collection_name.clone(),
            key: EntityKey::new(vec![PropertyRef {
                name: "offset".to_string(),
            }]),
            properties,
        });

        entity_container.entity_set.push(EntitySet {
            name: collection_name.clone(),
            entity_type: format!("{DEFAULT_NAMESPACE}.{collection_name}"),
        });
    }

    let metadata = Edmx::new(DataServices::new(vec![crate::metadata::Schema::new(
        DEFAULT_NAMESPACE.to_string(),
        entity_types,
        vec![entity_container],
    )]));

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("edmx:Edmx", &metadata))
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler(
    axum::Extension(ctx): axum::Extension<Arc<dyn CollectionContext>>,
    axum::extract::Query(query): axum::extract::Query<QueryParamsRaw>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response<String> {
    let query = query.decode();
    tracing::debug!(?query, ?headers, "Collection query");

    let df = ctx.query(query).await.unwrap();

    let schema: datafusion::arrow::datatypes::Schema = df.schema().clone().into();
    let record_batches = df.collect().await.unwrap();

    let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
    let raw_bytes: usize = record_batches
        .iter()
        .map(|b: &datafusion::arrow::array::RecordBatch| b.get_array_memory_size())
        .sum();

    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());
    crate::atom::write_atom_feed_from_records(
        &schema,
        record_batches,
        ctx.as_ref(),
        Utc::now(),
        &mut writer,
    )
    .unwrap();

    let body = String::from_utf8(writer.into_inner()).unwrap();

    tracing::debug!(
        media_type = MEDIA_TYPE_ATOM,
        num_rows,
        raw_bytes,
        xml_bytes = body.len(),
        "Prepared a response"
    );

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

fn write_object_to_xml<T>(tag: &str, object: &T) -> String
where
    T: serde::ser::Serialize,
{
    let mut writer =
        quick_xml::Writer::new(Vec::<u8>::with_capacity(DEFAULT_COLLECTION_RESPONSE_SIZE));

    writer
        .write_event(quick_xml::events::Event::Decl(
            quick_xml::events::BytesDecl::new("1.0", Some("utf-8"), None),
        ))
        .unwrap();

    writer.write_serializable(tag, object).unwrap();

    String::from_utf8(writer.into_inner()).unwrap()
}
