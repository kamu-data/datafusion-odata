use std::sync::Arc;

use axum::{extract::Query, response::Response, Extension};

use crate::{
    collection::QueryParamsRaw,
    context::{CollectionContext, OnUnsupported, ServiceContext, DEFAULT_NAMESPACE},
    error::{BatchUnexpectedRowsNumber, ODataError, UnexpectedBatchesNumber, UnsupportedDataType},
    metadata::{
        to_edm_type, DataServices, Edmx, EntityContainer, EntityKey, EntitySet, EntityType,
        Property, PropertyRef,
    },
    service::{Collection, Service, Workspace},
};

///////////////////////////////////////////////////////////////////////////////

pub const MEDIA_TYPE_ATOM: &str = "application/atom+xml;type=feed;charset=utf-8";
pub const MEDIA_TYPE_XML: &str = "application/xml;charset=utf-8";

const DEFAULT_COLLECTION_RESPONSE_SIZE: usize = 512_000;

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler(
    Extension(odata_ctx): Extension<Arc<dyn ServiceContext>>,
) -> Result<Response<String>, ODataError> {
    let mut collections = Vec::new();

    for coll in odata_ctx.list_collections().await? {
        collections.push(Collection {
            href: coll.collection_name()?,
            title: coll.collection_name()?,
        })
    }

    let service = Service::new(
        odata_ctx.service_base_url(),
        Workspace {
            title: DEFAULT_NAMESPACE.to_string(),
            collections,
        },
    );

    let xml = write_object_to_xml("service", &service)?;

    Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(xml)
        .map_err(ODataError::internal)
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler(
    Extension(odata_ctx): Extension<Arc<dyn ServiceContext>>,
) -> Result<Response<String>, ODataError> {
    let mut entity_types = Vec::new();
    let mut entity_container = EntityContainer {
        name: DEFAULT_NAMESPACE.to_string(),
        is_default: true,
        entity_set: Vec::new(),
    };

    for coll in odata_ctx.list_collections().await? {
        let collection_name = coll.collection_name()?;
        let mut properties = Vec::new();

        for field in coll.schema().await?.fields() {
            let typ = match to_edm_type(field.data_type()) {
                Ok(typ) => typ,
                Err(err) => match odata_ctx.on_unsupported_feature() {
                    OnUnsupported::Error => {
                        Err(UnsupportedDataType::new(field.data_type().clone()))?
                    }
                    OnUnsupported::Warn => {
                        tracing::error!(
                            table = collection_name,
                            field = field.name(),
                            error = %err,
                            error_dbg = ?err,
                            "Unsupported field type - skipping",
                        );
                        continue;
                    }
                },
            };

            properties.push(Property::primitive(field.name(), typ, field.is_nullable()));
        }

        // https://www.odata.org/documentation/odata-version-3-0/common-schema-definition-language-csdl/#csdl6.3
        let property_ref_name = match coll.key_column() {
            Ok(kc) => kc,
            Err(ODataError::KeyColumnNotAssigned(_)) => match properties.first() {
                Some(prop) => prop.name.clone(),
                None => collection_name.to_string(),
            },
            Err(err) => {
                tracing::error!(
                    table = collection_name,
                    error = %err,
                    error_dbg = ?err,
                    "Failed to get key column",
                );
                Err(err)?
            }
        };

        entity_types.push(EntityType {
            name: collection_name.clone(),
            key: EntityKey::new(vec![PropertyRef {
                name: property_ref_name,
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

    let xml = write_object_to_xml("edmx:Edmx", &metadata)?;

    Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(xml)
        .map_err(ODataError::internal)
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler(
    Extension(ctx): Extension<Arc<dyn CollectionContext>>,
    Query(query): Query<QueryParamsRaw>,
    _headers: axum::http::HeaderMap,
) -> Result<Response<String>, ODataError> {
    let query = query.decode();
    tracing::debug!(?query, "Decoded query");

    let df = ctx.query(query).await.map_err(ODataError::from)?;

    let schema: datafusion::arrow::datatypes::Schema = df.schema().clone().into();
    let record_batches = df.collect().await.map_err(ODataError::internal)?;

    let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
    let raw_bytes: usize = record_batches
        .iter()
        .map(|b: &datafusion::arrow::array::RecordBatch| b.get_array_memory_size())
        .sum();

    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());

    if ctx.addr()?.key.is_none() {
        crate::atom::write_atom_feed_from_records(
            &schema,
            record_batches,
            ctx.as_ref(),
            ctx.last_updated_time().await,
            &mut writer,
        )?;
    } else {
        let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
        if num_rows > 1 {
            return Err(BatchUnexpectedRowsNumber::new(num_rows).into());
        }

        if record_batches.len() > 1 {
            return Err(UnexpectedBatchesNumber::new(record_batches.len()).into());
        }

        let record_batch = match record_batches.into_iter().next() {
            Some(rb) => rb,
            None => {
                return Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(String::new())
                    .map_err(ODataError::internal);
            }
        };

        if record_batch.num_rows() != 1 {
            return Response::builder()
                .status(http::StatusCode::NOT_FOUND)
                .body(String::new())
                .map_err(ODataError::internal);
        }

        crate::atom::write_atom_entry_from_record(
            &schema,
            record_batch,
            ctx.as_ref(),
            ctx.last_updated_time().await,
            &mut writer,
        )?;
    }

    let body = String::from_utf8(writer.into_inner()).map_err(ODataError::internal)?;

    tracing::debug!(
        media_type = MEDIA_TYPE_ATOM,
        num_rows,
        raw_bytes,
        xml_bytes = body.len(),
        "Prepared a response"
    );

    Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .map_err(ODataError::internal)
}

///////////////////////////////////////////////////////////////////////////////

fn write_object_to_xml<T>(tag: &str, object: &T) -> Result<String, ODataError>
where
    T: serde::ser::Serialize,
{
    let mut writer =
        quick_xml::Writer::new(Vec::<u8>::with_capacity(DEFAULT_COLLECTION_RESPONSE_SIZE));

    writer
        .write_event(quick_xml::events::Event::Decl(
            quick_xml::events::BytesDecl::new("1.0", Some("utf-8"), None),
        ))
        .map_err(ODataError::internal)?;

    writer
        .write_serializable(tag, object)
        .map_err(ODataError::internal)?;

    Ok(String::from_utf8(writer.into_inner())?)
}
