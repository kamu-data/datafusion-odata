use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{DataType, *},
};
use quick_xml::events::*;

use crate::{
    context::{CollectionContext, OnUnsupported},
    error::{ODataError, UnsupportedColumnType},
};

///////////////////////////////////////////////////////////////////////////////

// https://www.odata.org/documentation/odata-version-3-0/atom-format/
//
// <?xml version="1.0" encoding="utf-8"?>
// <feed
//   xml:base="http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/"
//   xmlns="http://www.w3.org/2005/Atom"
//   xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
//   xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
//
//   <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy/</id>
//   <title type="text">tickers_spy</title>
//   <updated>2024-03-10T00:36:45Z</updated>
//   <link rel="self" title="tickers_spy" href="tickers_spy" />
//
//   <entry>
//     <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy(0)</id>
//     <category term="ODataDemo.tickers_spy" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
//     <link rel="edit" title="tickers_spy" href="tickers_spy(0)" />
//     <title />
//     <updated>2024-03-10T00:36:45Z</updated>
//     <author>
//       <name />
//     </author>
//     <content type="application/xml">
//       <m:properties>
//         <d:offset m:type="Edm.Int64">0</d:offset>
//         <d:from_symbol m:type="Edm.String">spy</d:from_symbol>
//         <d:to_symbol m:type="Edm.String">usd</d:to_symbol>
//         <d:close m:type="Edm.Double">135.5625</d:close>
//       </m:properties>
//     </content>
//   </entry>
//   <entry>
//     <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy(1)</id>
//     <category term="ODataDemo.tickers_spy" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
//     <link rel="edit" title="tickers_spy" href="tickers_spy(1)" />
//     <title />
//     <updated>2024-03-10T00:36:45Z</updated>
//     <author>
//       <name />
//     </author>
//     <content type="application/xml">
//       <m:properties>
//         <d:offset m:type="Edm.Int64">1</d:offset>
//         <d:from_symbol m:type="Edm.String">spy</d:from_symbol>
//         <d:to_symbol m:type="Edm.String">usd</d:to_symbol>
//         <d:close m:type="Edm.Double">136.5622</d:close>
//       </m:properties>
//     </content>
//   </entry>
// </feed>
//
// TODO: Use erased dyn Writer type
// TODO: Extract `CollectionInfo` type to avoid propagating
//       a bunch of individual parameters
pub fn write_atom_feed_from_records<W>(
    schema: &Schema,
    record_batches: Vec<RecordBatch>,
    ctx: &dyn CollectionContext,
    updated_time: DateTime<Utc>,
    on_unsupported: OnUnsupported,
    writer: &mut quick_xml::Writer<W>,
) -> Result<(), ODataError>
where
    W: std::io::Write,
{
    let mut service_base_url = ctx.service_base_url()?;
    let mut collection_base_url = ctx.collection_base_url()?;
    let collection_name = ctx.collection_name()?;
    let type_name = ctx.collection_name()?;
    let type_namespace = ctx.collection_namespace()?;

    assert!(service_base_url.starts_with("http"));
    assert!(collection_base_url.starts_with("http"));
    if !service_base_url.ends_with('/') {
        service_base_url.push('/');
    }
    if collection_base_url.ends_with('/') {
        collection_base_url.pop();
    }

    let fq_type = format!("{type_namespace}.{type_name}");

    let mut columns = Vec::new();
    let mut id_column_index = usize::MAX;

    for (index, field) in schema.fields().iter().enumerate() {
        let name = field.name().clone();
        if name == ctx.key_column_alias() {
            id_column_index = index;
            continue;
        }
        let tag = format!("d:{name}");
        let typ = match super::metadata::to_edm_type(field.data_type()) {
            Ok(typ) => typ,
            Err(err) => match on_unsupported {
                OnUnsupported::Error => panic!("{}", err),
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

        columns.push((index, name, tag, typ));
    }

    writer.write_event(quick_xml::events::Event::Decl(BytesDecl::new(
        "1.0",
        Some("utf-8"),
        None,
    )))?;

    let mut feed = BytesStart::new("feed");
    feed.push_attribute(("xml:base", service_base_url.as_str()));
    feed.push_attribute(("xmlns", "http://www.w3.org/2005/Atom"));
    feed.push_attribute((
        "xmlns:d",
        "http://schemas.microsoft.com/ado/2007/08/dataservices",
    ));
    feed.push_attribute((
        "xmlns:m",
        "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    ));

    writer.write_event(Event::Start(feed))?;

    // <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy/</id>
    // <title type="text">tickers_spy</title>
    // <updated>2024-03-10T00:36:45Z</updated>
    // <link rel="self" title="tickers_spy" href="tickers_spy" />
    writer
        .create_element("id")
        .write_text_content(BytesText::from_escaped(&collection_base_url))?;
    writer
        .create_element("title")
        .with_attribute(("type", "text"))
        .write_text_content(BytesText::from_escaped(&collection_name))?;
    writer
        .create_element("updated")
        .write_text_content(encode_date_time(&updated_time))?;
    writer
        .create_element("link")
        .with_attributes([
            ("rel", "self"),
            ("title", collection_name.as_str()),
            ("href", collection_name.as_str()),
        ])
        .write_empty()?;

    for batch in record_batches {
        for row in 0..batch.num_rows() {
            writer.write_event(Event::Start(BytesStart::new("entry")))?;

            // <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy(1)</id>
            // <category term="ODataDemo.tickers_spy" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
            // <link rel="edit" title="tickers_spy" href="tickers_spy(1)" />
            // <title />
            // <updated>2024-03-10T00:36:45Z</updated>
            // <author>
            //   <name />
            // </author>

            let id = encode_primitive_dyn(batch.column(id_column_index), row)?
                .unescape()
                .unwrap();

            let entry_url_rel = format!("{collection_name}({id})");
            let entry_url_full = format!("{collection_base_url}({id})");

            writer
                .create_element("id")
                .write_text_content(BytesText::from_escaped(entry_url_full))?;
            writer
                .create_element("category")
                .with_attributes([
                    (
                        "scheme",
                        "http://schemas.microsoft.com/ado/2007/08/dataservices/scheme",
                    ),
                    ("term", &fq_type),
                ])
                .write_empty()?;
            writer
                .create_element("link")
                .with_attributes([
                    ("rel", "edit"),
                    ("title", &collection_name),
                    ("href", &entry_url_rel),
                ])
                .write_empty()?;
            writer.create_element("title").write_empty()?;
            writer
                .create_element("updated")
                .write_text_content(encode_date_time(&updated_time))?;
            writer.write_event(Event::Start(BytesStart::new("author")))?;
            writer.create_element("name").write_empty()?;
            writer.write_event(Event::End(BytesEnd::new("author")))?;

            // <content type="application/xml">
            //   <m:properties>
            //     <d:offset m:type="Edm.Int64">1</d:offset>
            //     <d:from_symbol m:type="Edm.String">spy</d:from_symbol>
            //     <d:to_symbol m:type="Edm.String">usd</d:to_symbol>
            //     <d:close m:type="Edm.Double">136.5622</d:close>
            //   </m:properties>
            // </content>
            writer.write_event(Event::Start(
                BytesStart::new("content").with_attributes([("type", "application/xml")]),
            ))?;
            writer.write_event(Event::Start(BytesStart::new("m:properties")))?;

            for (i, _cname, ctag, typ) in &columns {
                let col = batch.column(*i);

                let mut start = BytesStart::new(ctag);
                start.push_attribute(("m:type", *typ));
                writer.write_event(Event::Start(start))?;
                writer.write_event(Event::Text(encode_primitive_dyn(col, row)?))?;
                writer.write_event(Event::End(BytesEnd::new(ctag)))?;
            }

            writer.write_event(Event::End(BytesEnd::new("m:properties")))?;
            writer.write_event(Event::End(BytesEnd::new("content")))?;
            writer.write_event(Event::End(BytesEnd::new("entry")))?;
        }
    }

    writer.write_event(Event::End(BytesEnd::new("feed")))?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

// https://www.odata.org/documentation/odata-version-3-0/atom-format/
//
// <?xml version="1.0" encoding="utf-8"?>
// <entry
//   xml:base="http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/"
//   xmlns="http://www.w3.org/2005/Atom"
//   xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
//   xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
//   <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy(0)</id>
//   <category term="ODataDemo.tickers_spy" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
//   <link rel="edit" title="tickers_spy" href="tickers_spy(0)" />
//   <title />
//   <updated>2024-03-10T00:36:45Z</updated>
//   <author>
//     <name />
//   </author>
//   <content type="application/xml">
//     <m:properties>
//       <d:offset m:type="Edm.Int64">0</d:offset>
//       <d:from_symbol m:type="Edm.String">spy</d:from_symbol>
//       <d:to_symbol m:type="Edm.String">usd</d:to_symbol>
//       <d:close m:type="Edm.Double">135.5625</d:close>
//     </m:properties>
//   </content>
// </entry>
// TODO: Use erased dyn Writer type
// TODO: Extract `CollectionInfo` type to avoid propagating
//       a bunch of individual parameters
pub fn write_atom_entry_from_record<W>(
    schema: &Schema,
    batch: RecordBatch,
    ctx: &dyn CollectionContext,
    updated_time: DateTime<Utc>,
    on_unsupported: OnUnsupported,
    writer: &mut quick_xml::Writer<W>,
) -> Result<(), ODataError>
where
    W: std::io::Write,
{
    assert_eq!(batch.num_rows(), 1);

    let mut service_base_url = ctx.service_base_url()?;
    let mut collection_base_url = ctx.collection_base_url()?;
    let collection_name = ctx.collection_name()?;
    let type_name = ctx.collection_name()?;
    let type_namespace = ctx.collection_namespace()?;

    assert!(service_base_url.starts_with("http"));
    assert!(collection_base_url.starts_with("http"));
    if !service_base_url.ends_with('/') {
        service_base_url.push('/');
    }
    if collection_base_url.ends_with('/') {
        collection_base_url.pop();
    }

    let fq_type = format!("{type_namespace}.{type_name}");

    let mut columns = Vec::new();
    let mut id_column_index = usize::MAX;

    for (index, field) in schema.fields().iter().enumerate() {
        let name = field.name().clone();
        if name == ctx.key_column_alias() {
            id_column_index = index;
            continue;
        }
        let tag = format!("d:{name}");
        let typ = match super::metadata::to_edm_type(field.data_type()) {
            Ok(typ) => typ,
            Err(err) => match on_unsupported {
                OnUnsupported::Error => panic!("{}", err),
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

        columns.push((index, name, tag, typ));
    }

    writer.write_event(quick_xml::events::Event::Decl(BytesDecl::new(
        "1.0",
        Some("utf-8"),
        None,
    )))?;

    let mut entry = BytesStart::new("entry");
    entry.push_attribute(("xml:base", service_base_url.as_str()));
    entry.push_attribute(("xmlns", "http://www.w3.org/2005/Atom"));
    entry.push_attribute((
        "xmlns:d",
        "http://schemas.microsoft.com/ado/2007/08/dataservices",
    ));
    entry.push_attribute((
        "xmlns:m",
        "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    ));

    writer.write_event(Event::Start(entry))?;

    // <id>http://a5d4b8ec90d5144a08efb47e789d49d5-1706314482.us-west-2.elb.amazonaws.com/tickers_spy(1)</id>
    // <category term="ODataDemo.tickers_spy" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
    // <link rel="edit" title="tickers_spy" href="tickers_spy(1)" />
    // <title />
    // <updated>2024-03-10T00:36:45Z</updated>
    // <author>
    //   <name />
    // </author>

    let row = 0;
    let id = encode_primitive_dyn(batch.column(id_column_index), row)?
        .unescape()
        .unwrap();

    let entry_url_rel = format!("{collection_name}({id})");
    let entry_url_full = format!("{collection_base_url}({id})");

    writer
        .create_element("id")
        .write_text_content(BytesText::from_escaped(entry_url_full))?;
    writer
        .create_element("category")
        .with_attributes([
            (
                "scheme",
                "http://schemas.microsoft.com/ado/2007/08/dataservices/scheme",
            ),
            ("term", &fq_type),
        ])
        .write_empty()?;
    writer
        .create_element("link")
        .with_attributes([
            ("rel", "edit"),
            ("title", &collection_name),
            ("href", &entry_url_rel),
        ])
        .write_empty()?;
    writer.create_element("title").write_empty()?;
    writer
        .create_element("updated")
        .write_text_content(encode_date_time(&updated_time))?;
    writer.write_event(Event::Start(BytesStart::new("author")))?;
    writer.create_element("name").write_empty()?;
    writer.write_event(Event::End(BytesEnd::new("author")))?;

    // <content type="application/xml">
    //   <m:properties>
    //     <d:offset m:type="Edm.Int64">1</d:offset>
    //     <d:from_symbol m:type="Edm.String">spy</d:from_symbol>
    //     <d:to_symbol m:type="Edm.String">usd</d:to_symbol>
    //     <d:close m:type="Edm.Double">136.5622</d:close>
    //   </m:properties>
    // </content>
    writer.write_event(Event::Start(
        BytesStart::new("content").with_attributes([("type", "application/xml")]),
    ))?;
    writer.write_event(Event::Start(BytesStart::new("m:properties")))?;

    for (i, _cname, ctag, typ) in &columns {
        let col = batch.column(*i);

        let mut start = BytesStart::new(ctag);
        start.push_attribute(("m:type", *typ));
        writer.write_event(Event::Start(start))?;
        writer.write_event(Event::Text(encode_primitive_dyn(col, row)?))?;
        writer.write_event(Event::End(BytesEnd::new(ctag)))?;
    }

    writer.write_event(Event::End(BytesEnd::new("m:properties")))?;
    writer.write_event(Event::End(BytesEnd::new("content")))?;
    writer.write_event(Event::End(BytesEnd::new("entry")))?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

fn encode_primitive_dyn(
    col: &Arc<dyn Array>,
    row: usize,
) -> Result<BytesText, UnsupportedColumnType> {
    let col_type = col.data_type().clone();
    if col.is_null(row) {
        Ok(BytesText::new("null"))
    } else {
        match col_type {
            DataType::Null => Err(UnsupportedColumnType::new(col_type)),
            DataType::Boolean => {
                let arr = col.as_boolean();
                let val = arr.value(row).to_string();
                Ok(BytesText::from_escaped(val))
            }
            DataType::Int8 => Ok(encode_primitive::<Int8Type>(col, row)),
            DataType::Int16 => Ok(encode_primitive::<Int16Type>(col, row)),
            DataType::Int32 => Ok(encode_primitive::<Int32Type>(col, row)),
            DataType::Int64 => Ok(encode_primitive::<Int64Type>(col, row)),
            DataType::UInt8 => Ok(encode_primitive::<UInt8Type>(col, row)),
            DataType::UInt16 => Ok(encode_primitive::<UInt16Type>(col, row)),
            DataType::UInt32 => Ok(encode_primitive::<UInt32Type>(col, row)),
            DataType::UInt64 => Ok(encode_primitive::<UInt64Type>(col, row)),
            DataType::Float16 => Ok(encode_primitive::<Float16Type>(col, row)),
            DataType::Float32 => Ok(encode_primitive::<Float32Type>(col, row)),
            DataType::Float64 => Ok(encode_primitive::<Float64Type>(col, row)),
            DataType::Timestamp(_, _) => {
                let arr = col.as_primitive::<TimestampMillisecondType>();
                let ticks = arr.value(row);
                let ts = chrono::DateTime::from_timestamp_millis(ticks).unwrap();
                Ok(encode_date_time(&ts))
            }
            DataType::Date32 => Err(UnsupportedColumnType::new(col_type)),
            DataType::Date64 => {
                let arr = col.as_primitive::<Date64Type>();
                let ticks = arr.value(row);
                let ts = chrono::DateTime::from_timestamp_millis(ticks).unwrap();
                Ok(encode_date_time(&ts))
            }
            DataType::Time32(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Time64(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Duration(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Interval(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Binary => Err(UnsupportedColumnType::new(col_type)),
            DataType::FixedSizeBinary(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::LargeBinary => Err(UnsupportedColumnType::new(col_type)),
            DataType::BinaryView => Err(UnsupportedColumnType::new(col_type)),
            DataType::Utf8 => {
                let arr = col.as_string::<i32>();
                let val = arr.value(row);
                Ok(BytesText::from_escaped(quick_xml::escape::escape(val)))
            }
            DataType::LargeUtf8 => {
                let arr = col.as_string::<i64>();
                let val = arr.value(row);
                Ok(BytesText::from_escaped(quick_xml::escape::escape(val)))
            }
            DataType::Utf8View => Err(UnsupportedColumnType::new(col_type)),
            DataType::List(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::FixedSizeList(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::LargeList(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::ListView(_) | DataType::LargeListView(_) => {
                Err(UnsupportedColumnType::new(col_type))
            }
            DataType::Struct(_) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Union(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Dictionary(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Decimal128(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Decimal256(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::Map(_, _) => Err(UnsupportedColumnType::new(col_type)),
            DataType::RunEndEncoded(_, _) => Err(UnsupportedColumnType::new(col_type)),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

fn encode_primitive<T>(arr: &Arc<dyn Array>, row: usize) -> BytesText
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: std::fmt::Display,
{
    let arr = arr.as_primitive::<T>();
    let val = arr.value(row).to_string();
    BytesText::from_escaped(val)
}

///////////////////////////////////////////////////////////////////////////////

fn encode_date_time(dt: &DateTime<Utc>) -> BytesText<'static> {
    let s = dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    BytesText::from_escaped(s)
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::{
        array::{Array, Date64Array, Int64Array},
        datatypes::{ArrowPrimitiveType, Date64Type},
    };

    #[test]
    fn test_encode_primitive_dyn() {
        let values: Int64Array = vec![1, 2, 3].into();
        let values = Arc::new(values) as Arc<dyn Array>;

        let result = encode_primitive_dyn(&values, 0).unwrap();
        assert_eq!(result, BytesText::new("1"));

        let values = [chrono::DateTime::from_timestamp_millis(1726012800000).unwrap()];
        let values: Date64Array = values
            .iter()
            .map(|d| Date64Type::from_naive_date(d.date_naive()))
            .collect::<Vec<<Date64Type as ArrowPrimitiveType>::Native>>()
            .into();
        let values = Arc::new(values) as Arc<dyn Array>;

        let result = encode_primitive_dyn(&values, 0).unwrap();
        assert_eq!(result.borrow(), BytesText::new("2024-09-11T00:00:00.000Z"));
    }
}
