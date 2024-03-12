use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{DataType, *},
};
use quick_xml::events::*;

use crate::context::CollectionContext;

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
pub fn write_atom_feed_from_records<W>(
    schema: &Schema,
    record_batches: Vec<RecordBatch>,
    ctx: &dyn CollectionContext,
    updated_time: DateTime<Utc>,
    writer: &mut quick_xml::Writer<W>,
) -> quick_xml::Result<()>
where
    W: std::io::Write,
{
    let service_base_url = ctx.service_base_url();
    let collection_base_url = ctx.collection_base_url();
    let collection_name = ctx.collection_name();
    let type_name = ctx.collection_name();
    let type_namespace = ctx.collection_namespace();

    assert!(service_base_url.starts_with("http"));
    assert!(collection_base_url.starts_with("http"));
    assert!(service_base_url.ends_with('/'));
    assert!(!collection_base_url.ends_with('/'));

    let fq_type = format!("{type_namespace}.{type_name}");

    let columns: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let column_tags: Vec<_> = columns.iter().map(|c| format!("d:{c}")).collect();

    let column_types: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| super::metadata::to_edm_type(f.data_type()))
        .collect();

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

            // TODO: Extract ID from offset
            let id = row;
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

            for (i, (ctag, typ)) in column_tags.iter().zip(&column_types).enumerate() {
                let col = batch.column(i);

                let mut start = BytesStart::new(ctag);
                start.push_attribute(("m:type", *typ));
                writer.write_event(Event::Start(start))?;

                let val = if col.is_null(row) {
                    BytesText::new("null")
                } else {
                    match col.data_type() {
                        DataType::Null => todo!(),
                        DataType::Boolean => {
                            let arr = col.as_boolean();
                            let val = arr.value(row).to_string();
                            BytesText::from_escaped(val)
                        }
                        DataType::Int8 => encode_primitive::<Int8Type>(col, row),
                        DataType::Int16 => encode_primitive::<Int16Type>(col, row),
                        DataType::Int32 => encode_primitive::<Int32Type>(col, row),
                        DataType::Int64 => encode_primitive::<Int64Type>(col, row),
                        DataType::UInt8 => encode_primitive::<UInt8Type>(col, row),
                        DataType::UInt16 => encode_primitive::<UInt16Type>(col, row),
                        DataType::UInt32 => encode_primitive::<UInt32Type>(col, row),
                        DataType::UInt64 => encode_primitive::<UInt64Type>(col, row),
                        DataType::Float16 => encode_primitive::<Float16Type>(col, row),
                        DataType::Float32 => encode_primitive::<Float32Type>(col, row),
                        DataType::Float64 => encode_primitive::<Float64Type>(col, row),
                        DataType::Timestamp(_, _) => {
                            let arr = col.as_primitive::<TimestampMillisecondType>();
                            let ticks = arr.value(row);
                            let ts = chrono::DateTime::from_timestamp_millis(ticks).unwrap();
                            encode_date_time(&ts)
                        }
                        DataType::Date32 => todo!(),
                        DataType::Date64 => todo!(),
                        DataType::Time32(_) => todo!(),
                        DataType::Time64(_) => todo!(),
                        DataType::Duration(_) => todo!(),
                        DataType::Interval(_) => todo!(),
                        DataType::Binary => todo!(),
                        DataType::FixedSizeBinary(_) => todo!(),
                        DataType::LargeBinary => todo!(),
                        DataType::Utf8 => {
                            let arr = col.as_string::<i32>();
                            let val = arr.value(row);
                            BytesText::from_escaped(quick_xml::escape::escape(val))
                        }
                        DataType::LargeUtf8 => todo!(),
                        DataType::List(_) => todo!(),
                        DataType::FixedSizeList(_, _) => todo!(),
                        DataType::LargeList(_) => todo!(),
                        DataType::Struct(_) => todo!(),
                        DataType::Union(_, _) => todo!(),
                        DataType::Dictionary(_, _) => todo!(),
                        DataType::Decimal128(_, _) => todo!(),
                        DataType::Decimal256(_, _) => todo!(),
                        DataType::Map(_, _) => todo!(),
                        DataType::RunEndEncoded(_, _) => todo!(),
                    }
                };

                writer.write_event(Event::Text(val))?;
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

fn encode_primitive<T>(arr: &Arc<dyn Array>, row: usize) -> BytesText
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: std::fmt::Display,
{
    let arr = arr.as_primitive::<T>();
    let val = arr.value(row).to_string();
    BytesText::from_escaped(val)
}

fn encode_date_time(dt: &DateTime<Utc>) -> BytesText<'static> {
    let s = dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    BytesText::from_escaped(s)
}
