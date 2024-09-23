use chrono::DateTime;
use datafusion::{
    common::{Column, ScalarValue},
    logical_expr::{expr::InList, BinaryExpr, Operator},
    prelude::*,
};
use odata_params::filters::{
    CompareOperator as ODataOperator, Expr as ODataExpr, Value as ODataValue,
};

use crate::error::{FilterParsingError, ODataError, UnsupportedFeature};

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
pub struct QueryParamsRaw {
    #[serde(rename = "$select")]
    pub select: Option<String>,
    #[serde(rename = "$orderby")]
    pub order_by: Option<String>,
    #[serde(rename = "$skip")]
    pub skip: Option<u64>,
    #[serde(rename = "$top")]
    pub top: Option<u64>,
    #[serde(rename = "$filter")]
    pub filter: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////

impl QueryParamsRaw {
    pub fn decode(self) -> Result<QueryParams, ODataError> {
        let select = self.select.unwrap_or_default();
        let mut select: Vec<_> = select.split(',').map(|s| s.to_string()).collect();
        select.retain(|i| !i.is_empty());

        let order_by_s = self.order_by.unwrap_or_default();
        let mut order_by_s: Vec<_> = order_by_s.split(',').collect();
        order_by_s.retain(|i| !i.is_empty());

        let mut order_by = Vec::new();
        for el in order_by_s {
            let (cname, asc) = if let Some(cname) = el.strip_suffix(" asc") {
                (cname, true)
            } else if let Some(cname) = el.strip_suffix(" desc") {
                (cname, false)
            } else {
                (el, true)
            };
            order_by.push((cname.to_string(), asc));
        }

        let skip = self.skip.map(|v| v as usize);
        let top = self.top.map(|v| v as usize);

        let filter = match self.filter {
            Some(fltr) => {
                let parsed_fltr = odata_params::filters::parse_str(fltr)?;
                Some(odata_expr_to_df_expr(&parsed_fltr)?)
            }
            None => None,
        };

        Ok(QueryParams {
            select,
            order_by,
            skip,
            top,
            filter,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct QueryParams {
    /// Column names
    pub select: Vec<String>,
    /// Tuples (column_name, ascending)
    pub order_by: Vec<(String, bool)>,
    /// Number of records to skip
    pub skip: Option<usize>,
    /// Maximum number of records to return
    pub top: Option<usize>,
    /// Filter a collection of resources   
    pub filter: Option<Expr>,
}

///////////////////////////////////////////////////////////////////////////////

impl QueryParams {
    pub fn apply(
        self,
        df: DataFrame,
        addr: &CollectionAddr,
        key_column: &str,
        key_column_alias: &str,
        default_rows: usize,
        max_rows: usize,
    ) -> datafusion::error::Result<DataFrame> {
        // Add key column as alias
        let df = df.with_column(key_column_alias, col(key_column))?;

        // Select desired columns
        let df = if self.select.is_empty() {
            df
        } else {
            let mut select: Vec<_> = self.select.iter().map(String::as_str).collect();
            select.push(key_column_alias);
            df.select_columns(&select)?
        };

        // If queried by key - ignore the rest
        if let Some(key) = &addr.key {
            return df.filter(col(key_column_alias).eq(lit(key.clone())));
        }

        let df = match self.filter {
            Some(filter) => df.filter(filter)?,
            None => df,
        };

        // Order by
        let df = if self.order_by.is_empty() {
            df
        } else {
            df.sort(
                self.order_by
                    .into_iter()
                    .map(|(c, asc)| col(c).sort(asc, true))
                    .collect(),
            )?
        };

        // Skip / limit
        df.limit(
            self.skip.unwrap_or(0),
            Some(std::cmp::min(self.top.unwrap_or(default_rows), max_rows)),
        )
    }
}

fn odata_expr_to_df_expr(res: &ODataExpr) -> Result<Expr, ODataError> {
    match res {
        ODataExpr::Or(l, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            Operator::Or,
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        ODataExpr::And(l, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            Operator::And,
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        ODataExpr::Compare(l, op, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            odata_op_to_df_op(op),
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        ODataExpr::Value(v) => Ok(Expr::Literal(odata_value_to_df_value(v)?)),
        ODataExpr::Not(e) => Ok(Expr::Not(Box::new(odata_expr_to_df_expr(e)?))),
        ODataExpr::In(i, l) => Ok(Expr::InList(InList::new(
            Box::new(odata_expr_to_df_expr(i)?),
            l.iter()
                .map(odata_expr_to_df_expr)
                .collect::<Result<Vec<Expr>, ODataError>>()?,
            false,
        ))),
        ODataExpr::Identifier(s) => Ok(Expr::Column(Column::new_unqualified(s))),
        ODataExpr::Function(..) => {
            Err(UnsupportedFeature::new("Function within the filter is not supported").into())
        }
    }
}

fn odata_value_to_df_value(v: &ODataValue) -> Result<ScalarValue, ODataError> {
    match v {
        ODataValue::String(s) => Ok(ScalarValue::LargeUtf8(Some(s.clone()))),
        ODataValue::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        ODataValue::Null => Ok(ScalarValue::Null),
        ODataValue::Number(d) => {
            let d = d
                .to_string()
                .parse::<i64>()
                .map_err(|_| FilterParsingError::new("Failed to parse number"))?;
            Ok(ScalarValue::Int64(Some(d)))
        }
        ODataValue::DateTime(d) => Ok(ScalarValue::Date64(Some(d.timestamp()))),
        ODataValue::Date(d) => {
            let d = d
                .and_hms_opt(0, 0, 0)
                .ok_or(FilterParsingError::new("Failed to parse date"))?;
            let timestamp =
                DateTime::<chrono::Utc>::from_naive_utc_and_offset(d, chrono::Utc).timestamp();
            Ok(ScalarValue::Date64(Some(timestamp)))
        }
        ODataValue::Uuid(u) => Ok(ScalarValue::LargeUtf8(Some(u.to_string()))),
        ODataValue::Time(_) => {
            Err(UnsupportedFeature::new("Time value in filter is not supported").into())
        }
    }
}

fn odata_op_to_df_op(op: &ODataOperator) -> Operator {
    match op {
        ODataOperator::Equal => Operator::Eq,
        ODataOperator::NotEqual => Operator::NotEq,
        ODataOperator::LessThan => Operator::Lt,
        ODataOperator::GreaterThan => Operator::Gt,
        ODataOperator::LessOrEqual => Operator::LtEq,
        ODataOperator::GreaterOrEqual => Operator::GtEq,
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionAddr {
    pub name: String,
    pub key: Option<String>,
}

impl CollectionAddr {
    pub fn decode(collection_path_element: &str) -> Option<Self> {
        let re = regex::Regex::new(r#"^(?<name>[A-Za-z0-9._-]+)(\((?<key>[^)]+)\))?$"#).unwrap();
        let c = re.captures(collection_path_element)?;

        let name = c.name("name")?.as_str().to_string();
        let key = c.name("key").map(|m| m.as_str().to_string());

        Some(Self { name, key })
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::CollectionAddr;

    #[test]
    fn test_collection_addr_decode() {
        assert_eq!(
            CollectionAddr::decode("coll"),
            Some(CollectionAddr {
                name: "coll".to_string(),
                key: None,
            })
        );

        assert_eq!(
            CollectionAddr::decode("Coll123"),
            Some(CollectionAddr {
                name: "Coll123".to_string(),
                key: None,
            })
        );

        assert_eq!(
            CollectionAddr::decode("Coll.x_12-3"),
            Some(CollectionAddr {
                name: "Coll.x_12-3".to_string(),
                key: None,
            })
        );

        assert_eq!(
            CollectionAddr::decode("Coll(123)"),
            Some(CollectionAddr {
                name: "Coll".to_string(),
                key: Some("123".to_string()),
            })
        );

        assert_eq!(
            CollectionAddr::decode("Coll('key')"),
            Some(CollectionAddr {
                name: "Coll".to_string(),
                key: Some("'key'".to_string()),
            })
        );
    }
}
