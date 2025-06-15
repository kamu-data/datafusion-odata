use chrono::{DateTime, Utc};
use datafusion::{
    logical_expr::{expr::InList, BinaryExpr, Operator},
    prelude::*,
    scalar::ScalarValue,
};
use odata_params::filters as odata_filters;

use crate::error::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ODataFilter(Expr);

impl From<ODataFilter> for Expr {
    fn from(value: ODataFilter) -> Self {
        value.0
    }
}

///////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for ODataFilter {
    type Err = ODataError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let odata_exprs = odata_params::filters::parse_str(s).map_err(ODataError::bad_request)?;
        let df_exprs = odata_expr_to_df_expr(&odata_exprs)?;
        Ok(ODataFilter(df_exprs))
    }
}

impl<'de> serde::Deserialize<'de> for ODataFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(ODataFilterVisitor)
    }
}

struct ODataFilterVisitor;

impl serde::de::Visitor<'_> for ODataFilterVisitor {
    type Value = ODataFilter;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "an OData $filter string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        v.parse().map_err(serde::de::Error::custom)
    }
}

///////////////////////////////////////////////////////////////////////////////

fn odata_expr_to_df_expr(res: &odata_filters::Expr) -> Result<Expr, ODataError> {
    match res {
        odata_filters::Expr::Or(l, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            Operator::Or,
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        odata_filters::Expr::And(l, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            Operator::And,
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        odata_filters::Expr::Compare(l, op, r) => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(odata_expr_to_df_expr(l)?),
            odata_op_to_df_op(op),
            Box::new(odata_expr_to_df_expr(r)?),
        ))),
        odata_filters::Expr::Value(v) => Ok(Expr::Literal(odata_value_to_df_value(v)?, None)),
        odata_filters::Expr::Not(e) => Ok(Expr::Not(Box::new(odata_expr_to_df_expr(e)?))),
        odata_filters::Expr::In(i, l) => Ok(Expr::InList(InList::new(
            Box::new(odata_expr_to_df_expr(i)?),
            l.iter()
                .map(odata_expr_to_df_expr)
                .collect::<Result<Vec<Expr>, ODataError>>()?,
            false,
        ))),
        odata_filters::Expr::Identifier(s) => Ok(Expr::Column(Column::new_unqualified(s))),
        odata_filters::Expr::Function(..) => {
            Err(UnsupportedFeature::new("Function within the filter is not supported").into())
        }
    }
}

fn odata_value_to_df_value(v: &odata_filters::Value) -> Result<ScalarValue, ODataError> {
    match v {
        odata_filters::Value::String(s) => Ok(ScalarValue::LargeUtf8(Some(s.clone()))),
        odata_filters::Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        odata_filters::Value::Null => Ok(ScalarValue::Null),
        odata_filters::Value::Number(d) => {
            let d = d
                .to_string()
                .parse::<i64>()
                .map_err(|_| BadRequest::new("Filter contains invalid number"))?;
            Ok(ScalarValue::Int64(Some(d)))
        }
        odata_filters::Value::DateTime(d) => Ok(ScalarValue::Date64(Some(d.timestamp()))),
        odata_filters::Value::Date(d) => {
            let d = d
                .and_hms_opt(0, 0, 0)
                .ok_or(BadRequest::new("Filter contains invalid date"))?;
            let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(d, Utc).timestamp();
            Ok(ScalarValue::Date64(Some(timestamp)))
        }
        odata_filters::Value::Uuid(u) => Ok(ScalarValue::LargeUtf8(Some(u.to_string()))),
        odata_filters::Value::Time(_) => {
            Err(UnsupportedFeature::new("Time value in filter is not supported").into())
        }
    }
}

fn odata_op_to_df_op(op: &odata_filters::CompareOperator) -> Operator {
    match op {
        odata_filters::CompareOperator::Equal => Operator::Eq,
        odata_filters::CompareOperator::NotEqual => Operator::NotEq,
        odata_filters::CompareOperator::LessThan => Operator::Lt,
        odata_filters::CompareOperator::GreaterThan => Operator::Gt,
        odata_filters::CompareOperator::LessOrEqual => Operator::LtEq,
        odata_filters::CompareOperator::GreaterOrEqual => Operator::GtEq,
    }
}

///////////////////////////////////////////////////////////////////////////////
