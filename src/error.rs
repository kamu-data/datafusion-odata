use datafusion::arrow::datatypes::DataType;
use std::string::FromUtf8Error;

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ODataError {
    #[error(transparent)]
    UnsupportedDataType(#[from] UnsupportedDataType),
    #[error(transparent)]
    UnsupportedColumnType(#[from] UnsupportedColumnType),
    #[error(transparent)]
    BatchUnexpectedRowsNumber(#[from] BatchUnexpectedRowsNumber),
    #[error(transparent)]
    UnexpectedBatchesNumber(#[from] UnexpectedBatchesNumber),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error(transparent)]
    UnsupportedFeature(#[from] UnsupportedFeature),
    #[error(transparent)]
    UnsupportedNetProtocol(#[from] UnsupportedNetProtocol),
    #[error(transparent)]
    CollectionNotFound(#[from] CollectionNotFound),
    #[error(transparent)]
    CollectionAddressNotAssigned(#[from] CollectionAddressNotAssigned),
    #[error(transparent)]
    KeyColumnNotAssigned(#[from] KeyColumnNotAssigned),
    #[error(transparent)]
    Internal(InternalError),
}

impl ODataError {
    pub fn internal(error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        Self::Internal(InternalError::new(error))
    }

    pub fn handle_no_table_as_collection_not_found(
        collection: impl Into<String>,
        err: datafusion::error::DataFusionError,
    ) -> Self {
        match err {
            datafusion::error::DataFusionError::Plan(e) if e.contains("No table named") => {
                Self::CollectionNotFound(CollectionNotFound::new(collection))
            }
            _ => Self::internal(err),
        }
    }
}

impl axum::response::IntoResponse for ODataError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Internal(_)
            | Self::BatchUnexpectedRowsNumber(_)
            | Self::UnexpectedBatchesNumber(_)
            | Self::FromUtf8Error(_) => {
                (http::StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
            }
            Self::CollectionNotFound(e) => e.into_response(),
            Self::UnsupportedDataType(e) => e.into_response(),
            Self::UnsupportedColumnType(e) => e.into_response(),
            Self::UnsupportedFeature(e) => e.into_response(),
            Self::CollectionAddressNotAssigned(e) => e.into_response(),
            Self::KeyColumnNotAssigned(e) => e.into_response(),
            Self::UnsupportedNetProtocol(e) => e.into_response(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Internal error")]
pub struct InternalError {
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl InternalError {
    pub fn new(error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        Self {
            source: error.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Collection {collection} not found")]
pub struct CollectionNotFound {
    pub collection: String,
}

impl CollectionNotFound {
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
        }
    }
}

impl axum::response::IntoResponse for CollectionNotFound {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_FOUND, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Key column not assigned")]
pub struct KeyColumnNotAssigned;

impl axum::response::IntoResponse for KeyColumnNotAssigned {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Collection address not assigned")]
pub struct CollectionAddressNotAssigned;

impl axum::response::IntoResponse for CollectionAddressNotAssigned {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unsupported data type: {data_type}")]
pub struct UnsupportedDataType {
    pub data_type: DataType,
}

impl UnsupportedDataType {
    pub fn new(data_type: DataType) -> Self {
        Self { data_type }
    }
}

impl axum::response::IntoResponse for UnsupportedDataType {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unsupported column type: {col_type}")]
pub struct UnsupportedColumnType {
    pub col_type: DataType,
}

impl UnsupportedColumnType {
    pub fn new(col_type: DataType) -> Self {
        Self { col_type }
    }
}

impl axum::response::IntoResponse for UnsupportedColumnType {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Batch has unexpected rows number: {num_rows}")]
pub struct BatchUnexpectedRowsNumber {
    pub num_rows: usize,
}

impl BatchUnexpectedRowsNumber {
    pub fn new(num_rows: usize) -> Self {
        Self { num_rows }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unexpected number of batches: {num}")]
pub struct UnexpectedBatchesNumber {
    pub num: usize,
}

impl UnexpectedBatchesNumber {
    pub fn new(num: usize) -> Self {
        Self { num }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unsupported net protocol: {url}")]
pub struct UnsupportedNetProtocol {
    pub url: String,
}

impl UnsupportedNetProtocol {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl axum::response::IntoResponse for UnsupportedNetProtocol {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unsupported feature: {feature}")]
pub struct UnsupportedFeature {
    pub feature: String,
}

impl UnsupportedFeature {
    pub fn new(feature: impl Into<String>) -> Self {
        Self {
            feature: feature.into(),
        }
    }
}

impl axum::response::IntoResponse for UnsupportedFeature {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
    }
}

///////////////////////////////////////////////////////////////////////////////

impl From<quick_xml::Error> for ODataError {
    fn from(error: quick_xml::Error) -> Self {
        ODataError::Internal(InternalError::new(error))
    }
}
