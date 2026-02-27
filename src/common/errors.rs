use thiserror::Error;
use tokio_tungstenite::tungstenite::http;

#[derive(Debug, Error)]
pub enum GenerationError {
    #[error("网络错误: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Http构造错误: {0}")]
    HttpError(http::Error),

    #[error("Header Value 格式错误: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

    #[error("serde json 转换错误: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
}