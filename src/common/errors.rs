use arrow_schema::ArrowError;
use hmac::digest::InvalidLength;
use thiserror::Error;
use tokio_tungstenite::tungstenite::http;

#[derive(Debug, Error)]
pub enum GenerationError {
    #[error("网络错误: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Http构造错误: {0}")]
    HttpError(http::Error),

    #[error("str 转 url错误: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("Header Value 格式错误: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

    #[error("serde json 转换错误: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("HeaderValue转换为str错误: {0}")]
    HeaderValueToStr(#[from] http::header::ToStrError),

    #[error("Hmac Sha1 格式错误: {0}")]
    HmacSha1InvalidLength(#[from] InvalidLength),

    #[error("Odps endpoint查找region信息错误: {0}")]
    NoRegionInEndpointError(#[from] regex::Error),

    #[error("Odps 发送arrow数据失败: {0}")]
    OdpsArrowError(#[from] ArrowError)
}