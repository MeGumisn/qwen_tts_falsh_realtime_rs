use serde::{Deserialize, Serialize};

///
/// ## 用于存放一些Tunnel相关的返回
///

#[derive(Debug,Serialize,Deserialize)]
pub struct TunnelDownloadSession{
    #[serde(rename="DownloadID")]
    pub download_id: String,

}