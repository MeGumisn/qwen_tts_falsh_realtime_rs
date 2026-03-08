use serde::{Deserialize, Serialize};

///
/// ## 用于存放一些Tunnel相关的返回
///
///
/// Tunnel Download Session
/// - 示例：
/// ```json
/// {
///     "DownloadID": "20260304091859291d3b1a1ee9b36a",
///     "Initiated": "Wed, 04 Mar 2026 09:18:59 GMT",
///     "Owner": "p4_203299136401391858",
///     "QuotaName": "default",
///     "RecordCount": 4,
///     "Schema": {
///         "IsVirtualView": false,
///         "columns": [{
///                 "column_id": "",
///                 "comment": "",
///                 "default_value": "",
///                 "name": "name",
///                 "nullable": true,
///                 "type": "string"},
///             {
///                 "column_id": "",
///                 "comment": "",
///                 "default_value": "",
///                 "name": "age",
///                 "nullable": true,
///                 "type": "bigint"}],
///         "partitionKeys": []},
///     "Status": "normal",
///     "SupportReadByRawSize": true
/// }
/// ```

#[derive(Debug, Serialize, Deserialize)]
pub struct TunnelDownloadSession {
    #[serde(rename = "DownloadID")]
    pub download_id: String,
    #[serde(rename = "Initiated")]
    pub initiated: String,
    #[serde(rename = "Owner")]
    pub owner: String,
    #[serde(rename = "QuotaName")]
    pub quota_name: String,
    #[serde(rename = "RecordCount")]
    pub record_count: i32,
    #[serde(rename = "Schema")]
    pub schema: TunnelTableSchema,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "SupportReadByRawSize")]
    pub support_read_by_raw_size: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TunnelTableSchema {
    #[serde(rename = "IsVirtualView")]
    pub is_virtual_view: bool,
    #[serde(rename = "columns")]
    pub columns: Vec<TunnelTableColumn>,
    #[serde(rename = "partitionKeys")]
    pub partition_keys: Vec<TunnelTableColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TunnelTableColumn {
    #[serde(rename = "column_id")]
    pub column_id: String,
    #[serde(rename = "comment")]
    pub comment: String,
    #[serde(rename = "default_value")]
    pub default_value: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "nullable")]
    pub nullable: bool,
    #[serde(rename = "type")]
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TunnelUploadSession {
    #[serde(rename = "Initiated")]
    pub initiated: String,
    #[serde(rename = "IsOverwrite")]
    pub is_overwrite: bool,
    #[serde(rename = "MaxFieldSize")]
    pub max_field_size: i32,
    #[serde(rename = "Owner")]
    pub owner: String,
    #[serde(rename = "QuotaName")]
    pub quota_name: String,
    #[serde(rename = "Schema")]
    pub schema: TunnelTableSchema,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "UploadID")]
    pub upload_id: String,
}

///
/// 请求参数 POST /projects/test_dat_maxcompute/tables/json_string?uploadid=202603090330154dd9c20b26084017
/// session.commit(block_id)后的返回结果
/// ```json
/// {
///     "UploadedBlockList":
///     [
///         {
///             "BlockID": 0,
///             "CreateTime": 0,
///             "Date": "",
///             "FileName": "0_1772998216627_1",
///             "RawSize": 0,
///             "RecordCount": 1,
///             "Version": 1772998216627
///         }
///     ]
/// }
///```
#[derive(Debug, Serialize, Deserialize)]
pub struct TunnelUploadedBlocks {
    #[serde(skip_serializing_if = "Option::is_none", rename = "UploadedBlockList")]
    pub uploaded_block_list: Option<Vec<UploadedBlock>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadedBlock{
    #[serde(rename = "BlockID")]
    pub block_id: u32,
    #[serde(rename = "CreateTime")]
    pub create_time: u64,
    #[serde(rename = "Date")]
    pub date: String,
    #[serde(rename = "FileName")]
    pub file_name: String,
    #[serde(rename = "RawSize")]
    pub raw_size: i32,
    #[serde(rename = "RecordCount")]
    pub record_count: i32,
    #[serde(rename = "Version")]
    pub version: u64,

}