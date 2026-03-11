use crate::common::errors::GenerationError;
use crate::odps::account::SignRequest;
use crate::odps::constants::insert_default_tunnel_header;
use crate::odps::models::{
    TunnelDownloadSession, TunnelTableSchema, TunnelUploadSession, TunnelUploadedBlocks,
};
use crate::odps::odps_arrow_reader::OdpsArrowReader;
use crate::odps::odps_arrow_writer::OdpsArrowWriter;
use crate::odps::rest_client::RestClient;
use arrow::array::RecordBatch;
use arrow_schema::ArrowError;
use bstr::ByteSlice;
use bytes::Bytes;
use futures_util::SinkExt;
use log::{debug, error, info, warn};
use reqwest::Method;
use reqwest::header::HeaderMap;
use std::cmp::{max, min};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::mpsc;

struct Odps<'a, T>
where
    T: SignRequest + Sync + Send + 'static,
{
    project_name: &'a str,
    endpoint: &'a str,
    tunnel_endpoint: String,
    rest_client: Arc<RestClient<T>>,
}

impl<'a, T> Odps<'a, T>
where
    T: SignRequest + Sync + Send + 'static,
{
    pub async fn new(account: T, endpoint: &'a str, project_name: &'a str) -> Self {
        let rest_client = Arc::new(RestClient::new(account));
        let tunnel_host = Self::get_tunnel_host(&rest_client, endpoint, project_name)
            .await
            .unwrap_or("".into());
        let tunnel_endpoint = format!("https://{}", tunnel_host);
        Self {
            project_name,
            endpoint,
            tunnel_endpoint,
            rest_client,
        }
    }

    async fn get_tunnel_host(
        rest_client: &RestClient<T>,
        endpoint: &str,
        project_name: &str,
    ) -> Result<String, GenerationError> {
        // https://service.cn-hangzhou.maxcompute.aliyun.com/api/projects/test_dat_maxcompute/tunnel?service
        let url_str = format!("{}/projects/{}/tunnel?service", endpoint, project_name);
        let response = rest_client
            .request(url_str.as_str(), Method::GET, endpoint, None, None)
            .await?;
        Ok(response.text().await?)
    }

    ///
    /// 步骤： sign_request 里calc_auth_str， 加入到header的Authorization
    /// request请求的header示例如下
    /// ```json
    /// {
    ///   "User-Agent": "pyodps/0.12.5.1 CPython/3.13.11 Windows/11",
    ///   "Date": "Wed, 04 Feb 2026 18:40:34 GMT",
    ///   "Authorization": "ODPS **your access id**/20260204/cn/odps/aliyun_v4_request:LJpwqdeznLSpwMCB2XZK0yp00qY="
    /// }
    /// ```
    pub async fn get_logview_host(&self) -> Result<String, GenerationError> {
        let url_str = format!("{}/logview/host", self.endpoint);
        let response = self
            .rest_client
            .request(url_str.as_str(), Method::GET, self.endpoint, None, None)
            .await?;
        Ok(response.text().await?)
    }

    /// - headers:
    /// ```json
    /// {
    ///     "odps-tunnel-date-transform": "v1",
    ///     "odps-tunnel-sdk-support-schema-evolution": "true",
    ///     "x-odps-tunnel-version": "6",
    ///     "Content-Length": "0"
    /// }
    /// ```
    ///
    pub async fn create_tunnel_download_session(
        &self,
        project_name: Option<&str>,
        table_name: &str,
        partition_spec: Option<&str>,
    ) -> Result<TunnelDownloadSession, GenerationError> {
        let project_name: &str = match project_name {
            Some(project_name) => project_name,
            None => self.project_name,
        };
        let url_str = if let Some(partition_spec) = partition_spec {
            format!(
                "{}/projects/{}/tables/{}?downloads=&partition={}&asyncmode=true",
                self.tunnel_endpoint, project_name, table_name, partition_spec
            )
        } else {
            format!(
                "{}/projects/{}/tables/{}?downloads=&asyncmode=true",
                self.tunnel_endpoint, self.project_name, table_name
            )
        };
        let mut headers = HeaderMap::new();
        insert_default_tunnel_header!(headers);
        // ODPS LTAI5tKSLbi8M5UmbBtXxKW4:vgFwyabNyS8zUPchd9bPf8gZras=
        // #[cfg(test)]
        // headers.insert("Date", "Wed, 04 Mar 2026 00:59:10 GMT".parse()?);

        let response = self
            .rest_client
            .request(
                url_str.as_str(),
                Method::POST,
                self.tunnel_endpoint.as_str(),
                Some(headers),
                None,
            )
            .await?;
        let download_session_text = response.text().await?;
        let download_session =
            serde_json::from_str::<TunnelDownloadSession>(&download_session_text)?;
        Ok(download_session)
    }

    pub async fn get_tunnel_arrow_data(
        &self,
        project_name: Option<&str>,
        table_name: &str,
        download_id: &str,
        partition_spec: Option<&str>,
        range: (usize, usize),
    ) -> Result<Bytes, GenerationError> {
        let project_name: &str = project_name.unwrap_or(self.project_name);
        let mut url_str = format!(
            "{}/projects/{}/tables/{}?data&arrow&downloadid={}&rowrange=({},{})",
            self.tunnel_endpoint, project_name, table_name, download_id, range.0, range.1
        );
        if let Some(partition_spec) = partition_spec {
            url_str.push_str("&partition=");
            url_str.push_str(partition_spec);
        }
        let mut headers = HeaderMap::new();
        insert_default_tunnel_header!(headers);

        let response = self
            .rest_client
            .request(
                url_str.as_str(),
                Method::GET,
                self.tunnel_endpoint.as_str(),
                Some(headers),
                None,
            )
            .await?;
        Ok(response.bytes().await?)
    }

    pub async fn read_arrow_bytes(
        &self,
        bytes: &mut Bytes,
        tunnel_schema: &TunnelTableSchema,
    ) -> Result<(), ArrowError> {
        debug!("read_arrow_bytes: {:#?}", bytes);
        let odps_arrow_reader = OdpsArrowReader::new(tunnel_schema)?;
        let reader = odps_arrow_reader.open_arrow_reader(bytes)?;
        info!("Odps arrow reader opened, schema: {:#?}", reader.schema());
        for batch in reader {
            let batch = batch?;
            info!("Batch: {:#?}", batch);
        }
        Ok(())
    }

    ///
    /// /projects/test_dat_maxcompute/tables/json_string?reload=False&uploads=1
    pub async fn create_tunnel_upload_session(
        &self,
        project_name: Option<&str>,
        table_name: &str,
        partition_spec: Option<&str>,
        overwrite: bool,
    ) -> Result<TunnelUploadSession, GenerationError> {
        let project_name = project_name.unwrap_or(self.project_name);
        let mut url_str = format!(
            "{}/projects/{}/tables/{}?reload=False&uploads=1",
            self.tunnel_endpoint, project_name, table_name
        );
        if let Some(partition_spec) = partition_spec {
            url_str.push_str("&partition=");
            url_str.push_str(partition_spec);
        }
        if overwrite {
            url_str.push_str("&overwrite=true");
        }

        let mut headers = HeaderMap::new();
        insert_default_tunnel_header!(headers);
        let response = self
            .rest_client
            .request(
                url_str.as_str(),
                Method::POST,
                self.tunnel_endpoint.as_str(),
                Some(headers),
                None,
            )
            .await?;
        let upload_session_text = response.text().await?;
        debug!("Upload session text: {:#?}", upload_session_text);
        let upload_session = serde_json::from_str::<TunnelUploadSession>(&upload_session_text)?;
        Ok(upload_session)
    }

    #[cfg(test)]
    fn unescape_python_bstr(s: &str) -> Vec<u8> {
        // 移除開頭的 b' 和結尾的 '
        let inner = s.strip_prefix("b'").and_then(|s| s.strip_suffix("'")).unwrap_or(s);

        let mut bytes = Vec::new();
        let mut chars = inner.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek() == Some(&'x') {
                chars.next(); // 跳過 'x'
                let hex: String = chars.by_ref().take(2).collect();
                bytes.push(u8::from_str_radix(&hex, 16).unwrap());
            } else {
                bytes.push(c as u8);
            }
        }
        bytes
    }

    ///
    /// PUT /projects/test_dat_maxcompute/tables/json_string?uploadid=2026030710275181dd321a20f6b624&arrow=&blockid=0
    pub async fn open_tunnel_arrow_writer(
        &self,
        project_name: Option<&str>,
        table_name: &str,
        upload_id: &str,
        partition_spec: Option<&str>,
        block_id: Option<&str>,
        arrow_data: RecordBatch,
    ) -> Result<(), GenerationError> {
        let project_name: &str = project_name.unwrap_or(self.project_name);
        let block_id = block_id.unwrap_or("0");
        let mut url_str = format!(
            "{}/projects/{}/tables/{}?uploadid={}&arrow=&blockid={}",
            self.tunnel_endpoint, project_name, table_name, upload_id, block_id
        );
        if let Some(partition_spec) = partition_spec {
            url_str.push_str("&partition=");
            url_str.push_str(partition_spec);
        }
        let mut headers = HeaderMap::new();
        insert_default_tunnel_header!(headers);

        headers.insert("Transfer-Encoding", "chunked".parse()?);
        headers.insert("Content-Type", "application/octet-stream".parse()?);

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        // 3. 異步處理器：負責發送請求
        let url_clone = url_str.clone();
        let rest_client = self.rest_client.clone();
        let headers_owned = headers.clone();
        let tunnel_endpoint_clone = self.tunnel_endpoint.clone();

        let request_task = tokio::spawn(async move {
            // TODO 这里chunked数据需要包装为 "%x\r\n%b\r\n"
            let chunked = true;
            while let Some(payload) = rx.recv().await {
                let mut payload = Vec::new();
                let _ = File::open("./chunk").unwrap().read_to_end(&mut payload);
                let _ = File::create("./chunk.txt").unwrap().write(payload.as_bstr()).unwrap();
                let payload_len = payload.len();
                info!(
                    "收到寫滿的 buffer，chunk encoding length {:X}, 準備發送...\n",
                    payload_len
                );
                let data_send = if chunked {
                    // 2. 構造 Chunked 格式: "{hex_len}\r\n{payload}\r\n"
                    let mut chunked_data = Vec::with_capacity(payload_len); // 預留空間給 Header/Footer

                    // 寫入 16 進位長度和 CRLF
                    use std::io::Write;
                    write!(chunked_data, "{:X}\r\n", payload_len).unwrap();

                    // 寫入原始數據
                    chunked_data.extend_from_slice(&payload);

                    // 寫入結尾的 CRLF
                    chunked_data.extend_from_slice(b"\r\n");
                    chunked_data
                } else {
                    payload
                };


                // 這裡可以安全地使用 .await，不會阻塞寫入線程
                if let Ok(response) = rest_client
                    .request(
                        &url_clone,
                        Method::PUT,
                        &tunnel_endpoint_clone,
                        Some(headers_owned.clone()),
                        Some(data_send),
                    )
                    .await
                {
                    let put_success = response.status().is_success();
                    match response.text().await {
                        Ok(content) => {
                            if put_success {
                                info!(
                                    "send data finished, response status is success: {:?}",
                                    content
                                )
                            } else {
                                error!(
                                    "send data finished, response status is failed: {:?}",
                                    content
                                );
                                panic!();
                            }
                        }
                        Err(e) => {
                            error!("send data failed: {:?}", e);
                            panic!();
                        }
                    }
                }
            }
        });

        let mut odps_arrow_writer = OdpsArrowWriter::new(65536, tx).await?;
        odps_arrow_writer
            .write_record([arrow_data].into_iter())
            .await?;
        odps_arrow_writer.close()?;
        drop(odps_arrow_writer);
        request_task.await.expect("request_task failed");
        Ok(())
    }

    pub async fn commit_tunnel_upload_session(
        &self,
        project_name: Option<&str>,
        table_name: &str,
        upload_id: &str,
        partition_spec: Option<&str>,
    ) -> Result<TunnelUploadedBlocks, GenerationError> {
        let project_name = project_name.unwrap_or(self.project_name);
        let mut url_str = format!(
            "{}/projects/{}/tables/{}?uploadid={}",
            self.tunnel_endpoint, project_name, table_name, upload_id
        );
        if let Some(partition_spec) = partition_spec {
            url_str.push_str("&partition=");
            url_str.push_str(partition_spec);
        }
        let mut headers = HeaderMap::new();
        insert_default_tunnel_header!(headers);
        let response = self
            .rest_client
            .request(
                url_str.as_str(),
                Method::POST,
                self.tunnel_endpoint.as_str(),
                Some(headers),
                None,
            )
            .await?;
        let content = response.text().await?;
        debug!("Upload session text: {:#?}", content);
        Ok(serde_json::from_str::<TunnelUploadedBlocks>(&content)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::odps::account::test_account;
    use arrow::array::record_batch;
    use log::info;
    use std::fs::File;
    use std::io::Read;

    #[tokio::test]
    async fn test_get_tunnel_host() {
        let account = test_account();
        let odps = Odps::new(
            account,
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            "test_dat_maxcompute",
        )
        .await;
        let tunnel_host = &odps.tunnel_endpoint;
        println!("{}", *tunnel_host);
        let logview_host = odps.get_logview_host().await.unwrap();
        println!("{}", logview_host);
    }

    #[tokio::test]
    async fn test_create_download_session() {
        let account = test_account();
        let odps = Odps::new(
            account,
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            "test_dat_maxcompute",
        )
        .await;
        let download_session = odps
            .create_tunnel_download_session(None, "json_string", None)
            .await
            .unwrap();
        info!("{:#?}", download_session);
    }

    #[tokio::test]
    async fn test_read_arrow_data() {
        let account = test_account();
        let project_name = "test_dat_maxcompute";
        let table_name = "json_string";
        let odps = Odps::new(
            account,
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            project_name,
        )
        .await;
        let download_session = odps
            .create_tunnel_download_session(None, table_name, None)
            .await
            .unwrap();
        let mut bytes = odps
            .get_tunnel_arrow_data(
                None,
                table_name,
                download_session.download_id.as_str(),
                None,
                (0, download_session.record_count as usize),
            )
            .await
            .unwrap();
        odps.read_arrow_bytes(&mut bytes, &download_session.schema)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_tunnel_upload_session() {
        let account = test_account();
        let project_name = "test_dat_maxcompute";
        let table_name = "json_string";
        let odps = Odps::new(
            account,
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            project_name,
        )
        .await;
        let upload_session = odps
            .create_tunnel_upload_session(None, table_name, None, true)
            .await
            .unwrap();
        println!("{:#?}", upload_session);
    }

    #[tokio::test]
    async fn test_open_tunnel_arrow_writer() {
        let account = test_account();
        let project_name = "test_dat_maxcompute";
        let table_name = "json_string";
        let odps = Odps::new(
            account,
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            project_name,
        )
        .await;
        let upload_session = odps
            .create_tunnel_upload_session(None, table_name, None, true)
            .await
            .unwrap();

        let mut name = String::new();
        let name_len = File::open("./name.txt")
            .unwrap()
            .read_to_string(&mut name)
            .unwrap();
        println!("name length: {}", name_len);
        let record_batch = record_batch!(("name", Utf8, [name]), ("age", Int64, [4])).unwrap();

        odps.open_tunnel_arrow_writer(
            None,
            table_name,
            upload_session.upload_id.as_str(),
            None,
            Some("1"),
            record_batch,
        )
        .await
        .unwrap();
        println!(
            "{:#?}",
            odps.commit_tunnel_upload_session(
                None,
                table_name,
                upload_session.upload_id.as_str(),
                None,
            )
            .await
            .unwrap()
        );
    }
}
