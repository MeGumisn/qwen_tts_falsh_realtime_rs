use crate::common::errors::GenerationError;
use crate::odps::account::SignRequest;
use crate::odps::constants::ODPS_TO_ARROW_MAPPING;
use crate::odps::models::{TunnelDownloadSession, TunnelTableSchema};
use crate::odps::odps_arrow_reader::{OdpsArrowReader, SkippedCursor};
use crate::odps::rest_client::RestClient;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter, write_message,
};
use arrow_schema::ArrowError;
use bytes::Bytes;
use log::{debug, info};
use reqwest::Method;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::http::HeaderMap;

struct Odps<'a, T>
where
    T: SignRequest,
{
    project_name: &'a str,
    endpoint: &'a str,
    tunnel_endpoint: String,
    rest_client: RestClient<T>,
}

impl<'a, T> Odps<'a, T>
where
    T: SignRequest,
{
    pub async fn new(account: T, endpoint: &'a str, project_name: &'a str) -> Self {
        let rest_client = RestClient::new(account);
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
            .request(url_str.as_str(), Method::GET, endpoint, None)
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
            .request(url_str.as_str(), Method::GET, self.endpoint, None)
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

        headers.insert("odps-tunnel-date-transform", "v1".parse()?);
        headers.insert("odps-tunnel-sdk-support-schema-evolution", "true".parse()?);
        headers.insert("x-odps-tunnel-version", "6".parse()?);
        headers.insert("Content-Length", "0".parse()?);
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
        let url_str = if let Some(partition_spec) = partition_spec {
            format!(
                "{}/projects/{}/tables/{}?data&arrow&downloadid={}&partition={}&rowrange=({},{})",
                self.tunnel_endpoint,
                project_name,
                table_name,
                download_id,
                partition_spec,
                range.0,
                range.1
            )
        } else {
            format!(
                "{}/projects/{}/tables/{}?data&arrow&downloadid={}&rowrange=({},{})",
                self.tunnel_endpoint, project_name, table_name, download_id, range.0, range.1
            )
        };
        let mut headers = HeaderMap::new();
        headers.insert("odps-tunnel-date-transform", "v1".parse()?);
        headers.insert("odps-tunnel-sdk-support-schema-evolution", "true".parse()?);
        headers.insert("x-odps-tunnel-version", "6".parse()?);
        headers.insert("Content-Length", "0".parse()?);

        let response = self
            .rest_client
            .request(
                url_str.as_str(),
                Method::GET,
                self.tunnel_endpoint.as_str(),
                Some(headers),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::odps::account::test_account;
    use log::info;
    use crate::odps;

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
}
