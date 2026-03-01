use crate::common::errors::GenerationError;
use base64::Engine;
use base64::engine::general_purpose;
use bstr::ByteSlice;
use chrono::Utc;
use hmac::{Hmac, Mac};
use log::debug;
use regex::Regex;
use reqwest::{Client, Method, Request, Response, Url};
use sha1::Sha1;
use std::str::FromStr;
use tokio_tungstenite::tungstenite::http::HeaderMap;

trait SignRequest {
    fn sign_request(
        &self,
        request: &mut Request,
        endpoint: &str,
        region_name: Option<&str>,
    ) -> Result<(), GenerationError>;
    fn build_canonical_str(
        header: &mut HeaderMap,
        method: &str,
        url_path: &str,
        sorted_url_path: &str,
    ) -> Result<String, GenerationError>;

    fn calc_auth_str(
        &self,
        canonical_str: &str,
        region_name: Option<&str>,
    ) -> Result<String, GenerationError>;
}

pub struct AliyunAccount<'a> {
    pub access_id: &'a str,
    pub secret_key: &'a str,
    base_url: &'a str,
}

impl<'a> AliyunAccount<'a> {
    pub fn new(access_id: &'a str, secret_key: &'a str) -> Self {
        Self {
            access_id,
            secret_key,
            base_url: "https://www.aliyun.com",
        }
    }
}

impl SignRequest for AliyunAccount<'_> {
    fn sign_request(
        &self,
        request: &mut Request,
        endpoint: &str,
        region_name: Option<&str>,
    ) -> Result<(), GenerationError> {
        let method = request.method().to_string();
        let url = request.url().to_string();
        let header = request.headers_mut();
        if let Some(partial_url) = url.strip_prefix(endpoint)
            && let Ok(temp_url) = Url::parse(self.base_url)
            && let Ok(mut u) = temp_url.join(partial_url)
        {
            let mut url_params = u.query_pairs().into_owned().collect::<Vec<_>>();
            url_params.sort_by(|a, b| a.0.cmp(&b.0));
            u.query_pairs_mut().clear().extend_pairs(url_params);
            if let Some(sorted_url_query) = u.query() {
                let canonical_str =
                    Self::build_canonical_str(header, &method, u.path(), sorted_url_query)?;
                #[cfg(test)]
                println!(
                    "{}",
                    self.calc_auth_str(canonical_str.as_str(), region_name)?
                );
                let auth_str = self.calc_auth_str(canonical_str.as_str(), region_name)?;
                request
                    .headers_mut()
                    .insert("Authorization", auth_str.parse()?);
            }
        }
        Ok(())
    }

    fn build_canonical_str(
        header: &mut HeaderMap,
        method: &str,
        url_path: &str,
        sorted_url_query: &str,
    ) -> Result<String, GenerationError> {
        let mut header_to_sign = Vec::new();
        let date = if header.contains_key("Date")
            && let Ok(date_str) = header["Date"].to_str()
            && !date_str.is_empty()
        {
            date_str.to_string()
        } else {
            let dt = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            header.insert("Date", dt.parse()?);
            dt
        };
        // 插入date
        header_to_sign.push(("Date", date.as_str()));
        // 插入content-type 和 content-md
        if header.contains_key("content-type") {
            header_to_sign.push(("content-type", header["content-type"].to_str()?));
        } else {
            header_to_sign.push(("content-type", ""));
        }
        if header.contains_key("content-md5") {
            header_to_sign.push(("content-md5", header["content-md5"].to_str()?));
        } else {
            header_to_sign.push(("content-md5", ""));
        }
        // 排序header_to_sign
        header_to_sign.sort_by(|a, b| b.0.cmp(a.0));

        let mut lines = vec![method.to_string()];
        header_to_sign.iter().for_each(|(k, v)| {
            if k.starts_with("x-odps-") {
                lines.push(format!("{}:{}", k, v));
            } else {
                lines.push(v.to_string())
            }
        });
        if !sorted_url_query.is_empty() {
            lines.push(format!("{}?{}", url_path, sorted_url_query));
        } else {
            lines.push(url_path.to_string());
        }
        Ok(lines.join("\n"))
    }

    fn calc_auth_str(
        &self,
        canonical_str: &str,
        region_name: Option<&str>,
    ) -> Result<String, GenerationError> {
        type HmacSha1 = Hmac<Sha1>;
        if let Some(region_name) = region_name {
            type HmacSha256 = Hmac<sha2::Sha256>;
            let date_str = Utc::now().format("%Y%m%d").to_string();
            let credential = [
                self.access_id,
                date_str.as_str(),
                region_name,
                "odps/aliyun_v4_request",
            ]
            .join("/");
            let k_secret = format!("{}{}", "aliyun_v4", self.secret_key);
            // k_date
            let mut k_date = HmacSha256::new_from_slice(k_secret.as_bytes())?;
            k_date.update(date_str.as_bytes());
            let k_date_digest = k_date.finalize().into_bytes();
            #[cfg(test)]
            debug!("k_date_digest: {:?}", k_date_digest.as_bstr());
            // k_region
            let mut k_region = HmacSha256::new_from_slice(k_date_digest.as_slice())?;
            k_region.update(region_name.as_bytes());
            let k_region_digest = k_region.finalize().into_bytes();
            #[cfg(test)]
            debug!("k_region_digest: {:?}", k_region_digest.as_bstr());
            // k_service
            let mut k_service = HmacSha256::new_from_slice(k_region_digest.as_slice())?;
            k_service.update("odps".as_bytes());
            let k_service_digest = k_service.finalize().into_bytes();
            #[cfg(test)]
            debug!("k_service_digest: {:?}", k_service_digest.as_bstr());
            // signature_key
            let mut signature_key = HmacSha256::new_from_slice(k_service_digest.as_slice())?;
            signature_key.update("aliyun_v4_request".as_bytes());
            let v4_signature_key_digest = signature_key.finalize().into_bytes();
            #[cfg(test)]
            debug!(
                "v4_signature_key_digest: {:?}",
                v4_signature_key_digest.as_bstr()
            );
            // signature
            let mut signature = HmacSha1::new_from_slice(v4_signature_key_digest.as_slice())?;
            signature.update(canonical_str.as_bytes());
            let sig_str = general_purpose::STANDARD
                .encode(signature.finalize().into_bytes().as_slice())
                .to_string();
            Ok(format!("ODPS {}:{}", credential, sig_str))
        } else {
            let mut signature = HmacSha1::new_from_slice(self.access_id.as_bytes())?;
            signature.update(canonical_str.as_bytes());
            let sig_str = general_purpose::STANDARD
                .encode(signature.finalize().into_bytes().as_slice())
                .to_string();
            Ok(format!("ODPS {}:{}", self.access_id, sig_str))
        }
    }
}

pub struct RestClient<'a> {
    account: AliyunAccount<'a>,
    endpoint: &'a str,
    client: Client,
}

impl<'a> RestClient<'a> {
    pub fn new(account: AliyunAccount<'a>, endpoint: &'a str) -> Self {
        Self {
            account,
            endpoint,
            client: Client::new(),
        }
    }

    pub const fn odps_ua(&self) -> &'a str {
        "odps_rs/0.12.5.1 rustc/1.9.2 Windows/11"
    }

    pub async fn request(
        &self,
        url_str: &str,
        method: Method,
    ) -> Result<Response, GenerationError> {
        let mut request = self
            .client
            .request(method, Url::from_str(url_str)?)
            .header("User-Agent", self.odps_ua())
            .build()?;
        let region_name = self.get_region_name()?;
        self.account
            .sign_request(&mut request, self.endpoint, Some(region_name.as_str()))?;
        #[cfg(test)]
        println!("{:#?}", request);
        Ok(self.client.execute(request).await?)
    }

    fn get_region_name(&self) -> Result<String, GenerationError> {
        let regex = Regex::new(r"service\.([^.]+)\.([a-z]{4,10})\.[a-z]{6}(|-inc)\.com")?;
        if let Some(capture) = regex.captures(&self.endpoint) {
            if let Some(region_name) = capture.get(1) {
                return Ok(region_name.as_str().to_string());
            }
        }
        Err(GenerationError::NoRegionInEndpointError(
            regex::Error::Syntax(format!("endpoint: {}中无具体region信息", self.endpoint)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    fn test_account<'a>() -> AliyunAccount<'a> {
        AliyunAccount::new("aaa", "bbb")
    }
    #[tokio::test]
    async fn test_build_canonical_str() {
        let account = test_account();
        let url = "http://service.odps.aliyun.com/api/logview/host";
        let endpoint = "http://service.odps.aliyun.com/api";
        let mut request = Client::new()
            .get(url)
            .header("User-Agent", "Actix-web")
            .header("Date", "Sun, 01 Mar 2026 13:58:21 GMT")
            .build()
            .unwrap();
        // {'Date': 'Sun, 01 Mar 2026 13:58:21 GMT', 'Authorization': 'ODPS aaa/20260301/cn-hangzhou/odps/aliyun_v4_request:CUltXlK0yLTMk6X+H5UapJX8GaU='}
        account
            .sign_request(&mut request, endpoint, Some("cn-hangzhou"))
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_logviewhost() {
        let account = test_account();
        let rest_client = RestClient::new(
            account,
            "http://service.cn-hangzhou.maxcompute.aliyun.com/api",
        );
        match rest_client
            .request(
                "http://service.cn-hangzhou.maxcompute.aliyun.com/api/logview/host",
                Method::GET,
            )
            .await
        {
            Ok(response) => {
                println!("{:#?}", response);
                println!("{:?}", response.text().await.unwrap());
            }
            Err(e) => println!("{:?}", e),
        }
    }
}
