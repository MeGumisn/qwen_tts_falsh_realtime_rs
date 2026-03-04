use crate::common::errors::GenerationError;
use base64::Engine;
use base64::engine::general_purpose;
use bstr::ByteSlice;
use chrono::Utc;
use hmac::{Hmac, Mac};
use log::debug;
use reqwest::Request;
use reqwest::header::HeaderMap;
use sha1::Sha1;
use std::collections::BTreeMap;
use url::Url;

pub trait SignRequest {
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
    ) -> Result<String, GenerationError>
    where
        Self: Sized;

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
            let url_params = u
                .query_pairs()
                .into_iter()
                .map(|(k, v)| {
                    // 判斷原始字串中是否存在 '='
                    // query_pairs() 無法直接告知有無等號，需檢查原始 query string 或手動判定
                    if v.is_empty() {
                        (k.into_owned(), None) // 視為無值參數
                    } else {
                        (k.into_owned(), Some(v.into_owned()))
                    }
                })
                .collect::<BTreeMap<String, Option<String>>>();
            // url_params.sort_by(|a, b| a.0.cmp(&b.0));
            // 直接extend会导致不带=号的param被添加上=, 这里改成手动处理
            // u.query_pairs_mut().clear().extend_pairs(url_params);
            let query_string = url_params
                .iter()
                .map(|(k, v)| match v {
                    Some(val) => format!("{}={}", k, val),
                    None => k.to_string(), // 不加等號
                })
                .collect::<Vec<_>>()
                .join("&");

            u.set_query(Some(&query_string));
            if let Some(sorted_url_query) = u.query() {
                let canonical_str =
                    Self::build_canonical_str(header, &method, u.path(), sorted_url_query)?;
                #[cfg(test)]
                debug!(
                    "canonical str: {}\nauth_str: {}",
                    canonical_str,
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
        let mut header_to_sign = BTreeMap::new();
        // 插入date
        if !header.contains_key("date") || !header.contains_key("Date") {
            let dt = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            header.insert("Date", dt.parse()?);
        }
        // 插入x-odps-开头的请求头
        for (key, value) in header {
            let k_str = key.as_str();
            if (k_str == "content-type" || k_str == "content-md5" || k_str.starts_with("x-odps-"))
                && let Ok(v_str) = value.to_str()
            {
                header_to_sign.insert(k_str, v_str);
            } else if (k_str == "date" || k_str == "Date")
                && let Ok(v_str) = value.to_str()
            {
                header_to_sign.insert("date", v_str);
            }
        }
        // 插入content-type 和 content-md
        if !header_to_sign.contains_key("content-type") {
            header_to_sign.insert("content-type", "");
        }
        if !header_to_sign.contains_key("content-md5") {
            header_to_sign.insert("content-md5", "");
        }

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
            let mut signature = HmacSha1::new_from_slice(self.secret_key.as_bytes())?;
            signature.update(canonical_str.as_bytes());
            let sig_str = general_purpose::STANDARD
                .encode(signature.finalize().into_bytes().as_slice())
                .to_string();
            Ok(format!("ODPS {}:{}", self.access_id, sig_str))
        }
    }
}

#[cfg(test)]
pub fn test_account<'a>() -> AliyunAccount<'a> {
    use crate::common::logging::init_logger;
    init_logger("debug");
    AliyunAccount::new(env!("ACCESS_ID"), env!("SECRET_KEY"))
}

#[cfg(test)]
mod tests {
    use crate::odps::account::{SignRequest, test_account};
    use reqwest::Client;

    #[tokio::test]
    async fn test_build_canonical_str() {
        let account = test_account();
        let url = "https://service.cn-hangzhou.maxcompute.aliyun.com/api/logview/host";
        let endpoint = "https://service.cn-hangzhou.maxcompute.aliyun.com/api";
        let mut request = Client::new()
            .get(url)
            .header("User-Agent", "Actix-web")
            .header("Date", "Sun, 01 Mar 2026 13:58:21 GMT")
            .build()
            .unwrap();
        ///
        /// ```json
        ///{
        ///        "user-agent": "odps_rs/0.12.5.1 rustc/1.9.2 Windows/11",
        ///         "date": "Tue, 03 Mar 2026 21:42:52 GMT",
        ///         "authorization": "ODPS LTAI5tKSLbi8M5UmbBtXxKW4/20260303/cn-hangzhou/odps/aliyun_v4_request:82UVPvSrqGD4LYcWU/a24dOwi0o=",
        ///}
        /// ```
        ///
        account
            .sign_request(&mut request, endpoint, Some("cn-hangzhou"))
            .unwrap();
    }
}
