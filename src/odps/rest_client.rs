use crate::common::errors::GenerationError;
use crate::odps::account::SignRequest;
use regex::Regex;
use reqwest::{Body, Client, ClientBuilder, Method, Proxy, Response, Url, Version};
use std::str::FromStr;
use futures_util::Stream;
use tokio_tungstenite::tungstenite::http::HeaderMap;


pub struct RestClient<T>
where
    T: SignRequest,
{
    account: T,
    client: Client,
}

impl<'a, T> RestClient<T>
where
    T: SignRequest,
{
    pub fn new(account: T) -> Self {

        let client = Client::builder()
            // .proxy(Proxy::all("http://127.0.0.1:8080").unwrap())
            // .danger_accept_invalid_certs(true)
            .build().unwrap();
        Self {
            account,
            client: client,
        }
    }

    pub const fn odps_ua(&self) -> &'a str {
        "odps_rs/0.12.5.1 rustc/1.9.2 Windows/11"
    }

    pub async fn request(
        &self,
        url_str: &str,
        method: Method,
        endpoint: &'a str,
        header_map: Option<HeaderMap>,
        body: Option<Body>,
    ) -> Result<Response, GenerationError> {
        let mut request_builder = self
            .client
            .request(method, Url::from_str(url_str)?)
            .header("User-Agent", self.odps_ua());
        if let Some(header_map) = header_map {
            request_builder = request_builder.headers(header_map);
        }
        if let Some(body) = body {
            request_builder = request_builder.body(body);
        }
        let mut request = request_builder.build()?;
        match Self::get_region_name(endpoint) {
            Ok(region_name) => {
                self.account
                    .sign_request(&mut request, endpoint, Some(region_name.as_str()))?
            }
            Err(_) => 
                self.account.sign_request(&mut request, endpoint, None)?,
        }
        #[cfg(test)]{
            use log::debug;
            debug!("{:#?}", request);
        }
        Ok(self.client.execute(request).await?)
    }

    fn get_region_name(endpoint: &str) -> Result<String, GenerationError> {
        let regex = Regex::new(r"service\.([^.]+)\.([a-z]{4,10})\.[a-z]{6}(|-inc)\.com")?;
        if let Some(capture) = regex.captures(endpoint)
            && let Some(region_name) = capture.get(1)
        {
            Ok(region_name.as_str().to_string())
        } else {
            Err(GenerationError::NoRegionInEndpointError(
                regex::Error::Syntax(format!("endpoint: {}中无具体region信息", endpoint)),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::odps::account::test_account;

    #[tokio::test]
    async fn test_get_logviewhost() {
        let account = test_account();
        let rest_client = RestClient::new(account);
        match rest_client
            .request(
                "https://service.cn-hangzhou.maxcompute.aliyun.com/api/logview/host",
                Method::GET,
                "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
                None,
                None,
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
