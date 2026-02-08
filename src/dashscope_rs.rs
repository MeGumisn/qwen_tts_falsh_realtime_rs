use raw_cpuid::CpuId;
use reqwest::header::HeaderMap;
use reqwest::{Client, Response};
use rustc_version::version;
use serde_json::{Map, Value, json};
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

struct Generation;

impl Generation {
    pub async fn call(
        model: &str,
        api_key: &str,
        url: &str,
        stream: bool,
        messages: Value,
        incremental_output: bool,
    ) -> Result<Response, GenerationError> {
        let client = Client::new();
        let builder = client.post(url);
        let header = Self::build_request_header(api_key, stream, incremental_output).await?;
        let body = Self::build_request_json(model, messages, stream, incremental_output).await;
        let response = builder.headers(header).json(&body).send().await?;
        Ok(response)
    }

    async fn build_request_header(
        api_key: &str,
        stream: bool,
        incremental_output: bool,
    ) -> Result<HeaderMap, GenerationError> {
        let mut headers = HeaderMap::new();
        if stream {
            headers.insert("Accept", "text/event-stream".parse()?);
            headers.insert("X-DashScope-SSE", "enable".parse()?);
        } else {
            headers.insert("Accept", "application/json".parse()?);
        }

        headers.insert("Authorization", format!("Bearer {}", api_key).parse()?);
        headers.insert("Content-Type", "application/json".parse()?);
        let rust_version = format!("{}", version().unwrap());

        // 1. 獲取 Platform (Windows 11 10.0.22621)
        let version = windows_version::OsVersion::current();

        // 構造 10.0.22621 格式
        let full_ver = format!("{}.{}.{}", version.major, version.minor, version.build);

        // 根據 Build 號判斷是 Win10 還是 Win11 (22000 以上為 Win11)
        let os_name = if version.build >= 22000 { "Windows-11" } else { "Windows-10" };

        let platform = format!("{}-{}-SP0", os_name, full_ver);

        // 2. 獲取 Processor (AMD64 Family... Stepping...)
        let cpuid = CpuId::new();
        let processor_info = if let Some(fms) = cpuid.get_feature_info() {
            let arch =  std::env::consts::ARCH; // 或透過環境變數 std::env::consts::ARCH 獲取
            let vendor = cpuid
                .get_vendor_info()
                .map(|v| v.to_string())
                .unwrap_or("Unknown".to_string());
            format!(
                "{} Family {} Model {} Stepping {}, {}",
                arch,
                fms.family_id(),
                fms.model_id(),
                fms.stepping_id(),
                vendor
            )
        } else {
            "Unknown Processor".to_string()
        };

        headers.insert(
            "user-agent",
            format!(
                "dashscope/{};rust/{};platform/{};processor/{};incremental_to_full/{}",
                "0.1.0",
                rust_version,
                platform,
                processor_info,
                if stream && !incremental_output { 1 } else { 0 }
            )
            .parse()?,
        );
        println!("{:#?}", headers);
        Ok(headers)
    }

    async fn build_request_json(
        model: &str,
        messages: Value,
        stream: bool,
        incremental_output: bool,
    ) -> Value {
        let parameters = if stream && incremental_output{
            json!({
                "stream": stream,
                "incremental_output": incremental_output
            })
        }else{
            json!({})
        };

        let body = json!({
           "model": model,
           "input": {
                "messages": messages
            },
            "parameters": parameters,
        });
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn test_generation() -> Result<(), GenerationError> {
        let model = "deepseek-r1";
        let stream = true;
        let res = Generation::call(
            model,
            env!("DASHSCOPE_API_KEY").to_string().as_str(),
            "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
            stream,
            json!([
            {
            "role": "user",
            "content": "你是谁?"
            }
            ]),
            true,
        )
        .await?;
        if !stream {
            println!("{:#?}", res);
            println!("{:#?}", res.text().await);
        } else {
            let mut stream = res.bytes_stream();
            while let Some(item) = stream.next().await {
                let chunk = item?;
                let text = String::from_utf8_lossy(&chunk);

                // 逐行過濾包含 "data:" 的內容
                for line in text.lines() {
                    if line.starts_with("data:") {
                        let json_str = line.strip_prefix("data:").unwrap().trim(); // 去掉 "data:" 並修剪空格

                        // 解析為 JSON Map
                        let json_map: Value = serde_json::from_str(json_str)?;

                        // 提取你需要的字段，例如推理內容
                        if let Some(content) =
                            json_map["output"]["choices"][0]["message"]["reasoning_content"]
                                .as_str()
                        {
                            print!("{}", content); // 實現逐字輸出效果
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
