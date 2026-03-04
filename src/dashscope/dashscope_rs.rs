use crate::common::errors::GenerationError;
use crate::common::{get_platform_info, get_processor_info};
use crate::dashscope::parameters::{
    DashScopeRequestBodyBuilder, HistoryMessage, Message, Parameters,
};
use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use reqwest::{Client, Response};
use rustc_version::version;
use serde::de::Error;
use serde_json::{Value, json};

pub struct Generation;

impl Generation {
    const fn base_url() -> &'static str {
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    }

    pub async fn call(
        model: &str,
        prompt: Option<&str>,
        history: Option<Vec<HistoryMessage>>,
        api_key: &str,
        messages: Option<Vec<Message>>,
        plugins: Option<&str>,
        workspace: Option<&str>,
        parameter: Parameters,
    ) -> Result<Response, GenerationError> {
        if prompt.is_none() && messages.is_none() {
            return Err(GenerationError::SerdeJsonError(serde_json::Error::custom(
                "prompt和messages不能同时为None",
            )));
        }
        let client = Client::new();
        let builder = client.post(Self::base_url());
        let header = Self::build_request_header(
            api_key,
            parameter.stream.unwrap_or(false),
            parameter.incremental_output.unwrap_or(false),
            workspace,
            plugins,
        )
        .await?;

        let body = DashScopeRequestBodyBuilder::new(model, prompt, history, messages, parameter)
            .build()?;
        println!("{:#?}", serde_json::to_string(&body)?);
        let response = builder
            .headers(header)
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;
        Ok(response)
    }

    pub async fn print_response(res: Response, stream: bool) -> Result<(), GenerationError> {
        if !stream {
            println!("{:#?}", res);
            println!("{:#?}", res.text().await);
        } else {
            let mut stream = res.bytes_stream();
            let mut is_reasoning_answer = true;
            let separator = "=".repeat(20);
            print!("\n{}思考内容{}\n", separator, separator);
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
                        if is_reasoning_answer
                            && let Some(content) =
                                json_map["output"]["choices"][0]["message"]["reasoning_content"]
                                    .as_str()
                            && !content.is_empty()
                        {
                            print!("{}", content); // 實現逐字輸出效果
                        }

                        if let Some(content) =
                            json_map["output"]["choices"][0]["message"]["content"].as_str()
                            && !content.is_empty()
                        {
                            if is_reasoning_answer {
                                is_reasoning_answer = false;
                                print!("\n{}回复内容{}\n", separator, separator);
                            }
                            print!("{}", content);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn build_request_header(
        api_key: &str,
        stream: bool,
        incremental_output: bool,
        workspace: Option<&str>,
        plugins: Option<&str>,
    ) -> Result<HeaderMap, GenerationError> {
        let mut headers = HeaderMap::new();
        if stream {
            headers.insert("Accept", "text/event-stream".parse()?);
            headers.insert("X-DashScope-SSE", "enable".parse()?);
            headers.insert("X-Accel-Buffering", "no".parse()?);
        } else {
            headers.insert("Accept", "application/json".parse()?);
        }
        if let Some(workspace) = workspace {
            headers.insert("X-DashScope-WorkSpace", workspace.parse()?);
        }

        if let Some(plugins) = plugins {
            headers.insert("X-DashScope-Plugins", plugins.parse()?);
        }

        headers.insert("Authorization", format!("Bearer {}", api_key).parse()?);
        headers.insert("Content-Type", "application/json".parse()?);
        let rust_version = format!("{}", version().unwrap());

        // 1. 獲取 Platform (Windows 11 10.0.22621)
        let (os_name, full_ver) = get_platform_info();
        let platform = format!("{}-{}-SP0", os_name, full_ver);
        // 2. 獲取 Processor (AMD64 Family... Stepping...)
        let processor_info = get_processor_info();

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
        #[cfg(test)]
        println!("{:#?}", headers);
        Ok(headers)
    }

    async fn build_request_json(
        model: &str,
        messages: Value,
        stream: bool,
        incremental_output: bool,
    ) -> Value {
        let parameters = if stream && incremental_output {
            json!({
                "stream": stream,
                "incremental_output": incremental_output
            })
        } else {
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
    use serde_json::{from_value, json};

    #[tokio::test]
    async fn test_generation() -> Result<(), GenerationError> {
        let model = "deepseek-r1";
        let stream = true;
        let messages = vec![Message::new("user".to_string(), "你是谁?".to_string())];
        let parameter = json!({
            "stream":stream,
            "incremental_output": stream
        });
        let parameter = from_value::<Parameters>(parameter)?;
        let res = Generation::call(
            model,
            None,
            None,
            env!("DASHSCOPE_API_KEY"),
            Some(messages),
            None,
            None,
            parameter,
        )
        .await?;
        Generation::print_response(res, stream).await?;
        Ok(())
    }
}
