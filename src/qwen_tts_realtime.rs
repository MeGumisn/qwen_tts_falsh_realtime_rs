use std::collections::HashMap;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

struct AudioFormat<'a> {
    format: &'a str,
    sample_rate: u32,
    channels: &'a str,
    bit_rate: &'a str,
    format_str: &'a str,
}

impl<'a> AudioFormat<'a> {
    fn new(
        format: &'a str,
        sample_rate: u32,
        channels: &'a str,
        bit_rate: &'a str,
        format_str: &'a str,
    ) -> Self {
        Self {
            format: format,
            sample_rate: sample_rate,
            channels: channels,
            bit_rate: bit_rate,
            format_str: format_str,
        }
    }
    pub const fn PCM_24000HZ_MONO_16BIT() -> Self {
        Self {
            format: "pcm",
            sample_rate: 24000,
            channels: "mono",
            bit_rate: "16bit",
            format_str: "pcm16",
        }
    }
}

trait QwenTtsRealtimeCallback {
    fn on_open(&self);
    fn on_close(&self);
    fn on_event(&self);
}

struct QwenTtsRealtime<'a> {
    url: String,
    api_key: &'a str,
    workspace: Option<&'a str>,
    callback: Option<&'a dyn QwenTtsRealtimeCallback>,
}

impl<'a> QwenTtsRealtime<'a> {
    pub fn new(
        model_name: &str,
        api_key: &'a str,
        url: Option<&str>,
        callback: Option<&'a dyn QwenTtsRealtimeCallback>,
    ) -> Result<Self, Error> {
        let url = if let Some(url) = url {
            format!("{}?model={}", url, model_name)
        } else {
            format!(
                "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={}",
                model_name
            )
        };
        Ok(Self {
            url: url,
            api_key: api_key,
            workspace: None,
            callback: callback,
        })
    }

    ///
    /// 与服务器建立连接，链接成功后需要update_session
    async fn connect(&self) -> Result<(), Error> {
        let ua = format!(
            "dashscope/1.18.0; rust/{};\
        platform/{}\
        processor/{}",
            option_env!("RUSTC_VERSION").unwrap_or("Unknown Rustc Version"),
            std::env::consts::OS,
            std::env::consts::ARCH,
        );

        let mut request = self.url.as_str().into_client_request()?;
        request.headers_mut().insert("user-agent", ua.parse()?);
        request
            .headers_mut()
            .insert("Authorization", format!("bearer {}", self.api_key).parse()?);
        if let Some(workspace) = self.workspace {
            request
                .headers_mut()
                .insert("X-DashScope-WorkSpace", workspace.parse()?);
        }
        let (stream, response) = connect_async(request).await.expect("Failed to connect");
        println!("服务器响应状态码: {}", response.status());
        for (name, value) in response.headers() {
            println!("响应头: {}: {:?}", name, value);
        }
        if let Some(callback) = self.callback {
            callback.on_open();
        }
        Ok(())
    }

    /// 建立连接成功后，需要添加session conf
    async fn update_session(
        &mut self,
        voice: &str,
        response_format: AudioFormat<'_>,
        mode: &str,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_connection() {
        let qwen_tts_realtime = QwenTtsRealtime::new(
            "qwen3-tts-flash-realtime",
            "",
            Some("wss://dashscope.aliyuncs.com/api-ws/v1/realtime"),
            None,
        );
        let _ = qwen_tts_realtime.unwrap().connect().await;
    }
}
