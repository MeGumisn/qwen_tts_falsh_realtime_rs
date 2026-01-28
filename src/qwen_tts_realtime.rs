use crate::logging::init_logger;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

pub struct AudioFormat<'a> {
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
    pub const PCM_24000HZ_MONO_16BIT: Self = Self {
        format: "pcm",
        sample_rate: 24000,
        channels: "mono",
        bit_rate: "16bit",
        format_str: "pcm16",
    };
}

pub trait QwenTtsRealtimeCallback {
    fn on_open(&self);
    fn on_close(&self, close_msg: &str);
    fn on_finish(&mut self, close_msg: &str);
    fn on_event(&mut self, message: &str) -> bool;
}

pub struct QwenTtsRealtime {
    stream_writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl QwenTtsRealtime {
    ///
    /// 与服务器建立连接，链接成功后需要update_session
    pub async fn new(
        model_name: &str,
        api_key: &str,
        url: Option<&str>,
        workspace: Option<&str>,
        callback: Option<Arc<Mutex<Box<dyn QwenTtsRealtimeCallback + Sync + Send>>>>,
    ) -> Self {
        let url = if let Some(url) = url {
            format!("{}?model={}", url, model_name)
        } else {
            format!(
                "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={}",
                model_name
            )
        };
        let ua = format!(
            "dashscope/1.18.0; rust/{};\
        platform/{}\
        processor/{}",
            option_env!("RUSTC_VERSION").unwrap_or("Unknown Rustc Version"),
            std::env::consts::OS,
            std::env::consts::ARCH,
        );

        let mut request = url.as_str().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("user-agent", ua.parse().unwrap());
        request.headers_mut().insert(
            "Authorization",
            format!("bearer {}", api_key).parse().unwrap(),
        );
        if let Some(workspace) = workspace {
            request
                .headers_mut()
                .insert("X-DashScope-WorkSpace", workspace.parse().unwrap());
        }
        let (stream, response) = connect_async(request).await.expect("Failed to connect");
        log::info!("服务器响应状态码: {}", response.status());
        response.headers().into_iter().for_each(|(name, value)| {
            log::info!("响应头: {}: {:?}", name, value);
        });

        let (stream_writer, mut stream_reader) = stream.split();
        // 有回调时这里异步任务循环维持连接， 没有回调时，这个函数结束stream就自动close了
        if let Some(callback) = callback {
            callback.lock().await.as_ref().on_open();
            tokio::spawn(async move {
                let callback_clone = Arc::clone(&callback);
                while let Some(message) = stream_reader.next().await {
                    match message {
                        Ok(msg) => {
                            if msg.is_text() {
                                log::info!("text message: {:?}", msg);
                                let need_aborted = callback_clone
                                    .lock()
                                    .await
                                    .as_mut()
                                    .on_event(msg.to_text().unwrap());
                                if need_aborted {
                                    break;
                                }
                            } else if msg.is_close() {
                                log::info!("close: {:?}", msg);
                                callback_clone
                                    .lock()
                                    .await
                                    .as_ref()
                                    .on_close("Connection closed by server");
                                break;
                            } else {
                                log::info!("other message: {:?}", msg);
                            }
                        }
                        Err(e) => {
                            log::error!("Error receiving message: {}", e);
                            break;
                        }
                    }
                }
                log::info!("reader task ended");
                callback_clone
                    .lock()
                    .await
                    .as_mut()
                    .on_finish("reader task ended");
            });
        }
        Self { stream_writer }
    }

    fn _generate_event_id(&self) -> String {
        format!("event_{}", Uuid::new_v4().to_string())
    }

    /// 建立连接成功后，需要添加session conf
    pub async fn update_session(
        &mut self,
        voice: &str,
        response_format: AudioFormat<'_>,
        mode: &str,
    ) -> Result<(), Error> {
        let config = json!({
            "voice":voice,
            "mode":mode,
            "response_format":response_format.format,
            "sample_rate":response_format.sample_rate,
        });
        let msg = json!({
            "event_id": self._generate_event_id(),
            "type": "session.update",
            "session": config,
        });
        self.stream_writer
            .send(Message::text(msg.to_string()))
            .await?;
        log::info!("send: {}", msg);
        Ok(())
    }

    pub async fn append_text(&mut self, text: &str) -> Result<(), Error> {
        let msg = json!({
                                "event_id": self._generate_event_id(),
                    "type": "input_text_buffer.append",
                    "text": text,
        });
        self.stream_writer
            .send(Message::text(msg.to_string()))
            .await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), Error> {
        let msg = json!({
            "event_id": self._generate_event_id(),
            "type": "session.finish"
        });
        self.stream_writer
            .send(Message::text(msg.to_string()))
            .await?;
        Ok(())
    }
}

pub async fn prepare_qwen_tts_realtime(
    callback: Option<Arc<Mutex<Box<dyn QwenTtsRealtimeCallback + Sync + Send>>>>,
) -> QwenTtsRealtime {
    init_logger("info");
    let api_key = std::env::var("DASHSCOPE_API_KEY").unwrap();
    log::info!("{}", api_key);
    QwenTtsRealtime::new(
        "qwen3-tts-flash-realtime",
        api_key.as_str(),
        Some("wss://dashscope.aliyuncs.com/api-ws/v1/realtime"),
        None,
        callback,
    )
        .await
}
#[cfg(test)]
mod tests {
    use super::*;


    #[tokio::test]
    async fn test_update_session() {
        let mut qwen_tts_realtime = prepare_qwen_tts_realtime(None).await;
        let _ = qwen_tts_realtime
            .update_session(
                "Cherry",
                AudioFormat::PCM_24000HZ_MONO_16BIT,
                "server_commit",
            )
            .await;
        println!("所有文本已發送，Reader 正在後台運行。按 Ctrl+C 結束...");
        tokio::signal::ctrl_c().await.unwrap();
    }

    #[tokio::test]
    async fn test_append_text() {
        let mut qwen_tts_realtime = prepare_qwen_tts_realtime(None).await;
        let _ = qwen_tts_realtime
            .update_session(
                "Cherry",
                AudioFormat::PCM_24000HZ_MONO_16BIT,
                "server_commit",
            )
            .await;
        let _ = qwen_tts_realtime
            .append_text("你好，欢迎使用Qwen TTS实时语音合成服务。")
            .await;

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for event");
    }
}
