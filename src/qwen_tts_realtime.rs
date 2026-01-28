use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use uuid::Uuid;

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
    const PCM_24000HZ_MONO_16BIT: Self = Self {
        format: "pcm",
        sample_rate: 24000,
        channels: "mono",
        bit_rate: "16bit",
        format_str: "pcm16",
    };
}

trait QwenTtsRealtimeCallback {
    fn on_open(&self);
    fn on_close(&self, close_msg: &str);
    fn on_event(&self, message: &str);
}

struct QwenTtsRealtime {
    stream_writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl QwenTtsRealtime {
    ///
    /// 与服务器建立连接，链接成功后需要update_session
    async fn new(
        model_name: &str,
        api_key: &str,
        url: Option<&str>,
        workspace: Option<&str>,
        callback: Arc<Option<Box<dyn QwenTtsRealtimeCallback + Sync + Send>>>,
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
        if let Some(callback) = callback.as_ref() {
            callback.on_open();
        }
        let (stream_writer, mut stream_reader) = stream.split();
        tokio::spawn(async move {
            while let Some(message) = stream_reader.next().await {
                match message {
                    Ok(msg) => {
                        if msg.is_text() {
                            log::info!("text message: {:?}", msg);
                            if let Some(callback) = callback.as_ref() {
                                callback.on_event(msg.to_text().unwrap());
                            }
                        } else if msg.is_close() {
                            log::info!("close: {:?}", msg);
                            if let Some(callback) = callback.as_ref() {
                                callback.on_close("Connection closed by server");
                            }
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
        });
        Self { stream_writer }
    }

    fn _generate_event_id(&self) -> String {
        format!("event_{}", Uuid::new_v4().to_string())
    }

    /// 建立连接成功后，需要添加session conf
    async fn update_session(
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

    async fn append_text(&mut self, text: &str) -> Result<(), Error> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::init_logger;
    use std::fs::{File, create_dir_all};
    use std::path::Path;

    struct MyCallback {
        file: File,
    }

    impl MyCallback {
        fn new(filename: &str) -> Self {
            let p = Path::new(filename);
            let file;
            if !p.exists() || !p.is_file() {
                if let Some(parent) = p.parent() {
                    create_dir_all(parent).unwrap();
                }
                file = File::create(p).unwrap();
            } else {
                file = File::open(p).unwrap();
            }
            Self { file: file }
        }
    }
    impl QwenTtsRealtimeCallback for MyCallback {
        fn on_open(&self) {
            println!("Connection opened");
        }

        fn on_close(&self, close_msg: &str) {
            println!("Connection closed: {}", close_msg);
        }

        fn on_event(&self, message: &str) {
            println!("Received event: {}", message);
        }
    }

    async fn prepare_qwen_tts_realtime<T>(callback: Arc<Option<T>>) -> QwenTtsRealtime {
        init_logger("info");
        let api_key = std::env::var("DASHSCOPE_API_KEY").unwrap();
        log::info!("{}", api_key);
        QwenTtsRealtime::new(
            "qwen3-tts-flash-realtime",
            api_key.as_str(),
            Some("wss://dashscope.aliyuncs.com/api-ws/v1/realtime"),
            None,
            Arc::new(None),
        )
        .await
    }
    #[tokio::test]
    async fn test_update_session() {
        let mut qwen_tts_realtime = prepare_qwen_tts_realtime::<u32>(Arc::new(None)).await;
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
        let text_to_synthesize = [
            "对吧~我就特别喜欢这种超市，",
            "尤其是过年的时候",
            "去逛超市",
            "就会觉得",
            "超级超级开心！",
            "想买好多好多的东西呢。",
        ];
        let mut qwen_tts_realtime = prepare_qwen_tts_realtime::<u32>(Arc::new(None)).await;
        let _ = qwen_tts_realtime
            .update_session(
                "Cherry",
                AudioFormat::PCM_24000HZ_MONO_16BIT,
                "server_commit",
            )
            .await;
        for text in text_to_synthesize.iter() {
            let _ = qwen_tts_realtime.append_text(text).await;
        }
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for event");
    }

    #[tokio::test]
    async fn test_tts_generate() {
        let mut qwen_tts_realtime =
            prepare_qwen_tts_realtime(Arc::new(Some(Box::new(MyCallback::new("result_24k.pcm")))))
                .await;
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
