use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
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
    fn on_event(&mut self, message: &str) -> bool;
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
                let callback_clone = callback.clone();
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
            });
        }
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

    async fn finish(&mut self) -> Result<(), Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::init_logger;
    use base64::Engine;
    use log::__private_api::log;
    use std::fs::{File, OpenOptions, create_dir_all};
    use std::io::Write;
    use std::path::Path;

    struct MyCallback {
        file: File,
    }

    impl MyCallback {
        fn new(filename: &str) -> Self {
            let p = Path::new(filename);
            if !p.exists() || !p.is_file() {
                if let Some(parent) = p.parent() {
                    create_dir_all(parent).unwrap();
                }
            }
            let file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(p)
                .unwrap();
            Self { file: file }
        }
    }
    impl QwenTtsRealtimeCallback for MyCallback {
        fn on_open(&self) {
            log::info!("Connection opened");
        }

        fn on_close(&self, close_msg: &str) {
            log::info!("Connection closed: {}", close_msg);
        }

        fn on_event(&mut self, message: &str) -> bool {
            log::info!("Received event: {}", message);
            let v: serde_json::Value = serde_json::from_str(message).unwrap();
            if let Some(event_type) = v.get("type") {
                if let Some(event_type_str) = event_type.as_str() {
                    match event_type_str {
                        "session.created" => {
                            log::info!("event: session created");
                        }
                        "response.audio.delta" => {
                            log::info!("event: response audio delta");
                            if let Some(recv_audio_b64) = v.get("delta") {
                                if let Some(recv_audio_b64_str) = recv_audio_b64.as_str() {
                                    let audio_bytes = base64::engine::general_purpose::STANDARD
                                        .decode(recv_audio_b64_str)
                                        .unwrap();
                                    self.file.write(&audio_bytes).unwrap();
                                }
                            }
                        }
                        "response.done" => {
                            log::info!("event: response done");
                        }
                        "session.finished" => {
                            log::info!("event: session finished");
                            return true;
                        }
                        _ => {
                            log::info!("unknown event type: {}", event_type_str);
                        }
                    }
                }
            }
            false
        }
    }

    async fn prepare_qwen_tts_realtime(
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

    #[tokio::test]
    async fn test_tts_generate() {
        let text_to_synthesize = [
            "对吧~我就特别喜欢这种超市，",
            "尤其是过年的时候",
            "去逛超市",
            "就会觉得",
            "超级超级开心！",
            "想买好多好多的东西呢。",
        ];
        let mut qwen_tts_realtime = prepare_qwen_tts_realtime(Some(Arc::new(Mutex::new(
            Box::new(MyCallback::new("result_24k.pcm")),
        ))))
        .await;
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
        let _ = qwen_tts_realtime.finish().await;
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for event");
    }
}
