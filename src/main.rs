use crate::qwen_tts_realtime::{AudioFormat, QwenTtsRealtimeCallback, prepare_qwen_tts_realtime};
use base64::Engine;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

mod logging;
mod qwen_tts_realtime;

struct MyCallback {
    file: File,
    session_finished: Arc<AtomicBool>,
}

impl MyCallback {
    fn new(filename: &str, session_finished: Arc<AtomicBool>) -> Self {
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
        Self {
            file,
            session_finished,
        }
    }
}

impl QwenTtsRealtimeCallback for MyCallback {
    fn on_open(&self) {
        log::info!("Connection opened");
    }

    fn on_close(&self, close_msg: &str) {
        log::info!("Connection closed: {}", close_msg);
    }

    fn on_finish(&mut self, close_msg: &str) {
        log::info!("Session finished: {}", close_msg);
        self.session_finished.store(true, Ordering::Relaxed);
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

#[tokio::main]
async fn main() {
    let session_finished = Arc::new(AtomicBool::new(false));
    let text_to_synthesize = [
        "对吧~我就特别喜欢这种超市，",
        "尤其是过年的时候",
        "去逛超市",
        "就会觉得",
        "超级超级开心！",
        "想买好多好多的东西呢。",
    ];
    let session_finished_arc_clone = Arc::clone(&session_finished);
    let mut qwen_tts_realtime = prepare_qwen_tts_realtime(Some(Arc::new(Mutex::new(Box::new(
        MyCallback::new("result_24k.pcm", session_finished_arc_clone),
    )))))
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
    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if session_finished.load(Ordering::Relaxed) {
                    println!("TTS 任務已自動完成。");
                    break;
                }
            }
        }
    }
}
