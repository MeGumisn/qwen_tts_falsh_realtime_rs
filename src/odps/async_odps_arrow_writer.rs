// use std::hash::Hasher;
// use tokio::io::{AsyncWrite, AsyncWriteExt, DuplexStream};
// use std::pin::Pin;
// use std::task::{Context, Poll};
// use std::io::{Cursor, Write};
// use std::cmp::min;
// use tokio::sync::mpsc;
//
// struct TrackedWriter {
//     inner: Cursor<Vec<u8>>,
//     buffer_size: usize,
//     chunk_size: u32,
//     position: u64,
//     crc: crc32c::Crc32cHasher,
//     crc_all: crc32c::Crc32cHasher,
//     tx: mpsc::UnboundedSender<Vec<u8>>,
//     // 這裡我們不再需要單獨的 chunk_writer，
//     // 因為 TrackedWriter 本身就是一個 AsyncWrite，可以直接對接給 Reqwest
// }
//
// impl AsyncWrite for TrackedWriter {
//     fn poll_write(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &[u8],
//     ) -> Poll<std::io::Result<usize>> {
//         // 因為 AsyncWrite 要求非阻塞，但你的 CRC 和 Cursor 操作是同步的（CPU bound）
//         // 我們可以直接在 poll_write 中執行這些邏輯
//
//         let mut bytes_written = 0;
//         let buf_len = buf.len();
//
//         while bytes_written < buf_len {
//             // 1. 計算剩餘空間
//             let rest_capacity = self.chunk_size as u64 - self.position;
//             let current_pos = self.inner.position();
//             let rest_buffer_capacity = self.buffer_size as u64 - current_pos;
//
//             let write_len = min(rest_capacity, (buf_len - bytes_written) as u64);
//             let write_len = min(write_len, rest_buffer_capacity) as usize;
//
//             if write_len == 0 {
//                 // 如果緩衝區滿了，我們需要先觸發發送
//                 // 注意：這裡的 take_chunk_data 必須是同步的或者是能立即完成的
//                 // 如果發送過程涉及 await，建議在 poll_write 外部處理
//                 if let Err(e) = self.as_mut().sync_take_chunk(false, false) {
//                     return Poll::Ready(Err(e));
//                 }
//                 continue;
//             }
//
//             // 2. 寫入數據與更新 CRC (同步操作)
//             let slice = &buf[bytes_written..bytes_written + write_len];
//             std::io::Write::write_all(&mut self.inner, slice)?;
//
//             let _ = self.crc.write(slice);
//             self.crc_all.write(slice);
//
//             self.position += write_len as u64;
//             bytes_written += write_len;
//
//             // 3. 檢查是否達到 Chunk 或 Buffer 上限
//             if self.position >= self.chunk_size as u64 {
//                 self.as_mut().sync_take_chunk(false, true)?;
//                 self.position = 0;
//             } else if self.inner.position() >= self.buffer_size as u64 {
//                 self.as_mut().sync_take_chunk(false, false)?;
//                 // 這裡不重置 position，因為 chunk 還沒完
//             }
//         }
//
//         Poll::Ready(Ok(bytes_written))
//     }
//
//     fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
//         Poll::Ready(std::io::Write::flush(&mut self.inner))
//     }
//
//     fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
//         Poll::Ready(Ok(()))
//     }
// }
//
// impl TrackedWriter {
//     // 將原本異步的發送邏輯簡化。如果是 UnboundedSender，send 是同步的。
//     fn sync_take_chunk(&mut self, finished: bool, is_chunk_end: bool) -> std::io::Result<()> {
//         let data = self.inner.get_mut();
//         let payload = std::mem::take(data);
//
//         if !payload.is_empty() {
//             // 這裡直接封裝成 Chunked 格式發送給 tx
//             // 讓接收端直接寫入 reqwest 的 duplex stream
//             // let mut chunk = Vec::with_capacity(payload.len() + 16);
//             // write!(&mut chunk, "{:X}\r\n", payload.len())?;
//             // chunk.extend_from_slice(&payload);
//             // chunk.extend_from_slice(b"\r\n");
//
//             self.tx.send(payload).map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))?;
//         }
//
//         // if finished {
//         //     self.tx.send(b"0\r\n\r\n".to_vec()).ok();
//         // }
//
//         self.inner.set_position(0);
//         Ok(())
//     }
// }