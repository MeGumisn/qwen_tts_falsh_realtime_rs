use arrow::array::RecordBatch;
use arrow::ipc::MetadataVersion;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow_schema::ArrowError;
use log::{debug, info};
use std::cmp::{max, min};
use std::hash::Hasher;
use std::io::{Cursor, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite};
use tokio::sync::mpsc;

///
/// ## 用于生成可以通过服务端校验的arrow data
/// 单次发送到服务器的字节数组中各组成部分(从首到尾):
/// - chunk_size：u32类型, (这里直接使用了65536), 大端序
/// - arrow 指示符: 固定为[0xff, 0xff, 0xff, 0xff] 4字节
/// - arrow ipc_message长度位: 4字节, 小端序
/// - Metadata: 由 IpcDataGenerator 生成
/// - Data Body: arrow数据, 对应RecordBatch里面的columns
/// - chunk_crc: chunk的校验码, 需要使用 CRC32C (Castagnoli)计算, 4字节, 大端序
struct TrackedWriter {
    inner: Cursor<Vec<u8>>,
    buffer_size: usize,
    chunk_size: u32,
    position: u64,
    // 用于计算单个record_batch的checksum
    crc: crc32c::Crc32cHasher,
    // 用于计算整个chunk的checksum
    crc_all: crc32c::Crc32cHasher,
    // 用於將寫滿的 chunk 傳遞給外部
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl TrackedWriter {
    const BUFFER_SIZE: usize = 4;
    fn new(writer: Cursor<Vec<u8>>, chunk_size: u32, tx: mpsc::UnboundedSender<Vec<u8>>) -> Self {
        Self {
            inner: writer,
            buffer_size: Self::BUFFER_SIZE,
            chunk_size,
            position: 0,
            crc: crc32c::Crc32cHasher::new(0),
            crc_all: crc32c::Crc32cHasher::new(0),
            tx,
        }
    }

    /// chunk填充完毕时写入结束标识
    pub fn write_finish_tags(&mut self) -> std::io::Result<usize> {
        let checksum = self.crc_all.finish() as u32;
        info!("write_finish_tags, checksum: {}", checksum);
        let crc_len = self.inner.write(&checksum.to_be_bytes())?;
        self.crc_all = crc32c::Crc32cHasher::new(0);
        Ok(crc_len)
    }

    ///
    /// chunk 65536已经写满，重置crc并写入新chunk
    /// buffer 256*1024已经写满，需要发送到服务端
    fn take_chunk_data(&mut self, finished: bool, chunk_finished:bool) -> std::io::Result<()> {
        if finished {
            self.write_finish_tags()?;
        } else if chunk_finished {
            // 只有当chunk写满之后才添加crc
            let checksum = self.crc.finish() as u32;
            self.inner.write_all(&checksum.to_be_bytes())?;
            debug!("write chunk checksum: {} completed, current position:{}, reset crc", checksum, self.inner.position());
            self.crc = crc32c::Crc32cHasher::new(0);
        }
        // 当buffer已满，发送数据到服务端
        if finished || self.inner.position() >= self.buffer_size as u64 {
            info!("buffer is full or write finished");
            // 这里将cursor中的内容置换出来
            let data = self.inner.get_mut();
            let data_to_send = std::mem::take(data);
            info!("try send buffer data");
            // 在同步環境中強行阻塞等待異步結果
            // 如果外部接收端關閉，這裡會報錯
            if let Err(e) = self.tx.send(data_to_send) {
                return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e));
            }
            self.inner.set_position(0);
        }
        self.position = 0;
        if finished {
            info!("write operation finished, write chunk size and continue");
            self.inner.set_position(0);
            // 在头部重新写入chunk size信息
            self.inner
                .write_all(self.chunk_size.to_be_bytes().as_ref())?;
        }
        Ok(())
    }
}

impl Write for TrackedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut bytes_written = 0;
        while bytes_written < buf.len() {
            // 1.计算可用空间
            let rest_capacity = self.chunk_size as usize - self.position as usize;
            // 预留4位给crc
            let rest_buffer_capacity = self.buffer_size - self.inner.position() as usize;
            let write_len = min(rest_capacity, buf.len() - bytes_written);
            let write_len = min(write_len, rest_buffer_capacity);
            //
            let n = self
                .inner
                .write(&buf[bytes_written..bytes_written + write_len])?;
            info!("write data to buf, data length: {}", n);
            self.crc.write(&buf[bytes_written..bytes_written + n]);
            debug!("update crc checksum");
            let cur_crc_checksum = self.crc.finish() as u32;
            self.crc = crc32c::Crc32cHasher::new(cur_crc_checksum);
            debug!("update crc all checksum");
            self.crc_all.write(&buf[bytes_written..bytes_written + n]);
            let cur_crc_all_checksum = self.crc_all.finish() as u32;
            self.crc_all = crc32c::Crc32cHasher::new(cur_crc_all_checksum);

            self.position += n as u64;
            // 发现chunk写满了
            if self.position >= self.chunk_size as u64 {
                debug!(
                    "当前chunk pos: {}, 当前crc: {:#?}, crc_all: {:#?}",
                    self.position,
                    cur_crc_checksum,
                    cur_crc_all_checksum
                );
                info!("chunk size limit exceeded, try write checksum");
                self.take_chunk_data(false, true)?;
                // 重置当前pos, 准备写入下个chunk的数据
                self.position = 0;
            }
            let inner_position = self.inner.position();
            if inner_position >= self.buffer_size  as u64 {
                info!("buffer size limit exceeded, try send data");
                self.take_chunk_data(false, false)?;
                self.position = 0;
            }
            bytes_written += n;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // 这里将cursor中的内容置换出来
        self.inner.flush()
    }
}

impl AsyncWrite for TrackedWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        // 因為 inner 是 Cursor (內存操作)，所以永遠不會回傳 Pending
        let n = self.write(buf)?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.inner.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // 這裡觸發最後的 finish 標籤
        self.take_chunk_data(true, false)?;
        Poll::Ready(Ok(()))
    }
}

pub struct OdpsArrowWriter {
    chunk_size: u32,
    tracked_writer: TrackedWriter,
    generator: IpcDataGenerator,
    used_size: usize,
}

impl OdpsArrowWriter {
    pub async fn new(
        chunk_size: u32,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Result<Self, ArrowError> {
        // 这里要给前后各留4个字节
        let chunk_buffer = Vec::with_capacity(TrackedWriter::BUFFER_SIZE);
        let mut cursor = Cursor::new(chunk_buffer);
        cursor.write_all(chunk_size.to_be_bytes().as_ref())?;
        let tracked_writer = TrackedWriter::new(cursor, chunk_size, tx);
        // 开头4个字节固定是chunk_size, 结尾4个字节固定是crc
        let generator = IpcDataGenerator::default();
        // 2. 每次寫入 Batch 時

        Ok(Self {
            chunk_size,
            tracked_writer,
            generator,
            used_size: 0,
        })
    }

    pub async fn write_record(
        &mut self,
        record_batch_iter: impl Iterator<Item=RecordBatch>,
    ) -> Result<bool, ArrowError> {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let write_option = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut compression_context = CompressionContext::default();

        self.tracked_writer.write_all(&[0xff, 0xff, 0xff, 0xff])?;
        for record_batch in record_batch_iter {
            if let Ok((_encoded_message, encoded_data)) = self.generator.encode(
                &record_batch,
                &mut dictionary_tracker,
                &write_option,
                &mut compression_context,
            ) {
                // 2. 獲取長度
                let header_size = encoded_data.ipc_message.len() as i32;
                self.tracked_writer.write_all(&header_size.to_le_bytes())?;
                self.tracked_writer.write_all(&encoded_data.ipc_message)?;
                self.tracked_writer.write_all(&encoded_data.arrow_data)?;
            }
        }
        Ok(true)
    }

    pub fn close(&mut self) -> Result<(), ArrowError> {
        self.tracked_writer.take_chunk_data(true, false)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;
    use crc32c::Crc32cHasher;
    #[test]
    fn test_crc() {
        let python_bytes = b"\xff\xff\xff\xff\xc8\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x04\x00\x18\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\n\x00\x18\x00\x0c\x00\x04\x00\x08\x00\n\x00\x00\x00l\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00lkj\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00";
        let rust_bytes = b"\xff\xff\xff\xff\xc8\0\0\0\x10\0\0\0\x0c\0\x1a\0\x18\0\x17\0\x04\0\x08\0\x0c\0\0\0 \0\0\0(\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x03\x04\0\n\0\x18\0\x0c\0\x08\0\x04\0\n\0\0\0<\0\0\0\x10\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\x02\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x05\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x08\0\0\0\0\0\0\0\x08\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\x03\0\0\0\0\0\0\0\x18\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0 \0\0\0\0\0\0\0\x08\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\0\0\0\0\x03\0\0\0lkj\0\0\0\0\0\xff\0\0\0\0\0\0\0\x04\0\0\0\0\0\0\0";
        let mut crc1 = Crc32cHasher::new(0);
        crc1.write(&rust_bytes[0..4]);
        let a = crc1.finish();
        crc1.write(&rust_bytes[4..8]);
        let b = crc1.finish();
        crc1.write(&rust_bytes[8..208]);
        let c = crc1.finish();
        crc1.write(&rust_bytes[208..244]);
        let d = crc1.finish();
        // let crc1 = crc1.finish() as u32;
        // let crc2 = crc32c::crc32c(rust_bytes);
        // let mut crc2 = Crc32cHasher::new(0);
        // crc2.write(rust_bytes);
        // let crc2 = crc2.finish() as u32;
        // // 去掉4个\xff是 1668077719
        println!("{}, {}, {}, {}", a, b, c, d);
    }

    #[test]
    fn test_crc_py() {
        let mut payload = Vec::new();
        let _ = File::open("./payload").unwrap().read_to_end(&mut payload);
        // 开头4位固定 65536， 4字节
        // 末尾4位 crc校验值
        let chunk_size = u32::from_be_bytes(payload[0..4].try_into().unwrap());
        let len = payload.len();

        let data_start = min(65540*2+4, len - 4);
        // data_end = crc_start
        let data_end = min(65540*3, len - 4);
        let crc_end = data_end+4;

        let crc_data = u32::from_be_bytes(payload[data_end..crc_end].try_into().unwrap());

        let calculated_crc = crc32c::crc32c(&payload[data_start..data_end]);
        println!("vec size: {}, chunk size: {}, crc data: {}, calculated crc data: {}", payload.len(), chunk_size, crc_data, calculated_crc);
    }
}
