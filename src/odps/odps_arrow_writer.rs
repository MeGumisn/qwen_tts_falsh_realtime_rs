use crate::odps::models::TunnelTableSchema;
use arrow::array::RecordBatch;
use arrow::ipc::MetadataVersion;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow_schema::ArrowError;
use bstr::ByteSlice;
use log::{debug, info};
use std::cmp::min;
use std::hash::Hasher;
use std::io::{Cursor, Write};
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
    fn new(writer: Cursor<Vec<u8>>, chunk_size: u32, tx: mpsc::UnboundedSender<Vec<u8>>) -> Self {
        Self {
            inner: writer,
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

    fn take_chunk_data(&mut self, finished: bool) -> std::io::Result<()> {
        debug!("arrow data: {:#?}", self.inner.get_ref().as_bstr());
        if finished {
            self.write_finish_tags()?;
        } else {
            let checksum = self.crc.finish() as u32;
            self.inner.write_all(&checksum.to_be_bytes())?;
            self.crc = crc32c::Crc32cHasher::new(0);
        }
        // 这里将cursor中的内容置换出来
        let data = self.inner.get_mut();
        let data_to_send = std::mem::take(data);

        info!("try send chunk data");
        // 在同步環境中強行阻塞等待異步結果
        // 如果外部接收端關閉，這裡會報錯
        if let Err(e) = self.tx.send(data_to_send) {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e));
        }
        self.inner.set_position(0);
        self.position = 0;
        if !finished {
            info!("write operation not finish, write chunk size and continue");
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
            let write_len = min(rest_capacity, buf.len() - bytes_written);
            //
            let n = self
                .inner
                .write(&buf[bytes_written..bytes_written + write_len])?;
            info!("write data to buf, data length: {}", n);
            self.crc.write(&buf[bytes_written..bytes_written + n]);
            let cur_crc_checksum = self.crc.finish() as u32;
            self.crc = crc32c::Crc32cHasher::new(cur_crc_checksum);
            self.crc_all.write(&buf[bytes_written..bytes_written + n]);
            let cur_crc_all_checksum = self.crc_all.finish() as u32;
            self.crc_all = crc32c::Crc32cHasher::new(cur_crc_all_checksum);
            debug!(
                "当前buf: {:?}, 当前crc: {:#?}, crc_all: {:#?}",
                buf.as_bstr(),
                cur_crc_checksum,
                cur_crc_all_checksum
            );
            self.position += n as u64;
            // 发现chunk写满了
            if self.position >= self.chunk_size as u64 {
                info!("chunk size limit exceeded, try refresh");
                self.take_chunk_data(false)?;
                info!("reset position");
                self.inner.set_position(0);
                self.position = 0;
                // 在头部重新写入chunk size信息
                self.inner
                    .write_all(self.chunk_size.to_be_bytes().as_ref())?;
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
        let buffer = Vec::with_capacity(chunk_size as usize + 8);
        let mut cursor = Cursor::new(buffer);
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
        record_batch_iter: impl Iterator<Item = RecordBatch>,
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
        self.tracked_writer.take_chunk_data(true)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
}
