use arrow::array::RecordBatch;
use arrow::ipc::MetadataVersion;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow_schema::ArrowError;
use crc32c::Crc32cHasher;
use futures_util::SinkExt;
use log::debug;
use std::cmp::min;
use std::hash::Hasher;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct AsyncOdpsArrowWriter<W: AsyncWrite + Unpin> {
    chunk_size: usize,
    chunk_writer: W,
    generator: IpcDataGenerator,
    cur_chunk_pos: usize,
    crc: Crc32cHasher,
    global_crc: Crc32cHasher,
}

impl<W> AsyncOdpsArrowWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn new(chunk_size: usize, mut chunk_writer: W) -> Result<Self, ArrowError> {
        let _ = &mut chunk_writer.write_u32(chunk_size as u32).await?;
        let generator = IpcDataGenerator::default();
        let crc = Crc32cHasher::new(0);
        let global_crc = Crc32cHasher::new(0);
        // 2. 每次寫入 Batch 時
        Ok(Self {
            chunk_size,
            chunk_writer,
            generator,
            cur_chunk_pos: 0,
            crc,
            global_crc,
        })
    }

    pub async fn write_record(
        &mut self,
        record_batch_iter: impl Iterator<Item = RecordBatch>,
    ) -> Result<bool, ArrowError> {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let write_option = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut compression_context = CompressionContext::default();
        for record_batch in record_batch_iter {
            if let Ok((_encoded_message, encoded_data)) = self.generator.encode(
                &record_batch,
                &mut dictionary_tracker,
                &write_option,
                &mut compression_context,
            ) {
                //  单个batch起始位以0xff开头
                self.write_chunk_data(&[0xff, 0xff, 0xff, 0xff]).await?;
                // 2. 獲取長度
                let header_size = encoded_data.ipc_message.len() as u32;
                self.write_chunk_data(&header_size.to_le_bytes()).await?;
                self.write_chunk_data(&encoded_data.ipc_message).await?;
                self.write_chunk_data(&encoded_data.arrow_data).await?;
            }
        }
        // 完成后写入global crc校验码
        Ok(true)
    }

    pub async fn write_chunk_data(&mut self, buf: &[u8]) -> Result<(), ArrowError> {
        let mut bytes_written = 0;
        debug!("buf len: {}", buf.len());
        while bytes_written < buf.len() {
            let rest_buf_len = buf.len() - bytes_written;
            let rest_chunk_size = self.chunk_size - self.cur_chunk_pos;
            let len = min(rest_buf_len, rest_chunk_size);
            let chunk_data = &buf[bytes_written..bytes_written + len];
            // 写入数据
            let n = self.chunk_writer.write(chunk_data).await?;
            // 更新两种crc
            self.crc.write(&chunk_data[0..n]);
            let checksum = self.crc.finish();
            self.crc = Crc32cHasher::new(checksum as u32);
            self.global_crc.write(&chunk_data[0..n]);
            let global_checksum = self.global_crc.finish();
            self.global_crc = Crc32cHasher::new(global_checksum as u32);
            // 移动当前pos
            self.cur_chunk_pos += n;
            bytes_written += n;
            debug!(
                "rest buf len: {}, rest chunk size: {}, bytes_written: {}, current chunk crc: {:x}, current global crc: {:x}, current chunk pos: {}",
                rest_buf_len - n,
                rest_chunk_size - n,
                bytes_written,
                checksum,
                global_checksum,
                self.cur_chunk_pos
            );
            // 超出65536时需要写入crc校验码并且重置crc
            if self.cur_chunk_pos >= self.chunk_size {
                debug!("current chunk finished, write checksum");
                self.chunk_writer.write_u32(checksum as u32).await?;
                //重置chunk crc
                self.crc = Crc32cHasher::new(0);
                self.cur_chunk_pos = 0;
            }
        }
        Ok(())
    }

    // 数据已经写入完成了, 重置global crc
    pub async fn write_finish_tags(mut self) -> Result<(), ArrowError> {
        let global_checksum = self.global_crc.finish();
        debug!(
            "odps writer finished, global_checksum: {:x}",
            global_checksum
        );
        self.chunk_writer.write_u32(global_checksum as u32).await?;
        self.chunk_writer.flush().await?;
        self.global_crc = Crc32cHasher::new(0);
        self.cur_chunk_pos = 0;
        Ok(())
    }
}
