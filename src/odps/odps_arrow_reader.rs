use crate::odps::constants::ODPS_TO_ARROW_MAPPING;
use crate::odps::models::TunnelTableSchema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message};
use arrow_schema::ArrowError;
use bytes::{Buf, Bytes};
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

pub struct SkippedCursor<'a> {
    arrow_schema_arc: Arc<Vec<u8>>,
    arrow_data: &'a mut Bytes,
    position: usize,
    chunk_size: u32,
}

// pub struct SkippedCursor {
//     arrow_schema_arc: Arc<Vec<u8>>,
//     arrow_data: Vec<u8>,
//     position: usize,
// }

impl<'a> SkippedCursor<'a> {
    pub fn new(arrow_schema_arc: Arc<Vec<u8>>, odps_arrow_data: &'a mut Bytes) -> Self {
        let chunk_size = odps_arrow_data.get_u32();
        Self {
            arrow_schema_arc,
            arrow_data: odps_arrow_data,
            position: 0,
            chunk_size,
        }
    }
}

impl Read for SkippedCursor<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let schema = self.arrow_schema_arc.as_slice();
        let schema_len = schema.len();
        // 最后4个字节是crc data, 直接跳过
        let data_len = self.arrow_data.len() - 4;
        let total_len = schema_len + data_len;

        // 如果讀取位置已到末尾，返回 0
        if self.position >= total_len {
            return Ok(0);
        }

        let bytes_read = if self.position < schema_len {
            // 情況 A：目前還在讀取第一個數組 (Schema)
            let mut part1 = &schema[self.position..];
            let n = part1.read(buf)?;
            self.position += n;

            // 如果 buf 還沒填滿且還有第二部分，可以遞迴或繼續讀取
            if n < buf.len() && self.position == schema_len {
                let n2 = self.read(&mut buf[n..])?;
                n + n2
            } else {
                n
            }
        } else {
            // 情況 B：目前在讀取第二個數組 (Data)
            let data_pos = self.position - schema_len;
            let mut part2 = &self.arrow_data[data_pos..];
            let n = part2.read(buf)?;
            self.position += n;
            n
        };

        Ok(bytes_read)
    }
}

impl Seek for SkippedCursor<'_> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let schema_len = self.arrow_schema_arc.len();
        let data_len = self.arrow_data.len();
        let total_len = (schema_len + data_len) as i64;

        let new_pos: i64 = match pos {
            // 從 0 開始偏移
            SeekFrom::Start(offset) => offset as i64,

            // 從當前位置偏移
            SeekFrom::Current(offset) => self.position as i64 + offset,

            // 從總長度末尾偏移 (offset 通常為負數)
            SeekFrom::End(offset) => total_len + offset,
        };

        // 錯誤處理：不能 seek 到 0 之前
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            ));
        }

        // 更新位置（允許 seek 到超出總長度，這符合標準庫 Cursor 的行為）
        self.position = new_pos as usize;
        Ok(self.position as u64)
    }
}

pub struct OdpsArrowReader {
    arrow_schema_bytes: Arc<Vec<u8>>,
}

impl OdpsArrowReader {
    pub fn new(tunnel_schema: &TunnelTableSchema) -> Result<Self, ArrowError> {
        let arrow_schema = ODPS_TO_ARROW_MAPPING.odps_to_arrow_schema(tunnel_schema)?;
        let mut schema_bytes = Vec::new();
        // write the schema, set the written bytes to the schema
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let write_options = IpcWriteOptions::default();
        let encoded_message = data_gen.schema_to_bytes_with_dictionary_tracker(
            &arrow_schema,
            &mut dictionary_tracker,
            &write_options,
        );

        let (_aligned_size, _body_len) =
            write_message(&mut schema_bytes, encoded_message, &write_options)?;
        let schema_bytes_arc = Arc::new(schema_bytes);
        Ok(Self {
            arrow_schema_bytes: schema_bytes_arc,
        })
    }

    pub fn open_arrow_reader<'a>(
        &self,
        bytes: &'a mut Bytes,
    ) -> Result<StreamReader<SkippedCursor<'a>>, ArrowError> {
        let cursor = SkippedCursor::new(self.arrow_schema_bytes.clone(), bytes);
        Ok(StreamReader::try_new(cursor, None)?)
    }
}
