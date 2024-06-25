use super::offset::Offset;
use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

pub trait Encoder: Sync + Send + 'static {
    type Error;
    fn encode(&self, buffer: &mut BytesMut) -> Result<usize, Self::Error>;
}

//////////////////////////////////////////////////////////////
/// 解码
//////////////////////////////////////////////////////////////
pub trait Decoder: Sync + Sized + Send + 'static {
    // 错误类型
    type Error;
    // 将bytes解析为对应的报文
    fn decode(bytes: Bytes) -> Result<Self, Self::Error>;
}

impl Encoder for Offset {
    type Error = anyhow::Error;

    fn encode(&self, buffer: &mut BytesMut) -> Result<usize, Self::Error> {
        buffer.put_u64(self.offset as u64);
        buffer.put_u64(self.len as u64);
        Ok(16)
    }
}

impl Decoder for Offset {
    type Error = anyhow::Error;

    fn decode(mut bytes: Bytes) -> Result<Self, Self::Error> {
        let len = bytes.len();
        if len != 16 {
            return Err(anyhow::anyhow!("invalid offset length: {}", len));
        }
        let offset = bytes.get_u64() as usize;
        let len = bytes.get_u64() as usize;
        Ok(Offset { offset, len })
    }
}

impl Encoder for Vec<Offset> {
    type Error = anyhow::Error;

    fn encode(&self, buffer: &mut BytesMut) -> Result<usize, Self::Error> {
        let mut buf = BytesMut::new();
        let len = self.len();
        self.into_iter().for_each(|offset| {
            let _ = offset.encode(&mut buf);
        });

        buffer.put_u32(len as u32);
        buffer.put(buf);
        Ok(len * 16 + 4)
    }
}

impl Decoder for Vec<Offset> {
    type Error = anyhow::Error;

    fn decode(mut bytes: Bytes) -> Result<Self, Self::Error> {
        let vec_len = bytes.get_u32();
        let mut vec = Vec::with_capacity(vec_len as usize);
        while bytes.len() > 0 {
            let a = bytes.split_to(16);
            let offset = Offset::decode(a)?;
            vec.push(offset);
        }
        Ok(vec)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::wal::offset::Offset;

    use super::{Decoder, Encoder};

    #[test]
    fn offset_encode_and_decode() {
        let offset = Offset {
            offset: 1234,
            len: 5678,
        };

        let mut buf = BytesMut::new();
        let len = offset.encode(&mut buf).unwrap();
        println!("len: {}", len);
        let new_offset = Offset::decode(buf.freeze()).unwrap();
        println!("offset: {:?}", new_offset);
    }

    #[test]
    fn offset_vec_encode_and_decode() {
        let mut offsets = Vec::new();
        for i in 0..10 {
            let offset = Offset {
                offset: i,
                len: 5678,
            };
            offsets.push(offset);
        }

        let mut buf = BytesMut::new();
        let len = offsets.encode(&mut buf).unwrap();
        println!("len: {}", len);
        let new_offsets = Vec::<Offset>::decode(buf.freeze()).unwrap();
        println!("offsets: {:?}", new_offsets);
    }
}
