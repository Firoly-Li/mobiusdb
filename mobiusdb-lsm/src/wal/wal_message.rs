use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::serialization::{Decoder, Encoder};

pub trait IntoWalMessage {
    fn into_wal_message(&self) -> WalMessage;
}

#[derive(Debug, Clone)]
pub struct WalMessage {
    data_len: u32,
    bytes: Bytes,
}

impl WalMessage {
    pub fn get_len(&self) -> u32 {
        self.data_len + 4
    }

    pub fn get_bytes_len(&self) -> u32 {
        self.data_len
    }

    pub fn get_bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl Encoder for WalMessage {
    type Error = anyhow::Error;

    fn encode(&self, buffer: &mut BytesMut) -> anyhow::Result<usize, Self::Error> {
        buffer.put_u32(self.get_len());
        buffer.put(self.bytes.as_ref());
        Ok(self.get_len() as usize)
    }
}

impl Decoder for WalMessage {
    type Error = anyhow::Error;

    fn decode(mut bytes: Bytes) -> anyhow::Result<Self, Self::Error> {
        Ok(Self {
            data_len: bytes.get_u32(),
            bytes: bytes,
        })
    }
}

/**
 * 适配其他数据类型
 */
impl<T: AsRef<[u8]>> From<T> for WalMessage {
    fn from(bytes: T) -> Self {
        Self {
            data_len: bytes.as_ref().len() as u32,
            bytes: bytes.as_ref().to_vec().into(),
        }
    }
}

impl<T: ::prost::Message> IntoWalMessage for Vec<T> {
    fn into_wal_message(&self) -> WalMessage {
        let mut wal_message_bytes = BytesMut::new();
        for fd in self {
            let mut bytes_mut = BytesMut::new();
            let _ = fd.encode(&mut bytes_mut).unwrap();
            wal_message_bytes.put_u32(bytes_mut.len() as u32);
            wal_message_bytes.put(bytes_mut);
        }
        WalMessage::from(wal_message_bytes)
    }
}
