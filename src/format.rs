/// Format configuration for SeqData
pub trait SeqDataFormat {
    /// Magic bytes. can be empty
    const MAGIC: &'static [u8];
    /// The size of the header in bytes
    const HEADER_SIZE: usize;
}

pub struct NoMagicNoHeader;

impl SeqDataFormat for NoMagicNoHeader {
    const MAGIC: &'static [u8] = &[];
    const HEADER_SIZE: usize = 0;
}
