// use std::io::{BufReader, Read, Seek, Write};
use std::marker::PhantomData;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::format::SeqDataFormat;

/// Writer for a new SeqData
pub struct SeqDataWriter<Format: SeqDataFormat> {
    file: File,
    phantom: PhantomData<Format>,
}

impl<Format: SeqDataFormat> SeqDataWriter<Format> {
    /// Create a new SeqData File at the location specified
    ///
    /// If the file already exists, this call will fail
    ///
    /// The header need to fits the size of Format::HEADER_SIZE
    pub async fn create<P: AsRef<Path>>(path: P, header: &[u8]) -> std::io::Result<Self> {
        if Format::HEADER_SIZE != header.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "header has invalid size, expecting {} but got {}",
                    Format::HEADER_SIZE,
                    header.len()
                ),
            ));
        }

        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(&Format::MAGIC).await?;
        file.write_all(header).await?;
        Ok(SeqDataWriter {
            file,
            phantom: PhantomData,
        })
    }

    /// Open a SeqData File at the location specified
    ///
    /// If the file already exists, this call will fail
    ///
    /// The header need to fits the size of Format::HEADER_SIZE
    pub async fn open<P: AsRef<Path>>(path: P, header: &[u8]) -> std::io::Result<(Self, Vec<u8>)> {
        if Format::HEADER_SIZE != header.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "header has invalid size, expecting {} but got {}",
                    Format::HEADER_SIZE,
                    header.len()
                ),
            ));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(false)
            .append(true)
            .open(path)
            .await?;

        file.seek(std::io::SeekFrom::Start(0)).await?;
        let header = read_magic_and_header(PhantomData::<Format>, &mut file).await?;
        file.seek(std::io::SeekFrom::End(0)).await?;

        Ok((
            SeqDataWriter {
                file,
                phantom: PhantomData,
            },
            header,
        ))
    }

    /// Append a new data chunk to this file
    pub async fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
        write_chunk(&mut self.file, data).await
    }
}

/// Reader for SeqData
pub struct SeqDataReader<Format: SeqDataFormat> {
    buf_reader: tokio::io::BufReader<File>,
    pos: u64,
    len: u64,
    phantom: PhantomData<Format>,
}

async fn read_magic_and_header<Format: SeqDataFormat>(
    _format: PhantomData<Format>,
    file: &mut File,
) -> std::io::Result<Vec<u8>> {
    // try to read the magic
    const MAGIC_READ_BUF_SIZE: usize = 16;
    let mut magic_read_buf = [0u8; MAGIC_READ_BUF_SIZE];
    let mut magic_slice = Format::MAGIC;
    while !magic_slice.is_empty() {
        let sz = Format::MAGIC.len().min(MAGIC_READ_BUF_SIZE);
        let rd = file.read(&mut magic_read_buf[0..sz]).await?;
        if rd == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF in magic reading",
            ));
        }
        if magic_slice[0..rd] != magic_read_buf[0..rd] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "magic do not match expected value",
            ));
        }
        magic_slice = &magic_slice[rd..];
    }

    let mut header = vec![0u8; Format::HEADER_SIZE];
    file.read_exact(&mut header).await?;
    Ok(header)
}

impl<Format: SeqDataFormat> SeqDataReader<Format> {
    /// Open a SeqData for reading
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<(Self, Vec<u8>)> {
        let mut file = File::open(path).await?;

        let phantom = PhantomData;
        let len = get_file_length(phantom, &mut file).await?;
        let header = read_magic_and_header(phantom, &mut file).await?;

        let buf_reader = tokio::io::BufReader::with_capacity(1024 * 1024, file);
        Ok((
            SeqDataReader {
                buf_reader,
                pos: 0,
                len,
                phantom,
            },
            header,
        ))
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Return the next block along with the current offset if it exists, or None if
    /// reached the end of file.
    pub async fn next(&mut self) -> Option<std::io::Result<(u64, Vec<u8>)>> {
        match read_chunk(&mut self.buf_reader).await {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(buf)) => {
                let current_pos = self.pos;
                self.pos += size_of::<PrefixLength>() as u64 + buf.len() as u64;
                Some(Ok((current_pos, buf)))
            }
        }
    }
}

/// Seq Data Reader with seek
pub struct SeqDataReaderSeek<Format: SeqDataFormat> {
    handle: File,
    phantom: PhantomData<Format>,
    start: u64,
    len: u64,
}

impl<Format: SeqDataFormat> SeqDataReaderSeek<Format> {
    /// Open a new Seq Data seeker
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<(Self, Vec<u8>)> {
        let mut handle = File::open(path).await?;

        let phantom = PhantomData;
        let len = get_file_length(phantom, &mut handle).await?;
        let header = read_magic_and_header(phantom, &mut handle).await?;

        let start = handle.seek(std::io::SeekFrom::Current(0)).await?;

        Ok((
            Self {
                handle,
                phantom,
                len,
                start,
            },
            header,
        ))
    }

    /// Return the next block along with the current offset if it exists, or None if
    /// reached the end of file.
    pub async fn next(&mut self) -> std::io::Result<Vec<u8>> {
        read_chunk(&mut self.handle).await.unwrap()
    }

    /// Return the next block at the offset specified
    ///
    /// Note that if the position specified is not a valid boundary,
    /// then arbitrary invalid stuff might be returns, or some Err
    /// related to reading data
    pub async fn next_at(&mut self, pos: u64) -> std::io::Result<Vec<u8>> {
        if pos >= self.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "trying to access data at {} but data length {}",
                    pos, self.len
                ),
            ));
        }

        let seek = self.start + pos;
        self.handle.seek(std::io::SeekFrom::Start(seek)).await?;
        self.next().await
    }
}

type PrefixLength = u32;

async fn read_chunk<R: AsyncRead + std::marker::Unpin>(
    file: &mut R,
) -> Option<std::io::Result<Vec<u8>>> {
    let mut lenbuf = [0; size_of::<PrefixLength>()];
    // try to read the length, if the length return a none, we just expect
    // having reached the end of the stream then
    match optional_read_exact(file, &mut lenbuf).await {
        None => None,
        Some(Err(e)) => Some(Err(e)),
        Some(Ok(())) => {
            let len = PrefixLength::from_le_bytes(lenbuf);

            // create a buffer of the prefix length 'len' and read all data
            let mut out = vec![0; len as usize];
            match file.read_exact(&mut out).await {
                Err(e) => Some(Err(e)),
                Ok(_sz) => Some(Ok(out)),
            }
        }
    }
}

async fn write_chunk(file: &mut File, data: &[u8]) -> std::io::Result<()> {
    let max = PrefixLength::MAX as usize;
    assert!(data.len() <= max);
    let len: u32 = data.len() as PrefixLength;
    let header = len.to_le_bytes();
    file.write_all(&header).await?;
    file.write_all(data).await?;
    Ok(())
}

async fn get_file_length<Format: SeqDataFormat>(
    _phantom: PhantomData<Format>,
    file: &mut File,
) -> std::io::Result<u64> {
    let meta = file.metadata().await?;
    let total_len = meta.len();

    let minimum_size = Format::MAGIC.len() as u64 + Format::HEADER_SIZE as u64;
    if total_len < minimum_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "file not contains enough bytes for magic and header",
        ));
    }
    Ok(total_len - minimum_size)
}

/// this is a version of read_exact that returns a None if the stream is empty
pub async fn optional_read_exact<R: AsyncRead + ?Sized + std::marker::Unpin>(
    this: &mut R,
    mut buf: &mut [u8],
) -> Option<std::io::Result<()>> {
    let mut read_bytes = 0;
    while !buf.is_empty() {
        match this.read(buf).await {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                read_bytes += n;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Some(Err(e)),
        }
    }
    if read_bytes == 0 {
        None
    } else if !buf.is_empty() {
        Some(Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "buffer partially filled",
        )))
    } else {
        Some(Ok(()))
    }
}
