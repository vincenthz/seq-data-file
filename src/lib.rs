//! Seq Data is a simple file format that contains multiple chunks of data prefixed by a length
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, Write};
use std::marker::PhantomData;
use std::path::Path;

/// Format configuration for SeqData
pub trait SeqDataFormat {
    /// Magic bytes. can be empty
    const MAGIC: &'static [u8];
    /// The size of the header in bytes
    const HEADER_SIZE: usize;
}

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
    pub fn create<P: AsRef<Path>>(path: P, header: &[u8]) -> std::io::Result<Self> {
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
            .open(path)?;
        file.write_all(&Format::MAGIC)?;
        file.write_all(header)?;
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
    pub fn open<P: AsRef<Path>>(path: P, header: &[u8]) -> std::io::Result<(Self, Vec<u8>)> {
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
            .open(path)?;

        file.seek(std::io::SeekFrom::Start(0))?;
        let header = read_magic_and_header(PhantomData::<Format>, &mut file)?;
        file.seek(std::io::SeekFrom::End(0))?;

        Ok((
            SeqDataWriter {
                file,
                phantom: PhantomData,
            },
            header,
        ))
    }

    /// Append a new data chunk to this file
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
        assert!(data.len() <= 0xffff_ffff);
        let len: u32 = data.len() as u32;
        let header = len.to_le_bytes();
        self.file.write_all(&header)?;
        self.file.write_all(data)?;
        Ok(())
    }
}

/// Reader for SeqData
pub struct SeqDataReader<Format: SeqDataFormat> {
    buf_reader: BufReader<File>,
    pos: u64,
    len: u64,
    phantom: PhantomData<Format>,
}

/// this is a version of read_exact that returns a None if the stream is empty
fn optional_read_exact<R: Read + ?Sized>(
    this: &mut R,
    mut buf: &mut [u8],
) -> Option<std::io::Result<()>> {
    let mut read_bytes = 0;
    while !buf.is_empty() {
        match this.read(buf) {
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

fn read_magic_and_header<Format: SeqDataFormat>(
    _format: PhantomData<Format>,
    file: &mut File,
) -> std::io::Result<Vec<u8>> {
    // try to read the magic
    const MAGIC_READ_BUF_SIZE: usize = 16;
    let mut magic_read_buf = [0u8; MAGIC_READ_BUF_SIZE];
    let mut magic_slice = Format::MAGIC;
    while !magic_slice.is_empty() {
        let sz = Format::MAGIC.len().min(MAGIC_READ_BUF_SIZE);
        let rd = file.read(&mut magic_read_buf[0..sz])?;
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
    file.read_exact(&mut header)?;
    Ok(header)
}

fn get_file_length<Format: SeqDataFormat>(
    _phantom: PhantomData<Format>,
    file: &mut File,
) -> std::io::Result<u64> {
    let meta = file.metadata()?;
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

impl<Format: SeqDataFormat> SeqDataReader<Format> {
    /// Open a SeqData for reading
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<(Self, Vec<u8>)> {
        let mut file = File::open(path)?;

        let phantom = PhantomData;
        let len = get_file_length(phantom, &mut file)?;
        let header = read_magic_and_header(phantom, &mut file)?;

        let buf_reader = BufReader::with_capacity(1024 * 1024, file);
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
    pub fn next(&mut self) -> Option<std::io::Result<(u64, Vec<u8>)>> {
        let mut lenbuf = [0; 4];
        // try to read the length, if the length return a none, we just expect
        // having reached the end of the stream then
        match optional_read_exact(&mut self.buf_reader, &mut lenbuf) {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(())) => {
                let len = u32::from_le_bytes(lenbuf);
                let mut out = vec![0; len as usize];
                match self.buf_reader.read_exact(&mut out) {
                    Err(e) => Some(Err(e)),
                    Ok(()) => {
                        let old_pos = self.pos;
                        self.pos += 4 + len as u64;
                        Some(Ok((old_pos, out)))
                    }
                }
            }
        }
    }
}

pub fn truncate_at(path: &Path, len: u64) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(false)
        .append(false)
        .open(path)?;
    file.set_len(len)?;
    Ok(())
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
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<(Self, Vec<u8>)> {
        let mut handle = File::open(path)?;

        let phantom = PhantomData;
        let len = get_file_length(phantom, &mut handle)?;
        let header = read_magic_and_header(phantom, &mut handle)?;

        let start = handle.seek(std::io::SeekFrom::Current(0))?;

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
    pub fn next(&mut self) -> std::io::Result<Vec<u8>> {
        let mut lenbuf = [0; 4];
        // try to read the length, if the length return a none, we just expect
        // having reached the end of the stream then
        match self.handle.read_exact(&mut lenbuf) {
            Err(e) => Err(e),
            Ok(()) => {
                let len = u32::from_le_bytes(lenbuf);
                let mut out = vec![0; len as usize];
                match self.handle.read_exact(&mut out) {
                    Err(e) => Err(e),
                    Ok(()) => Ok(out),
                }
            }
        }
    }

    /// Return the next block at the offset specified
    ///
    /// Note that if the position specified is not a valid boundary,
    /// then arbitrary invalid stuff might be returns, or some Err
    /// related to reading data
    pub fn next_at(&mut self, pos: u64) -> std::io::Result<Vec<u8>> {
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
        self.handle.seek(std::io::SeekFrom::Start(seek))?;
        self.next()
    }
}
