use std::{fs::OpenOptions, io::Read, path::Path};

/// this is a version of read_exact that returns a None if the stream is empty
pub fn optional_read_exact<R: Read + ?Sized>(
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
