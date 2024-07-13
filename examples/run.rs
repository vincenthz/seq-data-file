use std::path::{Path, PathBuf};

use seq_data_file::{SeqDataFormat, SeqDataReader, SeqDataReaderSeek, SeqDataWriter};

pub struct H;
pub struct H2;

impl SeqDataFormat for H {
    const MAGIC: &'static [u8] = &[];
    const HEADER_SIZE: usize = 0;
}

impl SeqDataFormat for H2 {
    const MAGIC: &'static [u8] = &[0xde, 0xad, 0xbe, 0xef];
    const HEADER_SIZE: usize = 10;
}

const DATA1: &[u8] = &[1, 2, 3, 4, 5, 6, 7];
const DATA2: &[u8] = &[125, 33, 6, 35, 6, 235, 46, 43, 25, 37];
const DATA3: &[u8] = &[
    10, 20, 30, 40, 50, 60, 70, 80, 90, 10, 20, 30, 40, 50, 60, 70, 80, 90, 10, 20, 30, 40, 50, 60,
    70, 80, 90,
];

fn main() {
    let sdf_file = PathBuf::from("a.sdf");

    if sdf_file.exists() {
        panic!("file {} already exists", &sdf_file.display())
    }

    run_writer_reader::<H>(&sdf_file);
    std::fs::remove_file(&sdf_file).unwrap();
    run_writer_reader::<H2>(&sdf_file);
    std::fs::remove_file(&sdf_file).unwrap();
}

fn run_writer_reader<H: SeqDataFormat>(sdf_file: &Path) {
    {
        let header = vec![0x90; H::HEADER_SIZE];
        let mut sdf = SeqDataWriter::<H>::create(&sdf_file, &header).unwrap();
        sdf.append(DATA1).unwrap();
        sdf.append(DATA2).unwrap();
        sdf.append(DATA3).unwrap();
    }

    let mut pos = Vec::new();
    {
        let (mut sdf, _header) = SeqDataReader::<H>::open(&sdf_file).unwrap();
        let (p1, r1) = sdf.next().unwrap().unwrap();
        let (p2, r2) = sdf.next().unwrap().unwrap();
        let (p3, r3) = sdf.next().unwrap().unwrap();

        pos.push(p1);
        pos.push(p2);
        pos.push(p3);

        assert_eq!(p1, 0);
        assert_eq!(r1, DATA1);

        assert_eq!(p2, 4 + DATA1.len() as u64);
        assert_eq!(r2, DATA2);

        assert_eq!(p3, 4 * 2 + DATA1.len() as u64 + DATA2.len() as u64);
        assert_eq!(r3, DATA3);

        if let Some(x) = sdf.next() {
            panic!("more data than expected {:?}", x)
        }
    }

    {
        let (mut sdf, _header) = SeqDataReaderSeek::<H>::open(sdf_file).unwrap();
        let r2 = sdf.next_at(pos[1]).unwrap();
        assert_eq!(r2, DATA2);
        let r1 = sdf.next_at(pos[0]).unwrap();
        assert_eq!(r1, DATA1);
        let r3 = sdf.next_at(pos[2]).unwrap();
        assert_eq!(r3, DATA3);
    }
}
