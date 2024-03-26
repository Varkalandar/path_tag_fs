use std::{fs::File, io::{Error, ErrorKind, Read, Seek, Write}, time::SystemTime};
use fuser::FileType;

use crate::{block_storage::BLOCK_SIZE, nodes::{AnyBlock, DataBlock, DirectoryBlock, EntryBlock, IndexBlock, ENTRY_SIZE}};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_write() {
        let mut bio = BlockIo::new("/tmp/dump");
        let b = DataBlock::new();
        let ab = AnyBlock::DataBlock(b);
        
        let result = bio.write_block(&ab, 0);

        assert!(result.is_ok());
        
        if let Result::Ok(size) = result {
            println!("size={}", size);
            assert!(size == BLOCK_SIZE);            
        }        
    }

    #[test]
    fn test_index_write() {
        let mut bio = BlockIo::new("/tmp/dump");
        let mut b = IndexBlock::new();
        b.block[0] = 1;
        b.block[127] = 2000000;
        
        
        let ab = AnyBlock::IndexBlock(b);        
        let result = bio.write_block(&ab, 0);

        println!("result={:?}", result);
        assert!(result.is_ok());
        
        if let Result::Ok(size) = result {
            println!("size={}", size);
            assert!(size == BLOCK_SIZE);            
        }
        
        let ib = bio.read_index_block(0);
        
        assert_eq!(ib.block[0], 1);        
        assert_eq!(ib.block[1], 0);        
        assert_eq!(ib.block[126], 0);        
        assert_eq!(ib.block[127], 2000000);        
    }
}


fn store_time(time: SystemTime, storage: &mut[u8]) {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => store(n.as_millis() as u64, storage),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}


fn store_32(value: u32, storage: &mut[u8]) {
    let bytes = u32::to_le_bytes(value);

    storage[0] = bytes[0];
    storage[1] = bytes[1];
    storage[2] = bytes[2];
    storage[3] = bytes[3];
}


fn store(value: u64, storage: &mut[u8]) {
    let bytes = u64::to_le_bytes(value);

    storage[0] = bytes[0];
    storage[1] = bytes[1];
    storage[2] = bytes[2];
    storage[3] = bytes[3];
    storage[4] = bytes[4];
    storage[5] = bytes[5];
    storage[6] = bytes[6];
    storage[7] = bytes[7];
}


fn kind_to_u8(kind: FileType) -> u8 {
    match kind {
        // Named pipe (S_IFIFO)
        FileType::NamedPipe => 1,
        // Character device (S_IFCHR)
        FileType::CharDevice => 2,
        // Block device (S_IFBLK)
        FileType::BlockDevice => 3,
        // Directory (S_IFDIR)
        FileType::Directory=> 4,
        // Regular file (S_IFREG)
        FileType::RegularFile => 5,
        // Symbolic link (S_IFLNK)
        FileType::Symlink => 6,
        // Unix domain socket (S_IFSOCK)
        FileType::Socket => 7,        
    }
}


fn to_u64(data: [u8;8]) -> u64 {
    u64::from_le_bytes(data)
}

pub struct BlockIo {
    file: File,
}

impl BlockIo {

    pub fn new(path: &str) -> BlockIo {
        let file = File::options().read(true).write(true).create(true).open(path);

        BlockIo {
            file: file.unwrap(),
        }
    }

    
    pub fn write_block(&mut self, ab: &AnyBlock, no: u64) -> Result<usize, Error> {
        let size;
        
        match ab {
            AnyBlock::EntryBlock(b) => {
                size = self.write_entry_block(b, no);
            }
            AnyBlock::IndexBlock(b) => {
                size = self.write_index_block(b, no);
            }
            AnyBlock::DirectoryBlock(b) => {
                size = self.write_directory_block(b, no);
            }
            AnyBlock::DataBlock(b) => {
                size = self.write_data_block(b, no);
            }
        }
        return size;
    }
    
    
    fn write_entry_block(&mut self, b: &EntryBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();
        
        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        let mut header = &mut data[0..8];        
        header.write("PTFEntry".as_bytes());
        
        let attrs = &b.attr;
        
        store(attrs.ino, &mut data[8..16]);
        store(attrs.size, &mut data[8..16]);
        store(attrs.blocks, &mut data[16..24]);
        store_time(attrs.atime, &mut data[24..32]);
        store_time(attrs.mtime, &mut data[32..40]);
        store_time(attrs.ctime, &mut data[40..48]);
        store_time(attrs.crtime, &mut data[48..56]);
        store_32(attrs.perm as u32, &mut data[56..60]);
        store_32(attrs.nlink, &mut data[60..64]);
        store_32(attrs.uid, &mut data[64..68]);
        store_32(attrs.gid, &mut data[68..72]);
        store_32(attrs.rdev, &mut data[72..76]);
        store_32(attrs.blksize, &mut data[76..80]);
        store_32(attrs.flags, &mut data[80..84]);

        // single bytes at the end
        data[84] = kind_to_u8(attrs.kind);
        data[85] = if b.is_tag {1} else {0};
        
        let result = self.file.write(&data);
        result
    }


    fn write_index_block(&mut self, b: &IndexBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];

        for i in 0..BLOCK_SIZE/8 {
            store(b.block[i], &mut data[i*8 .. (i+1)*8]);
        }

        let result = self.file.write(&data);
        println!("write_data_block() {:?} bytes written", result);

        return result;
    }


    fn write_directory_block(&mut self, b: &DirectoryBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        let mut pos = 0;

        for entry in &b.entries {

            store(entry.ino, &mut data[pos..pos+8]);
    
            let utf8 = entry.name.as_bytes();
            for i in 0..utf8.len() {
                data[pos+8+i] = utf8[i];
            }
            
            pos += ENTRY_SIZE;
        }

        store(b.next, &mut data[BLOCK_SIZE-8..BLOCK_SIZE]);

        let result = self.file.write(&data);
        println!("write_directory_block() {:?} bytes written", result);

        return result;
    }


    fn write_data_block(&mut self, b: &DataBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let size = self.file.write(&b.data);
        println!("write_data_block() {:?} bytes written", size);
        return size;
    }

    
    fn read_index_block(&mut self, no: u64) -> IndexBlock {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        let ok = self.file.seek(seek);
        
        let mut ib = IndexBlock::new();
        if ok.is_ok() {
            let mut buf = [0u8; 8];

            for i in 0..BLOCK_SIZE/8 {
                let check = self.file.read(&mut buf);
                if check.is_err() {
                    println!("read_index_block() read failed: {:?}", check);
                }
                
                ib.block[i] = to_u64(buf);
            }
            
        }
        return ib;
    }

}