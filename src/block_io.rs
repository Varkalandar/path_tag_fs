use std::{fs::File, io::{Error, Read, Seek, Write}, time::{Duration, SystemTime, UNIX_EPOCH}};
use fuser::FileType;

use crate::{nodes::{AnyBlock, DataBlock, DirectoryBlock, DirectoryEntry, EntryBlock, IndexBlock, ENTRY_SIZE}, path_tag_fs::BLOCK_SIZE};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_write_read() {
        let mut bio = BlockIo::new("/tmp/entry_block");
        let b = EntryBlock::new("", 1, FileType::RegularFile, false);
        let ab = AnyBlock::EntryBlock(b);
        
        let result = bio.write_block(&ab, 0);

        assert!(result.is_ok());
        
        if let Result::Ok(size) = result {
            println!("size={}", size);
            assert!(size == BLOCK_SIZE);
            
            let eb1 = EntryBlock::new("", 1, FileType::RegularFile, false);
            // now read it back and compare
            let eb = bio.read_entry_block(0);
            
            assert_eq!(1, eb.attr.ino);
            assert_eq!(eb1.attr.size, eb.attr.size);
            assert_eq!(eb1.attr.blocks, eb.attr.blocks);

// Some nanoseconds difference due to being stored as u64 ...
//            assert_eq!(eb1.attr.atime, eb.attr.atime);
//            assert_eq!(eb1.attr.mtime, eb.attr.mtime);
//            assert_eq!(eb1.attr.ctime, eb.attr.ctime);
//            assert_eq!(eb1.attr.crtime, eb.attr.crtime);
            assert_eq!(eb1.attr.kind, eb.attr.kind);
            assert_eq!(eb1.attr.perm, eb.attr.perm);
            assert_eq!(eb1.attr.nlink, eb.attr.nlink);
            assert_eq!(eb1.attr.uid, eb.attr.uid);
            assert_eq!(eb1.attr.gid, eb.attr.gid);
            assert_eq!(eb1.attr.rdev, eb.attr.rdev);
            assert_eq!(eb1.attr.blksize, eb.attr.blksize);
            assert_eq!(eb1.attr.flags, eb.attr.flags);

        }        
    }
    

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
    fn test_index_write_read() {
        let mut bio = BlockIo::new("/tmp/dump");
        let mut b = IndexBlock::new();
        b.block[0] = 1;
        b.block[127] = 2000000;
        b.next = 2;
        
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
        assert_eq!(ib.next, 2);        
    }
}


fn store_time(time: SystemTime, storage: &mut[u8]) {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => store(n.as_millis() as u64, storage),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}


fn read_time(storage: &[u8]) -> SystemTime {
    let d = Duration::from_millis(to_u64(storage));
    let time_opt = UNIX_EPOCH.checked_add(d);
    
    time_opt.unwrap()
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
        FileType::Directory => 4,
        // Regular file (S_IFREG)
        FileType::RegularFile => 5,
        // Symbolic link (S_IFLNK)
        FileType::Symlink => 6,
        // Unix domain socket (S_IFSOCK)
        FileType::Socket => 7,        
    }
}


fn u8_to_kind(kindval: u8) -> FileType {
    match kindval {
        // Named pipe (S_IFIFO)
        1 => FileType::NamedPipe,
        // Character device (S_IFCHR)
        2 => FileType::CharDevice,
        // Block device (S_IFBLK)
        3 => FileType::BlockDevice,
        // Directory (S_IFDIR)
        4 => FileType::Directory,
        // Regular file (S_IFREG)
        5 => FileType::RegularFile,
        // Symbolic link (S_IFLNK)
        6 => FileType::Symlink,
        // Unix domain socket (S_IFSOCK)
        7 => FileType::Socket,        
        0_u8 | 8_u8..=u8::MAX => todo!(),
    }
}


fn to_u64(data: &[u8]) -> u64 {
    let mut target: [u8; 8] = [0; 8];
    target.copy_from_slice(&data[0..8]);
    
    u64::from_le_bytes(target)
}


fn to_u32(data: &[u8]) -> u32 {
    let mut target: [u8; 4] = [0; 4];
    target.copy_from_slice(&data[0..4]);

    u32::from_le_bytes(target)
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


    pub fn flush(&mut self) {
        self.file.flush().unwrap();
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
        header.write("PTFEntry".as_bytes()).unwrap();
        
        let attrs = &b.attr;
        
        store(attrs.ino, &mut data[8..16]);
        store(attrs.size, &mut data[16..24]);
        store(attrs.blocks, &mut data[24..32]);
        store_time(attrs.atime, &mut data[32..40]);
        store_time(attrs.mtime, &mut data[40..48]);
        store_time(attrs.ctime, &mut data[48..56]);
        store_time(attrs.crtime, &mut data[56..64]);
        store_32(attrs.perm as u32, &mut data[64..68]);
        store_32(attrs.nlink, &mut data[68..72]);
        store_32(attrs.uid, &mut data[72..76]);
        store_32(attrs.gid, &mut data[76..80]);
        store_32(attrs.rdev, &mut data[80..84]);
        store_32(attrs.blksize, &mut data[84..88]);
        store_32(attrs.flags, &mut data[88..92]);

        // single bytes at the end
        data[92] = kind_to_u8(attrs.kind);
        data[93] = if b.is_tag {1} else {0};
        
        store(b.more_data, &mut data[96..104]);
        
        let result = self.file.write(&data);
        println!("write_entry_block()  block={} -> {:?} bytes written", no, result);

        result
    }


    fn write_index_block(&mut self, b: &IndexBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];

        for i in 0..b.block.len() {
            store(b.block[i], &mut data[i*8 .. (i+1)*8]);
        }
        
        let i = b.block.len();
        store(b.next, &mut data[i*8 .. (i+1)*8]);

        let result = self.file.write(&data);
        println!("write_index_block()  block={} -> {:?} bytes written", no, result);

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
        println!("write_directory_block() block={} -> {:?} bytes written", no, result);

        return result;
    }


    pub fn write_data_block(&mut self, b: &DataBlock, no: u64) -> Result<usize, Error> {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let size = self.file.write(&b.data);
        // println!("write_data_block() {:?} bytes written", size);
        return size;
    }

    
    pub fn read_entry_block(&mut self, no: u64) -> EntryBlock {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();
        
        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        let size = self.file.read(&mut data).unwrap();        
        assert!(size == BLOCK_SIZE);
        
        let header = &data[0..8];        
        assert!("PTFEntry".as_bytes() == header);

        // single bytes at the end
        let mut b = EntryBlock::new("", 0, FileType::RegularFile, false);
        let attrs = &mut b.attr;

        attrs.ino = to_u64(&data[8..16]);
        attrs.size = to_u64(&data[16..24]);
        attrs.blocks = to_u64(&data[24..32]);
        attrs.atime = read_time(&data[32..40]);
        attrs.mtime = read_time(&data[40..48]);
        attrs.ctime = read_time(&data[48..56]);
        attrs.crtime = read_time(&data[56..64]);
        attrs.perm = to_u32(&data[64..68]) as u16;
        attrs.nlink = to_u32(&data[68..72]);
        attrs.uid = to_u32(&data[72..76]);
        attrs.gid = to_u32(&data[76..80]);
        attrs.rdev = to_u32(&data[80..84]);
        attrs.blksize = to_u32(&data[84..88]);
        attrs.flags = to_u32(&data[88..92]);
        attrs.kind = u8_to_kind(data[92]);

        b.is_tag = data[93] == 1;
        
        b.more_data = to_u64(&data[96..104]);
        
        b        
    }


    pub fn read_index_block(&mut self, no: u64) -> IndexBlock {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        let ok = self.file.seek(seek);
        
        let mut ib = IndexBlock::new();
        if ok.is_ok() {
            let mut buf = [0u8; 8];

            for i in 0..BLOCK_SIZE/8 - 1 {
                let check = self.file.read(&mut buf);
                if check.is_err() {
                    println!("read_index_block() read failed: {:?}", check);
                }
                
                ib.block[i] = to_u64(&buf);
            }
            
            let _ = self.file.read(&mut buf);
            ib.next = to_u64(&buf);
        }
        return ib;
    }


    pub fn read_directory_block(&mut self, no: u64) -> DirectoryBlock {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let mut db = DirectoryBlock::new();
        let mut data: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        let _ = self.file.read(&mut data);        
        let mut pos = 0;

        let mut ino = 1;
        while ino != 0 {
            
            // scan for string end
            let mut end = pos + 8;
            while data[end] != 0 {
                end += 1;
            }

            let vec = Vec::from(&data[pos+8..end]);

            let entry = DirectoryEntry { 
                ino: to_u64(&data[pos..pos+8]),
                name: String::from_utf8(vec).unwrap(),
            };

            ino = entry.ino;
            if ino > 0 {
                db.entries.push(entry);
            }

            pos += ENTRY_SIZE;
        }

        db.next = to_u64(&data[BLOCK_SIZE-8..BLOCK_SIZE]);

        db
    }


    pub fn read_data_block(&mut self, no: u64) -> DataBlock {
        let seek = std::io::SeekFrom::Start(no  * BLOCK_SIZE as u64);
        self.file.seek(seek).unwrap();

        let mut db = DataBlock::new();
        let _ = self.file.read(&mut db.data);        

        db
    }
}