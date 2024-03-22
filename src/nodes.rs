use fuser::{FileAttr, FileType};
use crate::block_storage::BLOCK_SIZE;


pub struct EntryBlock {
    pub name: String,
    pub is_tag: bool,
    pub attr: FileAttr,
    
    // - if this is a file, more_data will point to an IndexNode
    // - if this is a directory, more_data will point to an DirectoryNode
    pub more_data: u64,
}

impl EntryBlock {
    pub fn new(name: String, ino: u64, kind: FileType, is_tag: bool) -> EntryBlock {

        let node = EntryBlock { 
            name: name,
            is_tag: is_tag,
            attr: make_attr(ino, kind),
            more_data: 0, 
        };
        
        return node;        
    }
}


pub struct IndexBlock {
    pub block: [u64; (BLOCK_SIZE/8) as usize],
}

impl IndexBlock {

    pub fn new() -> IndexBlock {
        IndexBlock { 
            block: [0; (BLOCK_SIZE/8)], 
        }
    }
}

pub struct DirectoryBlock {
    pub entry_name: String,
    pub entry_ino: u64,
    pub next: u64,
}

impl DirectoryBlock {

    pub fn new() -> DirectoryBlock {
        DirectoryBlock { 
            entry_name: "".to_string(), 
            entry_ino: 0, 
            next: 0 
        }
    }
}


pub struct DataBlock {
    pub data: [u8; BLOCK_SIZE],
}

impl DataBlock {
    pub fn new() -> DataBlock {
        DataBlock {
            data: [0; BLOCK_SIZE],
        }
    }
}

pub enum AnyBlock {
    EntryBlock(EntryBlock),
    IndexBlock(IndexBlock),
    DirectoryBlock(DirectoryBlock),
    DataBlock(DataBlock),
}


fn make_attr(ino: u64, kind: FileType) -> FileAttr
{
    let perm = if kind == FileType::Directory {0o755} else {0o644};
    let now = std::time::SystemTime::now();

    FileAttr {
        ino: ino,
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: kind,
        perm: perm,
        nlink: 2,
        uid: 501,
        gid: 100,
        rdev: 0,
        flags: 0,
        blksize: BLOCK_SIZE as u32,
    }
}
