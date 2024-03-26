use std::collections::HashMap;
use fuser::{FileAttr, FileType};

use crate::nodes::{AnyBlock, DataBlock, DirectoryBlock, DirectoryEntry, EntryBlock, IndexBlock, MAX_ENTRIES};
use crate::block_cache::BlockCache;


/*
// debug help
fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}    

fn debug_any_block(ab: &AnyBlock) {
    match ab {
        EntryBlock => {
            println!("Block is an EntryBlock");    
        }
        DirectoryBlock => {
            println!("Block is a DirectoryBlock");    
        }
        IndexBlock => {
            println!("Block is an IndexBlock");    
        }
        DataBlock => {
            println!("Block is a DataBlock");    
        }
    }
}
*/

pub const BLOCK_SIZE:usize = 2048;

pub struct BlockStorage {
    cache: BlockCache,
}


impl BlockStorage {
    
    pub fn new() -> BlockStorage {
        BlockStorage {
            cache: BlockCache::new(),
        }
    }
    
    
    pub fn initialize(& mut self, ino_root: u64) {
        
        // take special blocks
        self.cache.take_block(0);
        self.cache.take_block(1);
        
        let root = EntryBlock::new( "Root".to_string(), ino_root, FileType::Directory, false);

        self.cache.write_block(AnyBlock::EntryBlock(root), ino_root);

        self.mkdir(ino_root, &"Pathes".to_string());
        self.mkdir(ino_root, &"Tags".to_string());
    }


    pub fn retrieve_entry_block(&mut self, bno: u64) -> Option<&mut EntryBlock> {
        self.cache.retrieve_entry_block(bno)
    }
    
    
    pub fn find_child(&mut self, parent_ino: u64, name: &String) -> Option<u64> {

        println!("find_child() finding {} from indode {}", name, parent_ino);                

        let eb_opt = self.cache.retrieve_entry_block(parent_ino);

        match eb_opt {
            None => {
                println!("  error:  {} is no entry block", parent_ino);                
            }
            Some(eb) => {
                let mut next = eb.more_data;

                println!("  next directory block is {}", next);                

                while next != 0 {
                    let option = self.cache.retrieve_directory_block(next);
                    let db = option.unwrap();
                
                    for entry in &db.entries {
                        if *name == entry.name {
                            return Option::Some(entry.ino);   
                        }
                    }
                                    
                    next = db.next;
                }
            }
        }

        return None;        
    }


    pub fn list_children_names(&mut self, parent_ino: u64) -> Vec<(u64, String)> {
        let mut result = Vec::new();

        println!("list_children() listing from inode {}", parent_ino);                

        let eb_opt = self.cache.retrieve_entry_block(parent_ino);

        match eb_opt {
            None => {
                println!("  error:  {} is no entry block", parent_ino);                
            }
            Some(eb) => {
                let mut next = eb.more_data;

                println!("  next directory block is {}", next);                

                while next != 0 {
                    let option = self.cache.retrieve_directory_block(next);
                    
                    match option {
                        None => {
                            println!("  error:  {} is no directory block", next);                
                        }
                        Some(db) => {
                            for entry in &db.entries {
                                let name = entry.name.to_string();
                                let ino = entry.ino;
                                result.push((ino, name));                
                                next = db.next;
                            }
                        }
                    }
                }
            }
        }

        return result;
    }


    fn find_filetype(&mut self, ino: u64) -> Option<FileType> {
        println!("find_filetype()  finding type of inode {}", ino);                

        let inode = self.cache.retrieve_entry_block(ino);
        match inode {
            None => {
                println!("  error:  {} is no entry block", ino);                
            }
            Some(entry) => {
                return Some(entry.attr.kind);
            }
        }
        
        None 
    }


    pub fn list_children(&mut self, parent_ino: u64) -> Vec<(u64, fuser::FileType, String)> {
        let names = self.list_children_names(parent_ino);
        let mut result = Vec::new();

        for (ino, name) in names {
            let kind_opt = self.find_filetype(ino);
            result.push((ino, kind_opt.unwrap(), name));
        }

        result    
    }

    
    pub fn read(&mut self, index_block: u64, offset: i64, size: u64) -> Vec<u8> {
        println!("read() reading data");
        let mut result = Vec::new();

        if offset < 0 {
            println!("  error: data offset is negative, cannot read there.");
            return result;
        }

        let mut list = Vec::new();
        let mut ib_no = index_block;
        
        while ib_no != 0 {
            let ib_opt = self.cache.retrieve_index_block(ib_no);
            
            match ib_opt {
                None => {
                    println!("  error: Block {} is not an index block.", ib_no);
                    ib_no = 0;
                }
                Some(ib) => {
                    if ib.block[0] != 0 {
                        
                        let start = offset as usize / BLOCK_SIZE as usize;
                        let end = (offset + size as i64) as usize / BLOCK_SIZE as usize;    
        
                        for n in start..=end {
                            let dbno = ib.block[n];
                            list.push(dbno);
                        }
                    }
                    else {
                        println!("  error: No data blocks for file.");                
                    }
                    ib_no = ib.block[ib.block.len() - 1];
                }
            }
        }

        for bno in list {
            println!("  reading data block {}.", bno);                

            let db_opt = self.cache.retrieve_data_block(bno);
            match db_opt {
                None => {
                    println!("  error: block {} is no data block.", bno);                
                }
                Some(db) => {
                    println!("  copy data");                
                    result.extend_from_slice(&db.data);
                }
            }
        }
            
        return result;
    }


    pub fn write(&mut self, inode: u64, offset: i64, data: &[u8]) {

        if offset < 0 {
            println!("  data offset is negative, cannot write there.");
        }

        let list = self.write_data_blocks(offset as usize, data);

        let ib_no = self.cache.allocate_block() as u64;
        let mut ib = IndexBlock::new();            

        for i in 0..list.len() {
            ib.block[i] = list[i];            
        }

        self.cache.write_block(AnyBlock::IndexBlock(ib), ib_no);
        
        let eb_opt = self.cache.retrieve_entry_block(inode);
        let eb = eb_opt.unwrap();
        
        eb.more_data = ib_no;
        eb.attr.size = data.len() as u64;
    }

    
    fn write_data_blocks(&mut self, offset: usize, data: &[u8]) -> Vec<u64> {
        let mut result = Vec::new();

        let start = offset / BLOCK_SIZE as usize;
        let end = (offset + data.len()) / BLOCK_SIZE as usize;    

        for n in start..=end {

            let data_start = (n - start) * BLOCK_SIZE as usize;

            let db_no = self.cache.allocate_block() as u64;
            let mut db = DataBlock::new();

            let data_size = std::cmp::min(BLOCK_SIZE as usize, data.len() - data_start);

            println!("  writing {} bytes to data block {} chain={}", data_size, db_no, n);
            
            // db.data.copy_from_slice(src)
            db.data[0..data_size].copy_from_slice(&data[data_start..data_start+data_size]);
            result.push(db_no);
            self.cache.write_block(AnyBlock::DataBlock(db), db_no);
        }        
        
        result
    }


    pub fn mknod(&mut self, parent_ino: u64, name: &String, kind: FileType) -> Option<FileAttr> {
        println!("mknod() parent={} name={} kind={:?}", parent_ino, name, kind);

        let parent_opt = self.cache.retrieve_entry_block(parent_ino);

        match parent_opt {
            None => {
                println!("  error: {} is no allocated block.", parent_ino);
            }
            Some(parent) => {
                let bno = self.cache.allocate_block() as u64;
                self.add_directory_entry(parent_ino, &name.to_string(), bno);
                
                let entry = EntryBlock::new(name.to_string(), bno, kind, false);
                let attr: FileAttr = entry.attr.into();
                self.cache.write_block(AnyBlock::EntryBlock(entry), bno);
                
                return Some(attr);
            }
        }
        
        return None;
    }


    pub fn mkdir(&mut self, parent_ino: u64, name: &String) -> Option<FileAttr> {
        println!("mkdir() parent={} name={}", parent_ino, name);

        let parent_opt = self.cache.retrieve_entry_block(parent_ino);

        match parent_opt {
            None => {
                println!("  error: {} is no allocated block.", parent_ino);
            }
            Some(parent) => {
                let bno = self.cache.allocate_block() as u64;
                self.add_directory_entry(parent_ino, &name.to_string(), bno);
                
                let entry = EntryBlock::new(name.to_string(), bno, fuser::FileType::Directory, false);
                let attr: FileAttr = entry.attr.into();
                self.cache.write_block(AnyBlock::EntryBlock(entry), bno);
                
                self.add_directory_entry(bno, &".".to_string(), bno);            
                self.add_directory_entry(bno, &"..".to_string(), parent_ino);            
                
                return Some(attr);
            }
        }
        
        return None;
    }
    
    
    fn extend_directory_chain(&mut self, tail: u64, name: &String, ino: u64) -> u64 {

        println!("extend_directory_chain()  Adding new directory node to chain tail {} for name {} (inode {})", tail, name, ino);

        let bno = self.cache.allocate_block() as u64;
        let mut db = DirectoryBlock::new();
        db.entries.push(DirectoryEntry{ino: ino, name: name.to_string(),});
        
        let ab = AnyBlock::DirectoryBlock(db);
        self.cache.write_block(ab, bno);

        // tail can either be an entry block or an directory block
        // directory block is more common so we check that first
        let dir_opt = self.cache.retrieve_directory_block(tail);
        match dir_opt {
            None => {
                // ok, this should be an entry node then ...
                
                let entry_opt = self.cache.retrieve_entry_block(tail);
                let entry = entry_opt.unwrap();

                // add new block here  
                entry.more_data = bno;
            }
            Some(dir) => {
                // just add new block here  
                dir.next = bno;
            }
        }
        
        bno
    }

    
    pub fn store_directory_entry(&mut self, parent_ino: u64, name: &String, ino: u64) -> u64 {

        println!("store_directory_entry()  Trying to store new directory entry {} in inode {} from parent inode {}", name, ino, parent_ino);
        let mut result = 0;
        let parent_opt = self.cache.retrieve_entry_block(parent_ino);

        match parent_opt {
            None => {
                println!("  error: block {} is no entry block", parent_ino);
            }
            Some(parent) => {
                if parent.more_data == 0 {
                    println!("  no directory blocks for inode {}", parent_ino);
                    result = parent_ino;
                }
                else {
                    // traverse the chain
                    let mut next = parent.more_data;
                    while next != 0 {
                        let option = self.cache.retrieve_directory_block(next);
                        let db = option.unwrap();
  
                        result = next;
  
                        //  check if there are free entries
                        if db.entries.len() < MAX_ENTRIES {
                            println!("  storing entry in block {}", result);
                            db.entries.push(DirectoryEntry{ino: ino, name: name.to_string(),});
                            result = 0;
                            next = 0;
                        } else {
                            // blocks to check
                            next = db.next;
                        }  
                    }
                }
            }
        }
        
        result
    }    


    pub fn add_directory_entry(&mut self, parent_ino: u64, name: &String, ino: u64) {
        println!("store_directory_entry()  Add new directory entry {} in inode {} from parent inode {}", name, ino, parent_ino);
        
        // try to store the new entry in one of the existing directrory blocks of this inode 
        let tail = self.store_directory_entry(parent_ino, name, ino);
        
        if tail != 0 {
            // there were no free entries, but we got the tail of the chain
            self.extend_directory_chain(tail, name, ino);
        }
    }
}