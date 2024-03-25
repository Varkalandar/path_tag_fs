use std::collections::HashMap;
use fuser::{FileAttr, FileType};

use crate::nodes::{AnyBlock, DataBlock, DirectoryBlock, DirectoryEntry, EntryBlock, IndexBlock, MAX_ENTRIES};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_set() {
        let mut storage = BlockStorage::new();
        storage.take_block(0);
        storage.take_block(7);
        storage.take_block(8);
        storage.take_block(17);

        // second block
        storage.take_block(8192);
        storage.take_block(8199);
        storage.take_block(8200);        
    }
}

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
    bitmap: Vec<DataBlock>,
    
    // just in memory for now
    blocks: HashMap<u64, AnyBlock>,
}


impl BlockStorage {
    
    pub fn new() -> BlockStorage {
        let mut storage = BlockStorage {
            bitmap: Vec::new(),
            blocks: HashMap::new(),
        };
        
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        
        storage
    }
    
    fn calculate_bit_addr(bit_no: usize) -> (usize, usize, usize) {
        let bm_block = bit_no / (BLOCK_SIZE * 8);
        let bm_byte = (bit_no - bm_block * BLOCK_SIZE * 8) / 8;
        let bm_bit = bit_no % 8;

        (bm_block, bm_byte, bm_bit)        
    }
    
    
    pub fn take_block(&mut self, bit_no: usize) {
        let bit_addr = BlockStorage::calculate_bit_addr(bit_no);
        
        // println!("Bit {} is found in block {} byte {} bit {}", bit_no, bit_addr.0, bit_addr.1, bit_addr.2);
    
        let db = &mut self.bitmap[bit_addr.0];
        let data = &mut db.data;
        data[bit_addr.1] |= 1 << bit_addr.2;
    }
    
    fn get_bitmap_bit(&self, bit_no: usize) -> bool {
        let bit_addr = BlockStorage::calculate_bit_addr(bit_no);
        
        // println!("Bit {} is found in block {} byte {} bit {}", bit_no, bit_addr.0, bit_addr.1, bit_addr.2);
    
        let db = &self.bitmap[bit_addr.0];
        let data = &db.data;
        
        (data[bit_addr.1] & (1 << bit_addr.2)) > 0
    }
    
    pub fn find_free_block(&self) -> usize {
        let bm_blocks = self.bitmap.len();
        let block = 0;
           
        for n in 0..bm_blocks {
            let db = &self.bitmap[n];
            let data = &db.data;
            
            for b in 0..512 {
                if data[b] != 255 {
                    // there are free bits in this byte
                    let bit_start = n * BLOCK_SIZE * 8 + b * 8;
                    for bit_no in bit_start..bit_start+8 {
                        if self.get_bitmap_bit(bit_no) == false {
                            // this was an free entry
                            println!("found free block at {}", bit_no);
                            return bit_no;
                        }    
                    }
                }
            }
        }
        
        block
    }
    
    pub fn allocate_block(&mut self) -> usize {
        let n = self.find_free_block();
        self.take_block(n);
        n
    }
    
    pub fn store(&mut self, bno: u64, block: AnyBlock) {
        println!("store() Storing block {}", bno);                
        self.blocks.insert(bno, block);
    }
    
    pub fn find_child(&mut self, parent_ino: u64, name: &String) -> Option<u64> {

        println!("find_child() finding {} from indode {}", name, parent_ino);                

        let eb_opt = self.retrieve_entry_block(parent_ino);

        match eb_opt {
            None => {
                println!("  error:  {} is no entry block", parent_ino);                
            }
            Some(eb) => {
                let mut next = eb.more_data;

                println!("  next directory block is {}", next);                

                while next != 0 {
                    let option = self.retrieve_directory_block(next);
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

        let eb_opt = self.retrieve_entry_block(parent_ino);

        match eb_opt {
            None => {
                println!("  error:  {} is no entry block", parent_ino);                
            }
            Some(eb) => {
                let mut next = eb.more_data;

                println!("  next directory block is {}", next);                

                while next != 0 {
                    let option = self.retrieve_directory_block(next);
                    
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

        let inode = self.retrieve_entry_block(ino);
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

    
    pub fn retrieve_entry_block(&mut self, bno: u64) -> Option<&mut EntryBlock> {
        let abo = self.blocks.get_mut(&bno);
        
        match abo {
            None => {
                return None;                
            }
            Some(ab) => {
                if let AnyBlock::EntryBlock(eb) = ab {
                    return Some(eb);
                }
                return None;                
            }
        }
    }

    pub fn retrieve_directory_block(&mut self, bno: u64) -> Option<&mut DirectoryBlock> {
        let abo = self.blocks.get_mut(&bno);

        println!("retrieve_directory_block() block={}", bno);                
        
        match abo {
            None => {
                println!("  error: {} is no allocated block", bno);                
                return None;                
            }
            Some(ab) => {
                if let AnyBlock::DirectoryBlock(eb) = ab {
                    return Some(eb);
                }
                println!("  error: {} is no directory block", bno);
                // debug_any_block(ab);                
                return None;                
            }
        }
    }

    pub fn retrieve_index_block(&mut self, bno: u64) -> Option<&mut IndexBlock> {
        let abo = self.blocks.get_mut(&bno);
        
        match abo {
            None => {
                return None;                
            }
            Some(ab) => {
                if let AnyBlock::IndexBlock(eb) = ab {
                    return Some(eb);
                }
                return None;                
            }
        }
    }

    pub fn retrieve_data_block(&mut self, bno: u64) -> Option<&mut DataBlock> {
        let abo = self.blocks.get_mut(&bno);
        
        match abo {
            None => {
                return None;                
            }
            Some(ab) => {
                if let AnyBlock::DataBlock(eb) = ab {
                    return Some(eb);
                }
                return None;                
            }
        }
    }
    
    pub fn read(&mut self, index_block: u64, offset: i64, size: u64) -> Vec<u8> {
        let mut result = Vec::new();

        if offset < 0 {
            println!("  data offset is negative, cannot read there.");
            return result;
        }

        if index_block != 0 {
            let blocks = &mut self.blocks;
            
            let ib_opt = blocks.get(&index_block);
            
            match ib_opt {
                None => {
                    // error
                }
                Some(ab) => {
                    if let AnyBlock::IndexBlock(ib) = ab {
                        if ib.block[0] != 0 {
                            
                            let start = offset as usize / BLOCK_SIZE as usize;
                            let end = (offset + size as i64) as usize / BLOCK_SIZE as usize;    
            
                            for n in start..=end {
                                let dbno = ib.block[n];
                                println!("  reading data block {} chain={}.", dbno, n);                
        
                                let db_opt = blocks.get(&dbno);
                                match db_opt {
                                    None => {
                                        println!("  error: block {} is no data block.", dbno);                
                                    }
                                    Some(ab) => {
                                        if let AnyBlock::DataBlock(db) = ab {
                                            println!("  copy data");                
                                            result.extend_from_slice(&db.data);
                                        }
                                        else {
                                            // error
                                        }

                                    }
                                }
                            }
                        }
                        else {
                            println!("  error: No data blocks for file.");                
                        }
                    }
                    else {
                        // error
                    }                    
                }
            }
        }
        else {
            println!("  empty file without index block.");                
        }
            
        return result;
    }


    pub fn write(&mut self, inode: u64, offset: i64, data: &[u8]) {

        if offset < 0 {
            println!("  data offset is negative, cannot write there.");
        }

        let list = self.write_data_blocks(offset as usize, data);

        let ib_no = self.allocate_block() as u64;
        let mut ib = IndexBlock::new();            

        for i in 0..list.len() {
            ib.block[i] = list[i];            
        }

        self.store(ib_no, AnyBlock::IndexBlock(ib));
        
        let eb_opt = self.retrieve_entry_block(inode);
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

            let db_no = self.allocate_block() as u64;
            let mut db = DataBlock::new();

            let data_size = std::cmp::min(BLOCK_SIZE as usize, data.len() - data_start);

            println!("  writing {} bytes to data block {} chain={}", data_size, db_no, n);
            
            // db.data.copy_from_slice(src)
            db.data[0..data_size].copy_from_slice(&data[data_start..data_start+data_size]);
            result.push(db_no);
            self.store(db_no, AnyBlock::DataBlock(db));
        }        
        
        result
    }


    pub fn mknod(&mut self, parent_ino: u64, name: &String, kind: FileType) -> Option<FileAttr> {
        println!("mknod() parent={} name={} kind={:?}", parent_ino, name, kind);

        let parent_opt = self.blocks.get(&parent_ino);

        match parent_opt {
            None => {
                println!("  error: {} is no allocated block.", parent_ino);
            }
            Some(parent) => {
                let mut ok = false;
                if let AnyBlock::EntryBlock(_eb) = parent {
                    ok = true;
                }
                else {
                    println!("  error: {} is no entry block.", parent_ino);
                }
                
                if ok {
                    let bno = self.allocate_block() as u64;
                    self.add_directory_entry(parent_ino, &name.to_string(), bno);
                    
                    let entry = EntryBlock::new(name.to_string(), bno, kind, false);
                    let attr: FileAttr = entry.attr.into();
                    self.store(bno, AnyBlock::EntryBlock(entry));
                    
                    return Some(attr);
                }
            }
        }
        
        return None;
    }


    pub fn mkdir(&mut self, parent_ino: u64, name: &String) -> Option<FileAttr> {
        println!("mkdir() parent={} name={}", parent_ino, name);

        let parent_opt = self.blocks.get(&parent_ino);

        match parent_opt {
            None => {
                println!("  error: {} is no allocated block.", parent_ino);
            }
            Some(parent) => {
                let mut ok = false;
                if let AnyBlock::EntryBlock(_eb) = parent {
                    ok = true;
                }
                else {
                    println!("  error: {} is no entry block.", parent_ino);
                }
                
                if ok {
                    let bno = self.allocate_block() as u64;
                    self.add_directory_entry(parent_ino, &name.to_string(), bno);
                    
                    let entry = EntryBlock::new(name.to_string(), bno, fuser::FileType::Directory, false);
                    let attr: FileAttr = entry.attr.into();
                    self.store(bno, AnyBlock::EntryBlock(entry));
                    
                    self.add_directory_entry(bno, &".".to_string(), bno);            
                    self.add_directory_entry(bno, &"..".to_string(), parent_ino);            
                    
                    return Some(attr);
                }
            }
        }
        
        return None;
    }
    
    
    fn extend_directory_chain(&mut self, tail: u64, name: &String, ino: u64) -> u64 {

        println!("extend_directory_chain()  Adding new directory node to chain tail {} for name {} (inode {})", tail, name, ino);

        let bno = self.allocate_block() as u64;
        let mut db = DirectoryBlock::new();
        db.entries.push(DirectoryEntry{ino: ino, name: name.to_string(),});
        
        let ab = AnyBlock::DirectoryBlock(db);
        self.store(bno, ab);

        // tail can either be an entry block or an directory block
        // directory block is more common so we check that first
        let dir_opt = self.retrieve_directory_block(tail);
        match dir_opt {
            None => {
                // ok, this should be an entry node then ...
                
                let entry_opt = self.retrieve_entry_block(tail);
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
        let parent_opt = self.retrieve_entry_block(parent_ino);

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
                        let option = self.retrieve_directory_block(next);
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