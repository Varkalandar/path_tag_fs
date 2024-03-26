use std::collections::HashMap;
use std::io::Error;
use crate::{block_storage::BLOCK_SIZE, nodes::{AnyBlock, DataBlock, DirectoryBlock, EntryBlock, IndexBlock}};


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_set() {
        let mut storage = BlockCache::new();
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


pub struct BlockCache {
    pub bitmap: Vec<DataBlock>,
    
    // just in memory for now
    blocks: HashMap<u64, AnyBlock>,
}


impl BlockCache {


    pub fn new() -> BlockCache {
        let mut storage = BlockCache {
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
        let bit_addr = BlockCache::calculate_bit_addr(bit_no);
        
        // println!("Bit {} is found in block {} byte {} bit {}", bit_no, bit_addr.0, bit_addr.1, bit_addr.2);
    
        let db = &mut self.bitmap[bit_addr.0];
        let data = &mut db.data;
        data[bit_addr.1] |= 1 << bit_addr.2;
    }

    
    fn get_bitmap_bit(&self, bit_no: usize) -> bool {
        let bit_addr = BlockCache::calculate_bit_addr(bit_no);
        
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

    
    pub fn write_block(&mut self, ab: AnyBlock, no: u64) -> Result<usize, Error> {

        self.blocks.insert(no, ab);
        return Ok(0);
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
}