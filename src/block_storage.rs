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


pub const BLOCK_SIZE:u32 = 1024;

pub struct DataBlock {
    pub data: [u8; BLOCK_SIZE as usize],
}

struct BlockStorage {
    bitmap: Vec<DataBlock>,
}

impl DataBlock {
    fn new() -> DataBlock {
        DataBlock {
            data: [0; BLOCK_SIZE as usize],
        }
    }
}

impl BlockStorage {
    
    fn new() -> BlockStorage {
        let mut storage = BlockStorage {
            bitmap: Vec::new(),
        };
        
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        storage.bitmap.push(DataBlock::new());    
        
        storage
    }
    
    fn calculate_bit_addr(bit_no: u64) -> (usize, usize, usize) {
        let bm_block = bit_no / (BLOCK_SIZE as u64 * 8);
        let bm_byte = (bit_no - bm_block * BLOCK_SIZE as u64  * 8) / 8;
        let bm_bit = bit_no % 8;

        (bm_block as usize, bm_byte as usize, bm_bit as usize)        
    }
    
    
    fn take_block(&mut self, bit_no: u64) {
        let bit_addr = BlockStorage::calculate_bit_addr(bit_no);
        
        println!("Bit {} is found in block {} byte {} bit {}", bit_no, bit_addr.0, bit_addr.1, bit_addr.2);
    
        let db = &mut self.bitmap[bit_addr.0];
        let data = &mut db.data;
        data[bit_addr.1] |= 1 << bit_addr.2;
    }
    
}