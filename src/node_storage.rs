
use crate::block_storage::{DataBlock, BLOCK_SIZE};
use std::{collections::HashMap, sync::atomic::AtomicU64};


pub struct IndexBlock {
    block: [u64; (BLOCK_SIZE/8) as usize],
}

enum AnyBlock {
    IB(IndexBlock),
    DB(DataBlock),
}

pub struct NodeStorage {

    // just in memory for now
    nodes: HashMap<u64, AnyBlock>,
    next_block: AtomicU64,
}

impl NodeStorage {
    pub fn new() -> NodeStorage {
        NodeStorage {
            nodes: HashMap::new(),
            next_block: AtomicU64::new(1),
        }
    }
    
    fn allocate_index_block(&mut self) -> u64 {
        let ib_no = self.next_block.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    
        let ib = IndexBlock {
            block: [0; (BLOCK_SIZE/8) as usize],
        };
    
        println!("  allocating new index block {}", ib_no);

        self.nodes.insert(ib_no, AnyBlock::IB(ib));
    
        ib_no
    }
    
    fn allocate_data_block_if_needed(&mut self, index_block: u64, data_pos: usize) -> u64 {
        
        let blocks = & mut self.nodes;
        
        let ab = blocks.get_mut(&index_block).unwrap();

        if let AnyBlock::IB(ib) = ab {

            if ib.block[data_pos] == 0 {
                let db_no = self.next_block.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                println!("  allocating new data block {}, chain={}", db_no, data_pos);
                ib.block[data_pos] = db_no;
        
                let db = DataBlock {
                    data: [0; BLOCK_SIZE as usize],
                };
                blocks.insert(db_no, AnyBlock::DB(db));
                
                return db_no;
            }    
        }    

        0
    }
    
    
    pub fn read(&mut self, index_block: u64, offset: i64, size: u64) -> Vec<u8> {
        let mut result = Vec::new();

        if offset < 0 {
            println!("  data offset is negative, cannot read there.");
            return result;
        }

        if index_block != 0 {
            let blocks = & self.nodes;
            let ab = blocks.get(&index_block).unwrap();
    
            if let AnyBlock::IB(ib) = ab {
                if ib.block[0] != 0 {
                    
                    let start = offset as usize / BLOCK_SIZE as usize;
                    let end = (offset + size as i64) as usize / BLOCK_SIZE as usize;    
    
                    for n in start..=end {
                        let dbno = ib.block[n];
                        println!("  reading data block {} chain={}.", dbno, n);                

                        let db_opt = blocks.get(&dbno);
                        let adb = db_opt.unwrap();
                        if let AnyBlock::DB(db) = adb {
                            println!("  copy data");                
        
                            result.extend_from_slice(&db.data);
                        }
                        else {
                            println!("  error: block {} is no data block.", dbno);                
                        }
                    }
                }
                else {
                    println!("  error: No data blocks for file.");                
                }
            }
            else {
                println!("  empty file without index block.");                
            }
        }
            
        return result;
    }
    
    
    pub fn write(&mut self, index_block_in: u64, offset: i64, data: &[u8]) -> u64 {

        if offset < 0 {
            println!("  data offset is negative, cannot write there.");
            return 0;
        }
        
        let index_block = 
            if index_block_in == 0 { self.allocate_index_block() } else {index_block_in};
        
        self.write_data_blocks(index_block, offset as usize, data);
        
        index_block
    }


    fn write_data_blocks(&mut self, index_block: u64, offset: usize, data: &[u8]) {

        let start = offset / BLOCK_SIZE as usize;
        let end = (offset + data.len()) / BLOCK_SIZE as usize;    

        for n in start..=end {

            let data_start = (n - start) * BLOCK_SIZE as usize;
                
            let db_no = self.allocate_data_block_if_needed(index_block, n);
            let db_opt = self.nodes.get_mut(&db_no);
            let ab = db_opt.unwrap();
                
            if let AnyBlock::DB(db) = ab {
                
                let data_size = std::cmp::min(BLOCK_SIZE as usize, data.len() - data_start);

                println!("  writing {} bytes to data block {} chain={}", data_size, db_no, n);
                
                // db.data.copy_from_slice(src)
                db.data[0..data_size].copy_from_slice(&data[data_start..data_start+data_size]);
            }
            else {
                println!("  error: block {} is no data block", n);
            }
        }        
        
    }

}