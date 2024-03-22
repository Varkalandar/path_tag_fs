use std::{fs::File, io::{Error, ErrorKind, Write}};
use crate::{nodes::{AnyBlock, EntryBlock, IndexBlock, DirectoryBlock, DataBlock}};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_write() {
        let mut bio = BlockIo::new("/tmp/dump");
        let b = DataBlock::new();
        let ab = AnyBlock::DataBlock(b);
        
        let result = bio.write_block(ab);

        assert!(result.is_ok());
        
        if let Result::Ok(size) = result {
            println!("size={}", size);
            assert!(size == 1024);            
        }        
    }
}



struct BlockIo {
    file: File,
}

impl BlockIo {

    pub fn new(path: &str) -> BlockIo {
        let file = File::options().read(true).write(true).create(true).open(path);

        BlockIo {
            file: file.unwrap(),
        }
    }

    
    pub fn write_block(&mut self, ab: AnyBlock) -> Result<usize, Error> {
        let size;
        
        match ab {
            AnyBlock::EntryBlock(b) => {
                size = self.write_entry_block(b);
            }
            AnyBlock::IndexBlock(b) => {
                size = self.write_index_block(b);
            }
            AnyBlock::DirectoryBlock(b) => {
                size = self.write_directory_block(b);
            }
            AnyBlock::DataBlock(b) => {
                size = self.write_data_block(b);
            }
        }
        return size;
    }
    
    fn write_entry_block(&mut self, b: EntryBlock) -> Result<usize, Error> {
        let result: Result<usize, Error> = Result::Err(Error::new(ErrorKind::Other, "Not implemented"));
        return result;
    }

    fn write_index_block(&mut self, b: IndexBlock) -> Result<usize, Error> {
        let result: Result<usize, Error> = Result::Err(Error::new(ErrorKind::Other, "Not implemented"));
        return result;
    }

    fn write_directory_block(&mut self, b: DirectoryBlock) -> Result<usize, Error> {
        let result: Result<usize, Error> = Result::Err(Error::new(ErrorKind::Other, "Not implemented"));
        return result;
    }

    fn write_data_block(&mut self, b: DataBlock) -> Result<usize, Error> {
        let size = self.file.write(&b.data);
        println!("write_data_block() {:?} bytes written", size);
        return size;
    }

}