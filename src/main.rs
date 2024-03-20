use node_storage::NodeStorage;
use clap::{Arg, ArgAction, Command};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow
};
use libc::{ENOENT, ENOSYS, EPERM};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod node_storage;


const TTL: Duration = Duration::from_secs(1); // 1 second

const INO_ROOT:u64 = 1;
const INO_PATHES:u64 = 2;
const INO_TAGS:u64 = 3;


fn make_attr(ino: u64, kind: FileType) -> FileAttr
{
    let perm = if kind == FileType::Directory {0o755} else {0o644};
    
	FileAttr {
	    ino: ino,
	    size: 0,
	    blocks: 0,
	    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
	    mtime: UNIX_EPOCH,
	    ctime: UNIX_EPOCH,
	    crtime: UNIX_EPOCH,
	    kind: kind,
	    perm: perm,
	    nlink: 2,
	    uid: 501,
	    gid: 100,
	    rdev: 0,
	    flags: 0,
	    blksize: 512,
	}
}


fn safe_to_string(osstr: &OsStr) -> String {	
	let optional_name = osstr.to_str();

	let text =
	match optional_name {
		None => "",
		Some(x) => x, 
	};
	
	let mut result = String::new();
	result += text;
	return result;
}


fn as_file_type(mut mode: u32) -> FileType {
    mode &= libc::S_IFMT as u32;

    if mode == libc::S_IFREG as u32 {
        return FileType::RegularFile;
    } else if mode == libc::S_IFLNK as u32 {
        return FileType::Symlink;
    } else if mode == libc::S_IFDIR as u32 {
        return FileType::Directory;
    } else {
        print!("as_file_kind() unknown mode, mode={}", mode);
        return FileType::RegularFile;
    }
}


struct FsNode {
	name: String,
	is_tag: bool,
	attr: FileAttr,
	
	// if this is a directory, it may have children
	children: HashMap<String, u64>,
	
	// if this is a file, it may have index blocks
	// this is either0 or the number of the first index block of the file
    index_block: u64,
}


struct PathTagFs {
	nodes: HashMap<u64, FsNode>,
	next_node: AtomicU64, 
    next_file_handle: AtomicU64,
    storage: NodeStorage, 
}


impl FsNode {
	fn new(name: String, parent_ino: u64, ino: u64, kind: FileType, is_tag: bool) -> FsNode {
        let mut children = HashMap::new();

        if kind == FileType::Directory {
            children.insert(".".to_string(), ino);
            children.insert("..".to_string(), parent_ino);
        } 

		let node = FsNode { 
			name: name,
			is_tag: is_tag,
			attr: make_attr(ino, kind),
			children: children,
			index_block: 0, 
		};
		
		return node;		
	}

	fn add_node(&mut self, node: &FsNode) {
		self.children.insert(node.name.to_string(), node.attr.ino);
	}
	
	fn find_child(& self, name: &String) -> Option<&u64> {
		return self.children.get(name);
	}
}


impl PathTagFs {

	fn new() -> PathTagFs {
		PathTagFs {
			nodes: HashMap::new(),
			next_node: AtomicU64::new(4), 
            next_file_handle: AtomicU64::new(1),
            storage: NodeStorage::new(), 
		}
	}
	
	fn initialize(& mut self) {
		let mut root = FsNode::new("Root".to_string(), INO_ROOT, INO_ROOT, FileType::Directory, false);
		let pathes = FsNode::new("Pathes".to_string(), INO_ROOT, INO_PATHES, FileType::Directory, false);
		let tags = FsNode::new("Tags".to_string(), INO_ROOT, INO_TAGS, FileType::Directory, true);

		root.add_node(&pathes);
		root.add_node(&tags);
		
    	self.nodes.insert(pathes.attr.ino, pathes);
    	self.nodes.insert(tags.attr.ino, tags);

		// root is special because it has itself as parent
    	self.nodes.insert(root.attr.ino, root);
	}

    fn take_next(handle: & mut AtomicU64) -> u64
    {
        let result = handle.fetch_add(1, std::sync::atomic::Ordering::Relaxed);        
        return result;
    }
}


impl Filesystem for PathTagFs {

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        Ok(())
    }


    /// Clean up filesystem.
    /// Called on filesystem exit.
    fn destroy(&mut self) {
        
    }


    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, os_fname: &OsStr, reply: ReplyEntry) {
				
		let fname = safe_to_string(os_fname); 		
		println!("lookup name={} parent={}", fname, parent);
		
		let parent = self.nodes.get(&parent);
		
		match parent {
			None => reply.error(ENOENT),
			Some(parent) => {
				let ino: Option<&u64> = parent.find_child(&fname); 

				match ino {
					None => reply.error(ENOENT),
					Some(ino) => {
						let node: &FsNode = self.nodes.get(ino).unwrap();							
						reply.entry(&TTL, &node.attr, 0);
					}
				}
			}
		}
    }


    /// Get file attributes.
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
		println!("getattr inode={}", ino);

		let node = self.nodes.get(&ino);

        match node {
            Some(node) => reply.attr(&TTL, &node.attr),
            None => reply.error(ENOENT),
        }
    }


    /// Set file attributes.
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!(
            "setattr(ino: {:#x?}, mode: {:?}, uid: {:?}, \
            gid: {:?}, size: {:?}, fh: {:?}, flags: {:?})",
            ino, mode, uid, gid, size, fh, flags
        );
        
        let node_opt = self.nodes.get_mut(&ino);
        
        match node_opt {
            None => {
                
            }
            Some(node) => {
                let attrs = &mut node.attr;
                let time = &SystemTime::now();

                if let Some(size) = size {
                    println!("  setting new size={}", size);
                    attrs.size = size;                    
                    attrs.mtime = *time;
                }

                // self.write_inode(&attrs);
                reply.attr(&Duration::new(0, 0), &attrs);
            }
        }
        
        
        // reply.error(ENOSYS);
    }
    
   
    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.    
	fn mknod(
        &mut self,
        _req: &Request,
        parent_ino: u64,
        os_name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
       println!("mknod() parent={:#x?} name='{:?}' mode={} umask={:#x?})",
            parent_ino, os_name, mode, umask
        );


        let file_type = mode & libc::S_IFMT as u32;

        if file_type != libc::S_IFREG as u32
            && file_type != libc::S_IFLNK as u32
            && file_type != libc::S_IFDIR as u32
        {
            println!("mknod() implementation only supports regular files, symlinks, and directories. Got {:o}", mode);
            reply.error(libc::ENOSYS);
            return;
        }

        let parent_opt = self.nodes.get_mut(&parent_ino);

        match parent_opt {
            None => {
                reply.error(ENOENT);
            }
            Some(parent) => {
                let name = safe_to_string(os_name);            

                if parent.children.get(&name) == None {
                    let ino: u64 = PathTagFs::take_next(& mut self.next_node);
                    
                    let kind = as_file_type(mode);   
                    let new_node = FsNode::new(name, parent_ino, ino, kind, parent.is_tag);

                    parent.children.insert(new_node.name.to_string(), ino);     
                    reply.entry(&Duration::new(0, 0), &new_node.attr, 0);
                    self.nodes.insert(ino, new_node);
                }
                else {
                    reply.error(libc::EEXIST);
                }
            }
        }
    }
    
    
    /// Create a directory.
	fn mkdir(
        &mut self,
        _req: &Request,
        parent_ino: u64,
        os_name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        println!(
            "mkdir() parent={:#x?} name='{:?}' mode={} umask={:#x?})",
            parent_ino, os_name, mode, umask
        );
        
		let parent_opt = self.nodes.get_mut(&parent_ino);

		match parent_opt {
			None => {
		        reply.error(ENOENT);
			}
			Some(parent) => {
				let name = safe_to_string(os_name);
				
		        if parent.children.get(&name) == None {
					let ino: u64 = PathTagFs::take_next(& mut self.next_node);
					let new_node = FsNode::new(name, parent_ino, ino, FileType::Directory, parent.is_tag);
	
					parent.children.insert(new_node.name.to_string(), ino);		
					reply.entry(&Duration::new(0, 0), &new_node.attr, 0);
					self.nodes.insert(ino, new_node);
    			}
				else {
        		    reply.error(libc::EEXIST);
				}
			}
		}

        // reply.error(ENOSYS);
    }


    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {
        
    }

    /// Like forget, but take multiple forget requests at once for performance. The default
    /// implementation will fallback to forget.
    #[cfg(feature = "abi-7-16")]
    fn batch_forget(&mut self, req: &Request<'_>, nodes: &[fuse_forget_one]) {
        for node in nodes {
            self.forget(req, node.nodeid, node.nlookup);
        }
    }


    /// Read symbolic link.
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        println!("[Not Implemented] readlink(ino: {:#x?})", ino);
        reply.error(ENOSYS);
    }


    /// Remove a file.
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!(
            "[Not Implemented] unlink(parent: {:#x?}, name: {:?})",
            parent, name,
        );
        reply.error(ENOSYS);
    }

    /// Remove a directory.
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!(
            "[Not Implemented] rmdir(parent: {:#x?}, name: {:?})",
            parent, name,
        );
        reply.error(ENOSYS);
    }


    /// Create a symbolic link.
    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        println!(
            "[Not Implemented] symlink(parent: {:#x?}, link_name: {:?}, target: {:?})",
            parent, link_name, target,
        );
        reply.error(EPERM);
    }


    /// Rename a file.
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] rename(parent: {:#x?}, name: {:?}, newparent: {:#x?}, \
            newname: {:?}, flags: {})",
            parent, name, newparent, newname, flags,
        );
        reply.error(ENOSYS);
    }


    /// Create a hard link.
    fn link(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        println!(
            "link() called for {}, {}, {:?}",
            inode, new_parent, new_name
        );

        reply.error(EPERM);
    }


    /// Open a file.
    /// Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use this in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details.
    fn open(&mut self, _req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        println!("open() inode={:?} flags={:b}", inode, flags);

        // access forbidden
        // reply.error(libc::EACCES);

        let node_opt = self.nodes.get(&inode);

        match node_opt {
            None => {
                // invalid value, ist that ok here?
                reply.error(libc::EINVAL);
            }
            Some(_node) => {
                let handle = PathTagFs::take_next(&mut self.next_file_handle);
                let open_flags = 0; // ???
                reply.opened(handle, open_flags);
            }
        }
    }


    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in 'direct_io' mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. handle will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    ///
    /// flags: these are the file flags, such as O_SYNC. Only supported with ABI >= 7.9
    /// lock_owner: only supported with ABI >= 7.9
    fn read(
        &mut self,
        _req: &Request,
        inode: u64,
        handle: u64,
        offset: i64,
        req_size: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        println!(
            "read() called for inode={:?} handle={} flags={:b} offset={:?} size={:?}",
            inode, handle, flags, offset, req_size
        );
        assert!(offset >= 0);
        
        // if !self.check_file_handle_read(fh) {
        //    reply.error(libc::EACCES);
        //    return;
        // }

        // right now we just assume that all parameters were ok
        if true {
            let node = self.nodes.get(&inode).unwrap();
            let size = std::cmp::min(req_size as u64, node.attr.size);
            let buffer = self.storage.read(node.index_block, offset, size);

            reply.data(&buffer);
        } else {
            reply.error(libc::ENOENT);
        }
    }


    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in 'direct_io' mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. handle will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value.
    ///
    /// write_flags: will contain FUSE_WRITE_CACHE, if this write is from the page cache. If set,
    /// the pid, uid, gid, and fh may not match the value that would have been sent if write cachin
    /// is disabled
    /// flags: these are the file flags, such as O_SYNC. Only supported with ABI >= 7.9
    /// lock_owner: only supported with ABI >= 7.9
    fn write(
        &mut self,
        _req: &Request,
        inode: u64,
        handle: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        #[allow(unused_variables)] flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        println!("write() called for inode={:?} handle={} flags={:b} size={:?} at offset={}", 
            inode, handle, flags, data.len(), offset);
        assert!(offset >= 0);
        // if !self.check_file_handle_write(fh) {
        //    reply.error(libc::EACCES);
        //    return;
        // }

        // right now we do not write anyways, just framework for later
        if true {
            println!("  setting file size to {}", data.len());

            let node = self.nodes.get_mut(&inode).unwrap();
            let attrs = &mut node.attr;

            let ib = self.storage.write(node.index_block, offset, data);            
            println!("  setting index block of node {} to {}", inode, ib);
            node.index_block = ib;

            attrs.size = data.len() as u64;                    

            // fake it if we can't make it ...
            reply.written(data.len() as u32);
        } else {
            reply.error(libc::EBADF);
        }
    }


    /// Flush method.
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems shouldn't assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method didn't set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to 'lock_owner'.
    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        println!(
            "[Not Implemented] flush(ino: {:#x?}, fh: {}, lock_owner: {:?})",
            ino, fh, lock_owner
        );
        reply.error(ENOSYS);
    }
    

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all file
    /// descriptors are closed and all memory mappings are unmapped. For every open
    /// call there will be exactly one release call. The filesystem may reply with an
    /// error, but error values are not returned to close() or munmap() which triggered
    /// the release. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same flags as for
    /// open.
    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }


    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        println!(
            "[Not Implemented] fsync(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }


    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
    /// use this in other all other directory stream operations (readdir, releasedir,
    /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
    /// anything in fh, though that makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can change
    /// between opendir and releasedir.
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        println!(
            "opendir(ino: {:#x?}, flags: {})", ino, flags);
        reply.opened(0, 0);
    }


    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir directory_inode={} offset={}", ino, offset);


        let dirnode = self.nodes.get(&ino); 
        
        match dirnode {
            None => reply.error(ENOENT),
            Some(dirnode) => {
                let children = &dirnode.children;
                let mut i: i64 = 0;
                
                for (name, ino) in children {
                    let node = self.nodes.get(&ino);
                    
                    if i >= offset {
                        match node {
                            None => {
                                reply.error(ENOENT);
                                return;
                            }
                            Some(node) => {
                                println!("  entry: index={} inode={} name={}", i, ino, name);
                                
                                // i + 1 means the index of the next entry
                                if reply.add(*ino, (i + 1) as i64, node.attr.kind, name) {
                                    break;
                                }
                            }
                        }
                    }
                    i = i + 1;
                }               
                
                reply.ok();
            }
        }
    }


    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectoryPlus,
    ) {
        println!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }


    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }
    

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] fsyncdir(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }
    

    /// Get file system statistics.
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }
    

    /// Set an extended attribute.
    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        _value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] setxattr(ino: {:#x?}, name: {:?}, flags: {:#x?}, position: {})",
            ino, name, flags, position
        );
        reply.error(ENOSYS);
    }
    

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        println!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
        reply.error(ENOSYS);
    }
    

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        println!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino, size
        );
        reply.error(ENOSYS);
    }


    /// Remove an extended attribute.
    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        println!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino, name
        );
        reply.error(ENOSYS);
    }


    /// Check file access permissions.
    /// This will be called for the access() system call. If the 'default_permissions'
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        println!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(ENOSYS);
    }
    

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details. If this method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        println!(
            "[Not Implemented] create(parent: {:#x?}, name: {:?}, mode: {}, umask: {:#x?}, \
            flags: {:#x?})",
            parent, name, mode, umask, flags
        );
        reply.error(ENOSYS);
    }


    /// Test for a POSIX file lock.
    fn getlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        println!(
            "[Not Implemented] getlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {})",
            ino, fh, lock_owner, start, end, typ, pid
        );
        reply.error(ENOSYS);
    }
    

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case.  For checking lock ownership,
    /// 'fi->owner' must be used. The l_pid field in 'struct flock' should only be
    /// used to fill in this field in getlk(). Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] setlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {}, sleep: {})",
            ino, fh, lock_owner, start, end, typ, pid, sleep
        );
        reply.error(ENOSYS);
    }
    

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the 'blkdev' option
    fn bmap(&mut self, _req: &Request<'_>, ino: u64, blocksize: u32, idx: u64, reply: ReplyBmap) {
        println!(
            "[Not Implemented] bmap(ino: {:#x?}, blocksize: {}, idx: {})",
            ino, blocksize, idx,
        );
        reply.error(ENOSYS);
    }

    /// control device
    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        println!(
            "[Not Implemented] ioctl(ino: {:#x?}, fh: {}, flags: {}, cmd: {}, \
            in_data.len(): {}, out_size: {})",
            ino,
            fh,
            flags,
            cmd,
            in_data.len(),
            out_size,
        );
        reply.error(ENOSYS);
    }
    

    /// Poll for events
    #[cfg(feature = "abi-7-11")]
    fn poll(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        kh: u64,
        events: u32,
        flags: u32,
        reply: ReplyPoll,
    ) {
        println!(
            "[Not Implemented] poll(ino: {:#x?}, fh: {}, kh: {}, events: {}, flags: {})",
            ino, fh, kh, events, flags
        );
        reply.error(ENOSYS);
    }
    

    /// Preallocate or deallocate space to a file
    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] fallocate(ino: {:#x?}, fh: {}, offset: {}, \
            length: {}, mode: {})",
            ino, fh, offset, length, mode
        );
        reply.error(ENOSYS);
    }
    

    /// Reposition read/write file offset
    fn lseek(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        println!(
            "[Not Implemented] lseek(ino: {:#x?}, fh: {}, offset: {}, whence: {})",
            ino, fh, offset, whence
        );
        reply.error(ENOSYS);
    }
    

    /// Copy the specified range from the source inode to the destination inode
    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: ReplyWrite,
    ) {
        println!(
            "[Not Implemented] copy_file_range(ino_in: {:#x?}, fh_in: {}, \
            offset_in: {}, ino_out: {:#x?}, fh_out: {}, offset_out: {}, \
            len: {}, flags: {})",
            ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags
        );
        reply.error(ENOSYS);
    }
    
}



fn main() {
    let matches = Command::new("path_tag_fs")
        // .version(crate_version!())
        .version("0.1.0")
        .author("H. Malthaner")
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(1)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .action(ArgAction::SetTrue)
                .help("Allow root user to access filesystem"),
        )
        .get_matches();
        
    env_logger::init();
    
    let mountpoint = matches.get_one::<String>("MOUNT_POINT").unwrap();
    // let mut options = vec![MountOption::RO, MountOption::FSName("path_tag_fs".to_string())];
    let mut options = vec![MountOption::RW, MountOption::FSName("path_tag_fs".to_string())];
    
    if matches.get_flag("auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }
    
    if matches.get_flag("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    
    let mut file_system: PathTagFs = PathTagFs::new();     
    file_system.initialize();
    fuser::mount2(file_system, mountpoint, &options).unwrap();
}
