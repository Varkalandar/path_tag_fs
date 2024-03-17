use clap::{Arg, ArgAction, Command};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{ENOENT, ENOSYS};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1); // 1 second


const TEST_FILE_CONTENT: &str = "Hello World!\n";

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
	children: HashMap<String, u64>,
}


struct PathTagFs {
	nodes: HashMap<u64, FsNode>,
	next_node: u64, 
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
			next_node: 4, 
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


}


impl Filesystem for PathTagFs {

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


    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
		println!("getattr inode={}", ino);

		let node = self.nodes.get(&ino);

        match node {
            Some(node) => reply.attr(&TTL, &node.attr),
            None => reply.error(ENOENT),
        }
    }


    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
		println!("read inode={}", ino);

        if ino == 4 {
            reply.data(&TEST_FILE_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }


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
       println!("mknod() parent={:#x?} name={:?} mode={} umask={:#x?})",
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
                    let ino: u64 = self.next_node;
                    self.next_node += 1;
                    
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
            "mkdir() parent={:#x?} name={:?} mode={} umask={:#x?})",
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
					let ino: u64 = self.next_node;
                    self.next_node += 1;   
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
    
	fn link(
        &mut self,
        _req: &Request,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        _reply: ReplyEntry,
    ) {
        println!(
            "link() called for {}, {}, {:?}",
            inode, new_parent, new_name
        );
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
