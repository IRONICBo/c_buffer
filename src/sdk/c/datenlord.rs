use std::ffi::CStr;
use std::os::raw::{c_char, c_uint};
use std::ptr;
use bytes::BytesMut;
use tokio::runtime::Runtime;
use std::sync::{Arc, Mutex};

use crate::storage::fs_util::{CreateParam, RenameParam};
use crate::storage::localfs::LocalFS;
use crate::storage::virtualfs::VirtualFs;

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_error {
    pub code: c_uint,
    pub message: datenlord_bytes,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_bytes {
    pub data: *const u8,
    pub len: usize,
}

impl datenlord_error {
    fn new(code: c_uint, message: String) -> *mut datenlord_error {
        let message_bytes = message.into_bytes();
        let error = Box::new(datenlord_error {
            code,
            message: datenlord_bytes {
                data: message_bytes.as_ptr(),
                len: message_bytes.len(),
            },
        });
        Box::into_raw(error)
    }
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_sdk {
    localfs: Arc<Mutex<LocalFS>>, // 保存 `LocalFS` 实例
}

#[no_mangle]
pub extern "C" fn init(config: *const c_char) -> *mut datenlord_sdk {
    if config.is_null() {
        return ptr::null_mut();
    }

    let config_str = unsafe {
        CStr::from_ptr(config)
            .to_str()
            .unwrap_or("default config")
    };

    let localfs = LocalFS::new().unwrap();
    let sdk = Box::new(datenlord_sdk {
        localfs: Arc::new(Mutex::new(localfs)),
    });

    Box::into_raw(sdk)
}

#[no_mangle]
pub extern "C" fn free_sdk(sdk: *mut datenlord_sdk) {
    if !sdk.is_null() {
        unsafe {
            Box::from_raw(sdk);
        }
    }
}

#[no_mangle]
pub extern "C" fn exists(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> bool {
    if sdk.is_null() || dir_path.is_null() {
        return false;
    }

    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();
        // demo inode info
        localfs.lookup(1000, 1000, 1, path).await
    });

    result.is_ok()
}

#[no_mangle]
pub extern "C" fn mkdir(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> *mut datenlord_error {
    if sdk.is_null() || dir_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let param = CreateParam {
            parent: 1,// test inode
            name: path.to_string(),
            mode: 0o755,
            rdev: 0,
            uid: 1000,
            gid: 1000,
            node_type: nix::sys::stat::SFlag::S_IFDIR,
            link: None,
        };

        let mut localfs = sdk_ref.localfs.lock().unwrap();
        localfs.mkdir(param).await
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to create directory".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn delete_dir(
    sdk: *mut datenlord_sdk,
    dir_path: *const c_char,
    recursive: bool
) -> *mut datenlord_error {
    if sdk.is_null() || dir_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    // dimiss recursive now
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();
        localfs.rmdir(1000, 1000, 1, path).await
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to remove directory".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn rename_path(
    sdk: *mut datenlord_sdk,
    src_path: *const c_char,
    dest_path: *const c_char
) -> *mut datenlord_error {
    if sdk.is_null() || src_path.is_null() || dest_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let src = unsafe { CStr::from_ptr(src_path).to_str().unwrap_or_default() };
    let dest = unsafe { CStr::from_ptr(dest_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let param = RenameParam {
            old_parent: 1,
            old_name: src.to_string(),
            new_parent: 1,
            new_name: dest.to_string(),
            flags: 0,
        };
        let mut localfs = sdk_ref.localfs.lock().unwrap();
        localfs.rename(1000, 1000, param).await
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to rename path".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn copy_from_local_file(
    sdk: *mut datenlord_sdk,
    overwrite: bool,
    local_file_path: *const c_char,
    dest_file_path: *const c_char
) -> *mut datenlord_error {
    if sdk.is_null() || local_file_path.is_null() || dest_file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let local = unsafe { CStr::from_ptr(local_file_path).to_str().unwrap_or_default() };
    let dest = unsafe { CStr::from_ptr(dest_file_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();

        if !overwrite && localfs.lookup(1000, 1000, 1, dest).await.is_ok() {
            return Err(());
        }

        match std::fs::read(local) {
            Ok(content) => {
                match localfs.write(1, 0, 0, &content, 0).await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            Err(_) => Err(()),
        }
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to copy file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn copy_to_local_file(
    sdk: *mut datenlord_sdk,
    src_file_path: *const c_char,
    local_file_path: *const c_char
) -> *mut datenlord_error {
    if sdk.is_null() || src_file_path.is_null() || local_file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let src = unsafe { CStr::from_ptr(src_file_path).to_str().unwrap_or_default() };
    let local = unsafe { CStr::from_ptr(local_file_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let mut buf = BytesMut::new();
        let mut localfs = sdk_ref.localfs.lock().unwrap();

        // for demo purpose, we need to get the hole file size
        match localfs.read(1, 0, 0, 1024, &mut buf).await {
            Ok(size) => {
                match std::fs::write(local, &buf[..size]) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            Err(_) => Err(()),
        }
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to copy file to local".to_string()),
    }
}


#[no_mangle]
pub extern "C" fn stat(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();
        localfs.getattr(1).await  // 示例 inode
    });

    match result {
        Ok(attr) => {
            println!("File duration: {:?}, attr: {:?}", attr.0, attr.1);
            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to get file metadata".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn write_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    content: datenlord_bytes,
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let data = unsafe { std::slice::from_raw_parts(content.data, content.len) };

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();
        // demo params
        localfs.write(1, 0, 0, data, 0).await
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to write file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn read_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    out_content: *mut datenlord_bytes,
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() || out_content.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    // TODO, use outside buffer
    let result = rt.block_on(async {
        let mut localfs = sdk_ref.localfs.lock().unwrap();

        // Convert buffer to c buffer
        let out_content_data = unsafe { (*out_content).data as *mut u8 };
        let out_content_len = unsafe { (*out_content).len };
        let buffer: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(out_content_data, out_content_len) };

        localfs.read(1, 0, 0, buffer.len() as u32, buffer).await
    });

    match result {
        Ok(size) => {
            unsafe {
                (*out_content).len = size;
            }
            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to read file".to_string()),
    }
}