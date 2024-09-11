use std::ffi::CStr;
use std::os::raw::{c_char, c_uint, c_void};
use std::ptr;
use std::str;

#[repr(C)]
pub struct datenlord_error {
    pub code: c_uint,
    pub message: datenlord_bytes,
}

#[repr(C)]
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

#[no_mangle]
pub extern "C" fn init(config: *const c_char) -> *mut datenlord_error {
    if config.is_null() {
        return datenlord_error::new(1, "config is null".to_string());
    }

    let config_str = unsafe {
        CStr::from_ptr(config)
            .to_str()
            .map_err(|_| datenlord_error::new(2, "Invalid UTF-8 config".to_string()))
    };

    match config_str {
        Ok(_config) => {
            // 假设你有相应的 SDK 初始化逻辑
            // 这里应该进行日志级别和资源的初始化
            // init_sdk(_config);
            std::ptr::null_mut()
        }
        Err(err) => err,
    }
}

use std::fs;

#[no_mangle]
pub extern "C" fn exists(dir_path: *const c_char) -> bool {
    if dir_path.is_null() {
        return false;
    }

    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };
    fs::metadata(path).is_ok()
}

#[no_mangle]
pub extern "C" fn mkdir(dir_path: *const c_char) -> *mut datenlord_error {
    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };
    match fs::create_dir(path) {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to create directory".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn delete(dir_path: *const c_char, recursive: bool) -> *mut datenlord_error {
    let path = unsafe { CStr::from_ptr(dir_path).to_str().unwrap_or_default() };
    if recursive {
        match fs::remove_dir_all(path) {
            Ok(_) => std::ptr::null_mut(),
            Err(_) => datenlord_error::new(1, "Failed to remove directory recursively".to_string()),
        }
    } else {
        match fs::remove_dir(path) {
            Ok(_) => std::ptr::null_mut(),
            Err(_) => datenlord_error::new(1, "Failed to remove directory".to_string()),
        }
    }
}

#[no_mangle]
pub extern "C" fn rename(src_path: *const c_char, dest_path: *const c_char) -> *mut datenlord_error {
    let src = unsafe { CStr::from_ptr(src_path).to_str().unwrap_or_default() };
    let dest = unsafe { CStr::from_ptr(dest_path).to_str().unwrap_or_default() };
    match fs::rename(src, dest) {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to rename directory".to_string()),
    }
}

use std::io::{Read, Write};

#[no_mangle]
pub extern "C" fn copy_from_local_file(
    overwrite: bool,
    local_file_path: *const c_char,
    dest_file_path: *const c_char,
) -> *mut datenlord_error {
    let local = unsafe { CStr::from_ptr(local_file_path).to_str().unwrap_or_default() };
    let dest = unsafe { CStr::from_ptr(dest_file_path).to_str().unwrap_or_default() };

    if !overwrite && fs::metadata(dest).is_ok() {
        return datenlord_error::new(1, "Destination file already exists".to_string());
    }

    match fs::copy(local, dest) {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to copy file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn copy_to_local_file(src_file_path: *const c_char, local_file_path: *const c_char) -> *mut datenlord_error {
    let src = unsafe { CStr::from_ptr(src_file_path).to_str().unwrap_or_default() };
    let dest = unsafe { CStr::from_ptr(local_file_path).to_str().unwrap_or_default() };

    match fs::copy(src, dest) {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to copy file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn stat(file_path: *const c_char) -> *mut datenlord_error {
    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    match fs::metadata(path) {
        Ok(metadata) => {
            // 处理元数据，例如大小、修改时间等
            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to get file metadata".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn write_file(file_path: *const c_char, content: datenlord_bytes) -> *mut datenlord_error {
    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let data = unsafe { std::slice::from_raw_parts(content.data, content.len) };

    match fs::write(path, data) {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to write file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn read_file(file_path: *const c_char, out_content: *mut datenlord_bytes) -> *mut datenlord_error {
    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };

    match fs::read(path) {
        Ok(content) => {
            unsafe {
                (*out_content).data = content.as_ptr();
                (*out_content).len = content.len();
            }
            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to read file".to_string()),
    }
}
