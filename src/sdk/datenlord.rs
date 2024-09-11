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
