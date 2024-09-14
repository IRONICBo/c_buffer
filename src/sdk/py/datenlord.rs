use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use bytes::BytesMut;
use std::fs;
use crate::storage::localfs::LocalFS;
use crate::storage::fs_util::{CreateParam, RenameParam};
use crate::storage::virtualfs::{INum, VirtualFs};
use nix::sys::stat::SFlag;

#[pyclass]
struct DatenlordSDK {
    localfs: Arc<Mutex<LocalFS>>,
}

#[pymethods]
impl DatenlordSDK {
    #[new]
    fn new() -> PyResult<Self> {
        let localfs = LocalFS::new().unwrap();
        Ok(DatenlordSDK {
            localfs: Arc::new(Mutex::new(localfs)),
        })
    }

    fn exists(&self, dir_path: &str) -> PyResult<bool> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let localfs = sdk_ref.lock().unwrap();
            localfs.lookup(1000, 1000, 1, dir_path).await
        });
        Ok(result.is_ok())
    }

    fn mkdir(&self, dir_path: &str) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let param = CreateParam {
                parent: 34735213, // 示例 inode
                name: dir_path.to_string(),
                mode: 0o777,
                rdev: 0,
                uid: 1000,
                gid: 1000,
                node_type: SFlag::S_IFDIR,
                link: None,
            };
            let localfs = sdk_ref.lock().unwrap();
            localfs.mkdir(param).await
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to create directory"))
        }
    }

    fn deldir(&self, dir_path: &str, recursive: bool) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let localfs = sdk_ref.lock().unwrap();
            localfs.rmdir(1000, 1000, 1, dir_path).await // 示例 inode
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to remove directory"))
        }
    }

    fn rename_path(&self, src_path: &str, dest_path: &str) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let param = RenameParam {
                old_parent: 1,
                old_name: src_path.to_string(),
                new_parent: 1,
                new_name: dest_path.to_string(),
                flags: 0,
            };
            let localfs = sdk_ref.lock().unwrap();
            localfs.rename(1000, 1000, param).await
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to rename path"))
        }
    }

    fn copy_from_local_file(&self, local_file_path: &str, dest_file_path: &str, overwrite: bool) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let localfs = sdk_ref.lock().unwrap();
            if !overwrite && localfs.lookup(1000, 1000, 1, dest_file_path).await.is_ok() {
                return Err(());
            }

            match fs::read(local_file_path) {
                Ok(content) => localfs.write(1, 0, 0, &content, 0).await.map_err(|_| ()),
                Err(_) => Err(()),
            }
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to copy file"))
        }
    }

    fn copy_to_local_file(&self, src_file_path: &str, local_file_path: &str) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let mut buf = BytesMut::new();
            let localfs = sdk_ref.lock().unwrap();
            localfs.read(1, 0, 0, 1024, &mut buf).await.map_err(|_| ()) // 示例 inode 和最大读取字节数
        });

        match result {
            Ok(size) => {
                Ok(())
            }
            Err(_) => Err(pyo3::exceptions::PyOSError::new_err("Failed to copy file to local")),
        }
    }

    fn create_file(&self, file_path: &str) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let param = CreateParam {
                parent: 1,
                name: file_path.to_string(),
                mode: 0o644,
                rdev: 0,
                uid: 1000,
                gid: 1000,
                node_type: SFlag::S_IFREG,
                link: None,
            };

            let localfs = sdk_ref.lock().unwrap();
            localfs.mknod(param).await
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to create file"))
        }
    }

    fn stat(&self, file_path: &str) -> PyResult<(u64, u32, u32, u32, u32)> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let localfs = sdk_ref.lock().unwrap();
            localfs.getattr(1).await // 示例 inode
        });

        match result {
            Ok(attr) => {
                let file_stat = (
                    attr.1.size,  // 文件大小
                    attr.1.uid,   // 用户ID
                    attr.1.gid,   // 组ID
                    attr.1.nlink, // 硬链接数量
                    attr.1.rdev,  // 设备ID
                );
                Ok(file_stat)
            }
            Err(_) => Err(pyo3::exceptions::PyOSError::new_err("Failed to get file metadata")),
        }
    }

    fn write_file(&self, file_path: &str, content: Vec<u8>) -> PyResult<()> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let localfs = sdk_ref.lock().unwrap();
            localfs.write(34734588, 0, 0, &content, 0).await // 示例 inode
        });

        if result.is_ok() {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyOSError::new_err("Failed to write file"))
        }
    }

    fn read_file(&self, file_path: &str) -> PyResult<Vec<u8>> {
        let sdk_ref = &self.localfs;
        let rt = Runtime::new().unwrap();
        let mut buf = BytesMut::new();
        let result = rt.block_on(async {
            buf.reserve(1024);
            let localfs = sdk_ref.lock().unwrap();
            localfs.read(1, 0, 0, 1024, &mut buf).await.map_err(|_| ()) // 示例 inode
        });

        match result {
            Ok(size) =>  {
                Ok(Vec::from(&buf[..size]))
            }
            Err(_) => Err(pyo3::exceptions::PyOSError::new_err("Failed to read file")),
        }
    }
}

#[pyfunction]
fn init_sdk() -> PyResult<DatenlordSDK> {
    DatenlordSDK::new()
}

#[pymodule]
fn datenlord(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DatenlordSDK>()?;
    m.add_function(wrap_pyfunction!(init_sdk, m)?)?;
    Ok(())
}
