use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use pyo3::wrap_pyfunction;
use tokio::runtime::Runtime;
use std::sync::{Arc, Mutex};
use bytes::BytesMut;
use crate::storage::fs_util::{CreateParam, RenameParam};
use crate::storage::localfs::LocalFS;
use crate::storage::virtualfs::VirtualFs;

#[pyclass]
struct DatenlordSdk {
    localfs: Arc<Mutex<LocalFS>>, // 保存 `LocalFS` 实例
}

#[pymethods]
impl DatenlordSdk {
    #[new]
    fn new(config: &str) -> PyResult<Self> {
        let localfs = LocalFS::new().unwrap();
        Ok(Self {
            localfs: Arc::new(Mutex::new(localfs)),
        })
    }

    fn mkdir(&self, dir_path: &str) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let param = CreateParam {
                parent: 1,
                name: dir_path.to_string(),
                mode: 0o755,
                rdev: 0,
                uid: 1000,
                gid: 1000,
                node_type: nix::sys::stat::SFlag::S_IFDIR,
                link: None,
            };

            let mut localfs = self.localfs.lock().unwrap();
            localfs.mkdir(param).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to create directory"))
        })
    }

    fn exists(&self, dir_path: &str) -> PyResult<bool> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut localfs = self.localfs.lock().unwrap();
            localfs.lookup(1000, 1000, 1, dir_path).await.map(|_| true).or(Ok(false))
        })
    }

    fn delete_dir(&self, dir_path: &str, recursive: bool) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut localfs = self.localfs.lock().unwrap();
            if recursive {
                localfs.rmdir(1000, 1000, 1, dir_path).await
            } else {
                localfs.unlink(1000, 1000, 1, dir_path).await
            }.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to remove directory"))
        })
    }

    fn rename_path(&self, src_path: &str, dest_path: &str) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let param = RenameParam {
                old_parent: 1,
                old_name: src_path.to_string(),
                new_parent: 1,
                new_name: dest_path.to_string(),
                flags: 0,
            };

            let mut localfs = self.localfs.lock().unwrap();
            localfs.rename(1000, 1000, param).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to rename path"))
        })
    }

    fn copy_from_local_file(&self, overwrite: bool, local_file_path: &str, dest_file_path: &str) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut localfs = self.localfs.lock().unwrap();

            if !overwrite && localfs.lookup(1000, 1000, 1, dest_file_path).await.is_ok() {
                return Err(pyo3::exceptions::PyRuntimeError::new_err("Destination file exists"));
            }

            match std::fs::read(local_file_path) {
                Ok(content) => {
                    localfs.write(1, 0, 0, &content, 0).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to copy file"))
                }
                Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Failed to read local file")),
            }
        })
    }

    fn copy_to_local_file(&self, src_file_path: &str, local_file_path: &str) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut buf = BytesMut::new();
            let mut localfs = self.localfs.lock().unwrap();

            localfs.read(1, 0, 0, 1024, &mut buf).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to read remote file"))?;

            std::fs::write(local_file_path, &buf).map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to write local file"))
        })
    }

    fn read_file<'py>(&self, py: Python<'py>, file_path: &str) -> PyResult<&'py PyBytes> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut buf = BytesMut::new();
            let mut localfs = self.localfs.lock().unwrap();

            localfs.read(1, 0, 0, 1024, &mut buf).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to read file"))?;

            Ok(PyBytes::new(py, &buf))
        })
    }

    fn write_file(&self, file_path: &str, content: &[u8]) -> PyResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut localfs = self.localfs.lock().unwrap();
            localfs.write(1, 0, 0, content, 0).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Failed to write file"))
        })
    }
}

#[pyfunction]
fn init_sdk(config: &str) -> PyResult<DatenlordSdk> {
    DatenlordSdk::new(config)
}

#[pymodule]
fn datenlord(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DatenlordSdk>()?;
    m.add_function(wrap_pyfunction!(init_sdk, m)?)?;
    Ok(())
}
