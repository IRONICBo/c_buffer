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
}