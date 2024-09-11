use async_trait::async_trait;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::Mutex;
use crate::{DatenLordError, DatenLordResult, FileAttr, INum, CreateParam, SetAttrParam, RenameParam, DirEntry, StatFsParam, FileLockParam};
use bytes::BytesMut;
use std::os::unix::fs::PermissionsExt;
use std::ffi::OsStr;
use std::collections::HashMap;
use std::sync::Arc;

/// LocalFS 实现了 VirtualFs trait
#[derive(Debug)]
pub struct LocalFS {
    root: PathBuf,
    backend: Arc<BackendImpl>,
    open_files: Arc<Mutex<HashMap<u64, u64>>>, // 存储打开的文件句柄 (inode -> file handle)
}

impl LocalFS {
    /// 创建一个新的 LocalFS 实例，使用给定的 root 目录初始化文件系统
    pub fn new(root: &str) -> DatenLordResult<Self> {
        let root_path = PathBuf::from(root);
        let backend = Arc::new(tmp_fs_backend()?);

        Ok(Self {
            root: root_path,
            backend,
            open_files: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// 将 inode 转换为本地文件系统的路径
    fn inode_to_path(&self, ino: u64) -> PathBuf {
        self.root.join(ino.to_string())
    }
}

#[async_trait]
impl VirtualFs for LocalFS {
    /// 初始化文件系统
    async fn init(&self) -> DatenLordResult<()> {
        // 检查根目录是否存在
        if !self.root.exists() {
            fs::create_dir_all(&self.root)?;
        }
        Ok(())
    }

    /// 销毁文件系统
    async fn destroy(&self) -> DatenLordResult<()> {
        // 删除根目录及其所有内容
        fs::remove_dir_all(&self.root)?;
        Ok(())
    }

    /// Lookup：通过名称查找目录项并获取其属性
    async fn lookup(
        &self,
        _uid: u32,
        _gid: u32,
        _parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let path = self.root.join(name);
        let metadata = fs::metadata(&path)?;
        let attr = FileAttr::from(metadata); // 假设我们有 FileAttr::from 这样的函数
        let ino = metadata.ino();
        Ok((Duration::from_secs(1), attr, ino))
    }

    /// 获取文件属性
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)> {
        let path = self.inode_to_path(ino);
        let metadata = fs::metadata(&path)?;
        let attr = FileAttr::from(metadata);
        Ok((Duration::from_secs(1), attr))
    }

    /// 设置文件属性
    async fn setattr(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FileAttr)> {
        let path = self.inode_to_path(ino);
        let mut metadata = fs::metadata(&path)?;

        if let Some(mode) = param.mode {
            let mut permissions = metadata.permissions();
            permissions.set_mode(mode);
            fs::set_permissions(&path, permissions)?;
        }

        if let Some(size) = param.size {
            fs::File::create(&path)?.set_len(size)?;
        }

        let attr = FileAttr::from(metadata);
        Ok((Duration::from_secs(1), attr))
    }

    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>> {
        let path = self.inode_to_path(ino);
        let target = fs::read_link(&path)?;
        Ok(target.as_os_str().as_bytes().to_vec())
    }

    async fn open(&self, _uid: u32, _gid: u32, ino: u64, _flags: u32) -> DatenLordResult<u64> {
        let mut open_files = self.open_files.lock().await;
        let fh = ino;
        open_files.insert(ino, fh);
        Ok(fh)
    }

    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        buf: &mut BytesMut,
    ) -> DatenLordResult<usize> {
        // 使用 backend 读取文件数据
        let result = self.backend.read(ino, fh, offset, size as usize).await?;
        buf.extend_from_slice(&result);
        Ok(result.len())
    }

    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
    ) -> DatenLordResult<()> {
        self.backend.write(ino, fh, offset as u64, data).await?;
        Ok(())
    }

    async fn unlink(&self, _uid: u32, _gid: u32, _parent: INum, name: &str) -> DatenLordResult<()> {
        let path = self.root.join(name);
        fs::remove_file(&path)?;
        Ok(())
    }

    async fn mkdir(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let path = self.root.join(&param.name);
        fs::create_dir(&path)?;
        let metadata = fs::metadata(&path)?;
        let attr = FileAttr::from(metadata);
        Ok((Duration::from_secs(1), attr, metadata.ino()))
    }

    async fn rename(&self, _uid: u32, _gid: u32, param: RenameParam) -> DatenLordResult<()> {
        let old_path = self.root.join(&param.oldname);
        let new_path = self.root.join(&param.newname);
        fs::rename(old_path, new_path)?;
        Ok(())
    }

    async fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> DatenLordResult<()> {
        let mut open_files = self.open_files.lock().await;
        open_files.remove(&ino);
        Ok(())
    }

    async fn statfs(&self, _uid: u32, _gid: u32, ino: u64) -> DatenLordResult<StatFsParam> {
        let path = self.inode_to_path(ino);
        let stat = fs::metadata(&path)?;
        let param = StatFsParam::from(stat);
        Ok(param)
    }
}
