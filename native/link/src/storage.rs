//! 持久化抽象：本机身份种子 + 已配对设备名册。
//!
//! 宿主各自实现：冻结的 server 经引擎落在 `link_devices` 表与
//! `config` 表；agent 落在自己的私有状态文件。[`crate::LinkManager`] 只依赖本 trait。

use async_trait::async_trait;

use super::error::LinkResult;
use super::types::{PeerCandidate, PeerInfo, PeerRecord};

/// 设备互联持久化后端。
///
/// 所有方法都应是幂等、并发安全的；`LinkManager` 会在多个任务里同时调用。
#[async_trait]
pub trait LinkStorage: Send + Sync + 'static {
    /// 读取已持久化的本机身份私钥 seed（32 字节）；从未生成过返回 `None`。
    async fn load_identity_seed(&self) -> LinkResult<Option<[u8; 32]>>;

    /// 持久化新生成的身份私钥 seed。
    async fn save_identity_seed(&self, seed: &[u8; 32]) -> LinkResult<()>;

    /// 落库（新增或覆盖）一条已配对设备。
    async fn upsert(&self, record: &PeerRecord) -> LinkResult<()>;

    /// 读取全部已配对设备（最近活跃降序）。
    async fn list(&self) -> LinkResult<Vec<PeerRecord>>;

    /// 按指纹读取单台设备。
    async fn get(&self, fingerprint: &str) -> LinkResult<Option<PeerRecord>>;

    /// 解除配对。返回是否删到行。
    async fn remove(&self, fingerprint: &str) -> LinkResult<bool>;

    /// 刷新最近活跃时间（Unix 秒）。
    async fn touch(&self, fingerprint: &str, at: i64) -> LinkResult<()>;

    /// 覆盖一台已配对设备的候选端点。返回是否命中已配对设备。
    async fn update_candidates(
        &self,
        fingerprint: &str,
        candidates: &[PeerCandidate],
    ) -> LinkResult<bool>;

    /// 记录对端经已认证链路自报的信息。默认实现丢弃（没有对应存储列的后端，
    /// 如引擎 `link_devices` 表——对端信息只在本次运行内有意义）。
    async fn set_peer_info(&self, fingerprint: &str, info: &PeerInfo) -> LinkResult<bool> {
        let _ = (fingerprint, info);
        Ok(false)
    }
}

/// 仅供本 crate 测试的内存后端。
#[cfg(test)]
pub(crate) mod memory {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::LinkStorage;
    use crate::error::{LinkError, LinkResult};
    use crate::types::{PeerCandidate, PeerInfo, PeerRecord};

    #[derive(Default)]
    pub(crate) struct MemoryLinkStorage {
        seed: Mutex<Option<[u8; 32]>>,
        peers: Mutex<HashMap<String, PeerRecord>>,
        pub(crate) fail_metadata_writes: std::sync::atomic::AtomicBool,
    }

    fn poisoned<T>(_: std::sync::PoisonError<T>) -> LinkError {
        LinkError::Store("memory storage poisoned".into())
    }

    #[async_trait]
    impl LinkStorage for MemoryLinkStorage {
        async fn load_identity_seed(&self) -> LinkResult<Option<[u8; 32]>> {
            Ok(*self.seed.lock().map_err(poisoned)?)
        }

        async fn save_identity_seed(&self, seed: &[u8; 32]) -> LinkResult<()> {
            *self.seed.lock().map_err(poisoned)? = Some(*seed);
            Ok(())
        }

        async fn upsert(&self, record: &PeerRecord) -> LinkResult<()> {
            self.peers
                .lock()
                .map_err(poisoned)?
                .insert(record.fingerprint.clone(), record.clone());
            Ok(())
        }

        async fn list(&self) -> LinkResult<Vec<PeerRecord>> {
            let mut all: Vec<PeerRecord> = self
                .peers
                .lock()
                .map_err(poisoned)?
                .values()
                .cloned()
                .collect();
            all.sort_by_key(|record| std::cmp::Reverse(record.last_seen_at));
            Ok(all)
        }

        async fn get(&self, fingerprint: &str) -> LinkResult<Option<PeerRecord>> {
            Ok(self
                .peers
                .lock()
                .map_err(poisoned)?
                .get(fingerprint)
                .cloned())
        }

        async fn remove(&self, fingerprint: &str) -> LinkResult<bool> {
            Ok(self
                .peers
                .lock()
                .map_err(poisoned)?
                .remove(fingerprint)
                .is_some())
        }

        async fn touch(&self, fingerprint: &str, at: i64) -> LinkResult<()> {
            if let Some(record) = self.peers.lock().map_err(poisoned)?.get_mut(fingerprint) {
                record.last_seen_at = at;
            }
            Ok(())
        }

        async fn update_candidates(
            &self,
            fingerprint: &str,
            candidates: &[PeerCandidate],
        ) -> LinkResult<bool> {
            if self
                .fail_metadata_writes
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(LinkError::Store("metadata write rejected".into()));
            }
            let mut peers = self.peers.lock().map_err(poisoned)?;
            match peers.get_mut(fingerprint) {
                Some(record) => {
                    record.candidates = candidates.to_vec();
                    Ok(true)
                }
                None => Ok(false),
            }
        }

        async fn set_peer_info(&self, fingerprint: &str, info: &PeerInfo) -> LinkResult<bool> {
            if self
                .fail_metadata_writes
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(LinkError::Store("metadata write rejected".into()));
            }
            let mut peers = self.peers.lock().map_err(poisoned)?;
            match peers.get_mut(fingerprint) {
                Some(record) => {
                    record.info = info.clone();
                    Ok(true)
                }
                None => Ok(false),
            }
        }
    }
}
