//! 设备互联（`fluxdown_link`）的引擎数据库存储后端。
//!
//! 互联协议本体（身份 / 配对 / mDNS / 传输）住在独立的 `fluxdown_link` crate，与宿主无关；
//! 引擎只负责把它的持久化抽象 [`LinkStorage`] 落到 `config` 表（身份私钥 seed）与
//! `link_devices` 表（已配对名册）。冻结的 `native/server` 用它构造
//! `LinkManager`；当前 agent 使用独立状态存储。仅 `link` feature 下编译。

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use fluxdown_link::{LinkError, LinkResult, LinkStorage, PeerCandidate, PeerRecord};

use crate::db::{Db, DbError, LinkDeviceRow};

/// 引擎 `config` 表中持久化身份私钥 seed（base64 的 32 字节）的键名。
pub const IDENTITY_CONFIG_KEY: &str = "link.identity_secret";

fn store_error(error: DbError) -> LinkError {
    LinkError::Store(format!("{error:#}"))
}

/// 绑定到引擎数据库的 [`LinkStorage`] 实现：处理 `candidates` 的 JSON 序列化与
/// 空 platform 的 `Option` 归一。
#[derive(Clone)]
pub struct DbLinkStorage {
    db: Db,
}

impl DbLinkStorage {
    /// 绑定到引擎数据库。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl LinkStorage for DbLinkStorage {
    async fn load_identity_seed(&self) -> LinkResult<Option<[u8; 32]>> {
        let stored = self
            .db
            .get_config(IDENTITY_CONFIG_KEY)
            .await
            .map_err(store_error)?;
        Ok(stored
            .and_then(|b64| B64.decode(b64.trim()).ok())
            .and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok()))
    }

    async fn save_identity_seed(&self, seed: &[u8; 32]) -> LinkResult<()> {
        self.db
            .set_config(IDENTITY_CONFIG_KEY, &B64.encode(seed))
            .await
            .map_err(store_error)
    }

    async fn upsert(&self, record: &PeerRecord) -> LinkResult<()> {
        let candidates_json =
            serde_json::to_string(&record.candidates).unwrap_or_else(|_| "[]".to_string());
        self.db
            .link_upsert_device(
                &record.fingerprint,
                &record.identity_pub,
                &record.name,
                record.platform.as_deref().unwrap_or(""),
                &record.link_secret,
                &candidates_json,
                record.paired_at,
                record.last_seen_at,
            )
            .await
            .map_err(store_error)
    }

    async fn list(&self) -> LinkResult<Vec<PeerRecord>> {
        let rows = self.db.link_load_devices().await.map_err(store_error)?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn get(&self, fingerprint: &str) -> LinkResult<Option<PeerRecord>> {
        Ok(self
            .db
            .link_load_device(fingerprint)
            .await
            .map_err(store_error)?
            .map(row_to_record))
    }

    async fn remove(&self, fingerprint: &str) -> LinkResult<bool> {
        self.db
            .link_delete_device(fingerprint)
            .await
            .map_err(store_error)
    }

    async fn touch(&self, fingerprint: &str, at: i64) -> LinkResult<()> {
        self.db
            .link_touch_device(fingerprint, at)
            .await
            .map_err(store_error)
    }

    async fn update_candidates(
        &self,
        fingerprint: &str,
        candidates: &[PeerCandidate],
    ) -> LinkResult<bool> {
        let candidates_json =
            serde_json::to_string(candidates).unwrap_or_else(|_| "[]".to_string());
        self.db
            .link_update_candidates(fingerprint, &candidates_json)
            .await
            .map_err(store_error)
    }
}

/// 把持久化行映射为强类型 [`PeerRecord`]。`candidates` JSON 解析失败退化为空列表
/// （不让单条坏数据毒死整份名册加载，与引擎其他「坏行降级」纪律一致）。
fn row_to_record(row: LinkDeviceRow) -> PeerRecord {
    let candidates: Vec<PeerCandidate> =
        serde_json::from_str(&row.candidates_json).unwrap_or_default();
    PeerRecord {
        fingerprint: row.fingerprint,
        identity_pub: row.identity_pub,
        name: row.name,
        platform: if row.platform.is_empty() {
            None
        } else {
            Some(row.platform)
        },
        link_secret: row.link_secret,
        candidates,
        paired_at: row.paired_at,
        last_seen_at: row.last_seen_at,
        info: fluxdown_link::PeerInfo::default(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use fluxdown_link::{PeerInfo, TransportKind};

    use super::*;

    async fn storage() -> DbLinkStorage {
        let url = format!(
            "sqlite:file:linkstore_{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4().simple()
        );
        DbLinkStorage::new(Db::connect(&url).await.unwrap())
    }

    fn record(fingerprint: &str) -> PeerRecord {
        PeerRecord {
            fingerprint: fingerprint.to_string(),
            identity_pub: vec![9u8; 32],
            name: "phone".into(),
            platform: None,
            link_secret: vec![3u8; 32],
            candidates: vec![PeerCandidate {
                kind: TransportKind::Direct,
                address: "10.0.0.2:17800".into(),
            }],
            paired_at: 10,
            last_seen_at: 20,
            info: PeerInfo::default(),
        }
    }

    #[tokio::test]
    async fn identity_seed_persists_across_handles() {
        let store = storage().await;
        assert!(store.load_identity_seed().await.unwrap().is_none());
        store.save_identity_seed(&[5u8; 32]).await.unwrap();
        assert_eq!(store.load_identity_seed().await.unwrap(), Some([5u8; 32]));
    }

    #[tokio::test]
    async fn roster_roundtrip_normalizes_empty_platform_and_updates_candidates() {
        let store = storage().await;
        store.upsert(&record("fp-1")).await.unwrap();

        let loaded = store.get("fp-1").await.unwrap().unwrap();
        assert_eq!(loaded.platform, None);
        assert_eq!(loaded.candidates.len(), 1);

        let fresh = vec![PeerCandidate {
            kind: TransportKind::Direct,
            address: "https://nas.example.com/fluxdown".into(),
        }];
        assert!(store.update_candidates("fp-1", &fresh).await.unwrap());
        assert!(!store.update_candidates("missing", &fresh).await.unwrap());
        assert_eq!(store.list().await.unwrap()[0].candidates, fresh);

        assert!(store.remove("fp-1").await.unwrap());
        assert!(!store.remove("fp-1").await.unwrap());
    }
}
