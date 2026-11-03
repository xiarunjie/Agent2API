//! 进程本地会话粘滞。管理读取不续期，清除后在途请求不得把旧绑定写回来。
//!
//! ── 为什么「失效」必须留痕（issue #2）────────────────────────────
//! 粘性是一层**静默**优化：命中与否只体现在 `requests.route.stickyHit` 这一个
//! 布尔上，失效时请求照样成功、账号照样轮换。于是「绑定根本没写进去」和
//! 「写了但每次都被淘汰」在外部看起来完全一样 —— 上游 #443 的 655 条请求
//! 0 次命中就是这么被发现的：没有任何一条日志能指出哪一侧断了。
//!
//! 因此本模块把**状态变化**（不是每次命中）记进日志库，事件名取排查时可以直接
//! 搜的词：`account-switch`（同一会话换了账号）、`identity-changed`（同一把会话键
//! 的来源变了）、`evicted-ttl`（过期释放）、`evicted-capacity`（表满淘汰）、
//! `cleared`（人工/设置清空）、`skipped-*`（写入被跳过的原因）。
//!
//! ── 为什么事件要先收集再打日志 ──────────────────────────────
//! 日志落库可能碰 SQLite 的锁，而本模块的锁只该护住那张内存表。所有事件都在
//! 释放 `table` 之后才写日志（见各方法里的 `let event = { … };` 形状）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::server::logging;

/// 绑定表的上限（活跃会话数）。到顶后淘汰最久未用的那条。
const MAX_ENTRIES: usize = 10_000;

/// 「会话键为空，粘性不生效」只提示一次的开关。
///
/// 这条不是逐请求的事实（同一台机器上可能条条如此），逐条记录会把日志页刷满，
/// 反而看不到真正稀缺的失效事件；而完全不提又正是 issue #2 里最难排的那种
/// 「静默不生效」。取「每个进程第一次」—— 有就一眼看见，没有就闭嘴。
static EMPTY_KEY_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct StickyEntry {
    id: String,
    account_id: String,
    model: String,
    source: String,
    created_at: i64,
    last_seen: i64,
    ttl_ms: i64,
    hits: u64,
}

#[derive(Default)]
struct StickyTable {
    entries: HashMap<String, StickyEntry>,
    generation: u64,
}

#[derive(Default)]
pub(super) struct StickySessions {
    table: Mutex<StickyTable>,
}

/// 需要落日志的状态变化（在释放表锁之后统一写）。
enum Event {
    /// 同一会话换了承载账号（前一个账号不可用 / 失败顺延过来）。
    AccountSwitch { preview: String, from: String, to: String, model: String },
    /// 同一把会话键的来源标签变了（例如从内容哈希换成了客户端显式标识）。
    IdentityChanged { preview: String, from: String, to: String, model: String },
    /// 表满淘汰。
    CapacityEvicted { preview: String, account_id: String, capacity: usize },
    /// 过期释放（只记「恰好被这次查找撞上」的那条，不做批量统计 —— 批量统计
    /// 会在每次惰性清理时刷一行，量级由流量决定，不是有价值的事件）。
    EvictedTtl { preview: String, account_id: String, ttl_ms: i64 },
    /// 人工 / 设置变更清空。
    Cleared { removed: usize, scope: String },
    /// 写入被丢弃（本请求期间清空过，代次已变）。
    StaleGeneration,
}

impl Event {
    fn log(self) {
        match self {
            Self::AccountSwitch { preview, from, to, model } => logging::log(
                "[Upstream]",
                &format!(
                    "会话粘性切换账号（account-switch）：会话 {preview} 的绑定由账号 {from} 改为 {to}（模型 {model}）"
                ),
            ),
            Self::IdentityChanged { preview, from, to, model } => logging::log(
                "[Upstream]",
                &format!(
                    "会话粘性标识来源变化（identity-changed）：会话 {preview} 的键来源由 {from} 变为 {to}（模型 {model}），绑定延续到新来源"
                ),
            ),
            Self::CapacityEvicted { preview, account_id, capacity } => logging::log(
                "[Upstream]",
                &format!(
                    "会话粘性容量淘汰（evicted-capacity）：绑定表已满 {capacity} 条，移除最久未用的会话 {preview}（账号 {account_id}）"
                ),
            ),
            Self::EvictedTtl { preview, account_id, ttl_ms } => logging::log(
                "[Upstream]",
                &format!(
                    "会话粘性过期（evicted-ttl）：会话 {preview} 超过 {} 分钟没有再来往，绑定账号 {account_id} 已释放，本次重新选路",
                    ttl_ms / 60_000
                ),
            ),
            Self::Cleared { removed, scope } => logging::log(
                "[Upstream]",
                &format!("会话粘性清除（cleared）：{scope}，移除 {removed} 条绑定"),
            ),
            Self::StaleGeneration => logging::log(
                "[Upstream]",
                "会话粘性写入被丢弃（skipped-stale-generation）：本请求执行期间绑定表被清空过，不把旧绑定写回来",
            ),
        }
    }
}

/// 日志里的会话标识：取条目 id 前 12 位（与 `/api/routing/sticky` 的
/// `keyPreview` 同一形态）。不落原始会话键 —— 那可能是客户端的会话标识原文。
fn preview(id: &str) -> String {
    format!("{}…", &id[..id.len().min(12)])
}

pub(super) fn entry_id(key: &str, model: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(model.trim().to_ascii_lowercase().as_bytes());
    hasher.update(b"\0");
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

impl StickySessions {
    pub(super) fn generation(&self) -> u64 {
        self.table.lock().map(|table| table.generation).unwrap_or(0)
    }

    pub(super) fn lookup(&self, key: &str, model: &str, now: i64, ttl_ms: i64) -> Option<String> {
        if key.is_empty() || ttl_ms <= 0 { return None; }
        let id = entry_id(key, model);
        let mut table = self.table.lock().ok()?;
        // 先判「这次要查的这条是不是已经到了**它自己**的 TTL」，再统一清理：
        // 过期事件要归到「谁回来得太晚」这条会话上，批量清理那一步只知道条数。
        let expired = table
            .entries
            .get(&id)
            .filter(|entry| now.saturating_sub(entry.last_seen) > entry.ttl_ms)
            .map(|entry| (entry.account_id.clone(), entry.ttl_ms));
        table.entries.retain(|_, entry| now.saturating_sub(entry.last_seen) <= entry.ttl_ms);
        if let Some((account_id, own_ttl)) = expired {
            drop(table);
            Event::EvictedTtl {
                preview: preview(&id),
                account_id,
                ttl_ms: own_ttl,
            }
            .log();
            return None;
        }
        let entry = table.entries.get_mut(&id)?;
        // 当前档位比写下时更短（例如从 session 切到 auto）时，按**当前**档位
        // 判失效，但**不删**这条绑定：档位是「这会儿还愿不愿意沿用」的策略，
        // 不是「销毁别人记录」的指令 —— 切回更长的档位后，只要它自己的 TTL
        // 还没到，绑定就该继续有效（管理页的读取同理，见 `list`）。
        if now.saturating_sub(entry.last_seen) > ttl_ms {
            return None;
        }
        // 只续「最近使用时刻」，**不改**写下时的 TTL：读取不是重写。否则一条
        // 6 小时档的绑定在 auto 档下被命中一次就缩成 5 分钟，切回 session 后
        // 到点被清掉 —— 读操作又把别人的记录改短了。
        entry.last_seen = now;
        entry.hits = entry.hits.saturating_add(1);
        Some(entry.account_id.clone())
    }

    pub(super) fn bind(
        &self, key: &str, model: &str, account_id: &str, source: &str,
        now: i64, ttl_ms: i64, generation: u64,
    ) {
        if key.is_empty() {
            // 逐条记录会刷屏，但第一次必须说：这正是「粘性从来没生效」的最常见
            // 原因（请求既没有显式会话标识，也没有带 messages 的会话特征）。
            if !EMPTY_KEY_WARNED.swap(true, Ordering::Relaxed) {
                logging::log(
                    "[Upstream]",
                    "会话粘性不生效（skipped-empty-key）：有请求既没有显式会话标识（AH-Thread-Id / X-Session-Id / prompt_cache_key 等）、也没有可用的首条用户消息，无法建立会话键；本条每个进程只提示一次",
                );
            }
            return;
        }
        if account_id.is_empty() || ttl_ms <= 0 {
            // ttl 为 0 是用户的显式选择（off / turn 档），不是故障。
            return;
        }
        let mut event: Option<Event> = None;
        {
            let Ok(mut table) = self.table.lock() else { return };
            if table.generation != generation {
                event = Some(Event::StaleGeneration);
            } else {
                table.entries.retain(|_, entry| now.saturating_sub(entry.last_seen) <= entry.ttl_ms);
                let id = entry_id(key, model);
                if table.entries.len() >= MAX_ENTRIES && !table.entries.contains_key(&id) {
                    // 先取出「最久未用」的键与账号（借用在这里就结束），再删。
                    let oldest = table
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_seen)
                        .map(|(key, entry)| (key.clone(), entry.account_id.clone()));
                    if let Some((oldest_key, oldest_account)) = oldest {
                        table.entries.remove(&oldest_key);
                        event = Some(Event::CapacityEvicted {
                            preview: preview(&oldest_key),
                            account_id: oldest_account,
                            capacity: MAX_ENTRIES,
                        });
                    }
                }
                // `or_insert_with` 会吞掉「之前是谁」这个事实，而 account-switch /
                // identity-changed 判的就是它 —— 先取一份旧值再覆盖。
                let previous = table
                    .entries
                    .get(&id)
                    .map(|entry| (entry.account_id.clone(), entry.source.clone()));
                let entry = table.entries.entry(id.clone()).or_insert_with(|| StickyEntry {
                    id,
                    account_id: account_id.to_string(),
                    model: model.to_string(),
                    source: source.to_string(),
                    created_at: now,
                    last_seen: now,
                    ttl_ms,
                    hits: 0,
                });
                match previous {
                    Some((previous_account, previous_source)) => {
                        if previous_account != account_id {
                            event = Some(Event::AccountSwitch {
                                preview: preview(&entry.id),
                                from: previous_account,
                                to: account_id.to_string(),
                                model: model.to_string(),
                            });
                        } else if previous_source != source {
                            event = Some(Event::IdentityChanged {
                                preview: preview(&entry.id),
                                from: previous_source,
                                to: source.to_string(),
                                model: model.to_string(),
                            });
                        }
                    }
                    None => {}
                }
                entry.account_id = account_id.to_string();
                entry.source = source.to_string();
                entry.last_seen = now;
                entry.ttl_ms = ttl_ms;
            }
        }
        if let Some(event) = event {
            event.log();
        }
    }

    /// 管理页读取：**不续期**（`last_seen` / `hits` 一律不动），只按每条绑定
    /// **自己的** TTL 清过期项。
    ///
    /// 为什么清理用 `entry.ttl_ms` 而不是查询传进来的 `ttl_ms`：那是「这次查询
    /// 想看的档位」，不是这条绑定写下时的档位。用手上的档位淘汰别人的记录，
    /// 会在「切到 auto 看一眼、再切回 session」之后把本该还有效的 6 小时绑定
    /// 全部清掉 —— 读一次管理页不该有副作用。
    pub(super) fn list(&self, model: Option<&str>, now: i64, ttl_ms: i64) -> Vec<Value> {
        let Ok(mut table) = self.table.lock() else { return Vec::new() };
        if ttl_ms <= 0 { return Vec::new(); }
        table.entries.retain(|_, entry| now.saturating_sub(entry.last_seen) <= entry.ttl_ms);
        let mut entries: Vec<&StickyEntry> = table.entries.values()
            .filter(|entry| model.map_or(true, |model| entry.model.eq_ignore_ascii_case(model)))
            .collect();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.last_seen));
        entries.into_iter().map(|entry| json!({
            "id": entry.id,
            "keyPreview": preview(&entry.id),
            "accountId": entry.account_id,
            "model": entry.model,
            "source": entry.source,
            "createdAt": entry.created_at,
            "lastSeen": entry.last_seen,
            "expiresAt": entry.last_seen + entry.ttl_ms,
            "remainingMs": (entry.last_seen + entry.ttl_ms - now).max(0),
            "hits": entry.hits,
        })).collect()
    }

    pub(super) fn clear(&self, id: Option<&str>, model: Option<&str>) -> usize {
        let Ok(mut table) = self.table.lock() else { return 0 };
        let before = table.entries.len();
        table.entries.retain(|key, entry| {
            !(id.map_or(true, |id| id == key)
                && model.map_or(true, |model| entry.model.eq_ignore_ascii_case(model)))
        });
        let removed = before - table.entries.len();
        // 清空会推进代次：清空之前发起的那些请求**不得**把旧绑定写回来
        // （见 `bind` 的代次判定），否则「清空」在并发下只是暂时的。
        table.generation = table.generation.wrapping_add(1);
        drop(table);
        if removed > 0 {
            Event::Cleared {
                removed,
                scope: match (id, model) {
                    (Some(id), Some(model)) => format!("指定绑定 {id}（模型 {model}）"),
                    (Some(id), None) => format!("指定绑定 {id}"),
                    (None, Some(model)) => format!("模型 {model} 的全部绑定"),
                    (None, None) => "全部绑定".to_string(),
                },
            }
            .log();
        }
        removed
    }
}
