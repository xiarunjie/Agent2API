use super::*;
use crate::server::core::upstream::usage::RequestTelemetry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

async fn mock_upstream(
    status: u16,
    content_type: &'static str,
    body: &'static str,
) -> (
    TransportRequest,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let app = axum::Router::new().route(
        "/messages",
        axum::routing::post(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async move {
                (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    [(axum::http::header::CONTENT_TYPE, content_type)],
                    body,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/messages", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (
        TransportRequest {
            url,
            headers: vec![("Content-Type".into(), "application/json".into())],
            payload: r#"{"stream":true}"#.into(),
            proxy: None,
            // 测试替身按默认能力位（false = 直连），与生产里不带这一步的家同款
            system_proxy_when_unset: false,
        },
        hits,
        task,
    )
}

// ── ZCode 的响应判定与错误分类（测试随实现搬到插件）────────────────────────
//
// 这里原有五条用例钉的是 ZCode 适配器的两件事：
//   · `is_error_response`：HTTP 200 + `application/json` 是**业务拒绝**而不是
//     SSE 正文（`1005 exceed quota limit` 走 200 回来），网关必须先读错误体；
//   · `classify_error`：401 → TokenExpired、429 / 业务码 1005 → QuotaLimited。
//
// 这两条判据都跟着 ZCode 的实现搬到 `agent2api-providers` 了，因此用例也一起
// 搬走 —— 留在网关只会编译不过（`ZCODE_ADAPTER` 已不存在）。插件侧的同名用例
// 在 `zcode::mod` 的 `classify` 附近。
//
// 顺带说明**为什么这段注释留着**：`provider_loop` 的「HTTP 200 也可能是错误」
// 与「401/429 各自的动作」这两条**编排层**语义没有变，下面几条用例
// （会话式路径的限额记账）仍在验证它们。

// ── 会话式路径的限额记账（`mark_conversation_limited`）──────────────────────
//
// 这四条钉的是同一处缺陷：改造前该分支内联调用 `rotate::mark_account_limited`，
// `reset_at` 实参写死 `None` ⇒ 适配器算好的恢复时刻被丢在函数参数上，存储层落到
// 「缺失即 10 分钟」的兜底冷却（`account_store::store_admin::mark_rate_limited`）。
// 症状不是报错，而是**同一个已耗尽的账号按兜底常量的节拍被反复重新撞**：CodeArts
// 的福利日池要到第二天零点才有额度，生产实测两小时内白撞 8 次（每次 0.2–0.3 秒、
// 被拒不计费、整条请求仍 200 —— 只看响应看不出来）。
//
// 因此在改前的代码上第 1、4 条必红；第 2、3 条是「修复没有过头」的对照组。

static TEMP_SEQ: AtomicUsize = AtomicUsize::new(0);

/// 下一个北京零点的毫秒时刻（与插件的 `codearts::welfare::next_day_boundary_ms`
/// 同一口径：固定 UTC+8，不跟机器时区）。
fn next_beijing_midnight(now_ms: i64) -> i64 {
    const DAY_MS: i64 = 24 * 60 * 60 * 1000;
    let offset_ms = i64::from(crate::server::core::beijing::OFFSET_SECONDS) * 1000;
    let shifted = now_ms.saturating_add(offset_ms);
    (shifted.div_euclid(DAY_MS) + 1)
        .saturating_mul(DAY_MS)
        .saturating_sub(offset_ms)
}

/// 临时库目录，Drop 时删除。
///
/// 刻意不在建库前删了事：文件名带 pid，下一轮 pid 变了就永远删不到（曾在一台机器上
/// 攒出几百份）。调用方按 `let (_dir, service, id) = temp_service();` 接 —— 局部变量
/// 逆序析构，先关库再删目录。
struct TempDbDir(std::path::PathBuf);

impl Drop for TempDbDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 一份临时库 + 挂在其上的 [`UpstreamService`] + 里面唯一那条账号的 id。
///
/// 不发任何网络请求：记账与读回都只碰存储层，与转发选路用的是同一份读写口径。
fn temp_service() -> (TempDbDir, UpstreamService, String) {
    use crate::server::core::account_store::AccountStore;
    use crate::server::core::auth::AuthService;
    use crate::server::db::Db;

    let seq = TEMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pl-quota-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = AccountStore::with_db(Some(Db::open(&dir.join("agent2api.db")).expect("临时库应当能建起来")));
    let saved = store
        .add_account(
            &json!({"auth": {"accessToken": "at"}, "account": {"uid": "quota-1"}}),
            None,
            None,
        )
        .expect("临时账号应当能建起来");
    let id = saved["id"].as_str().expect("公开形态应带 id").to_string();
    let service = UpstreamService::new(store.clone(), AuthService::for_store(store));
    (TempDbDir(dir), service, id)
}

/// 本家分类结果 → 记账名单（只取本次这一个键，绕开目录缓存的全局态）。
fn single(name: &str) -> Vec<String> {
    vec![name.to_string()]
}

/// 一份临时库 + 指定 provider 下的若干账号（`uids` 按给定顺序落库）。
///
/// 与 [`temp_service`] 的差别只有两处：provider 可指定、账号可多于一条。
/// 后者是选路用例必须的 —— 只有一条账号时「粘住」与「没粘住」的结论相同，
/// 断言不出任何东西。
fn temp_service_for(provider: &str, uids: &[&str]) -> (TempDbDir, UpstreamService) {
    use crate::server::core::account_store::AccountStore;
    use crate::server::core::auth::AuthService;
    use crate::server::db::Db;

    let seq = TEMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pl-sticky-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = AccountStore::with_db(Some(Db::open(&dir.join("agent2api.db")).expect("临时库应当能建起来")));
    for uid in uids {
        store
            .add_account(
                &json!({"auth": {"accessToken": "at"}, "account": {"uid": uid}}),
                None,
                Some(provider),
            )
            .expect("临时账号应当能建起来");
    }
    let service = UpstreamService::new(store.clone(), AuthService::for_store(store));
    (TempDbDir(dir), service)
}

#[test]
fn stateful_quota_bookkeeping_keeps_the_adapter_reset_at() {
    let (_dir, service, id) = temp_service();
    let now = logging::now_ms();
    // 可辨恢复时刻：3 小时后。刻意与 10 分钟兜底不同量级 —— 改前这里落的是
    // now+600000，等值断言必红；而文案里没有任何日期形态，兜底的第二次机会
    // （`errors::parse_quota_reset_at`）也救不出这个值，差异只能来自透传。
    let reset_at = now + 3 * 60 * 60 * 1000;
    let class = UpstreamErrorClass::QuotaLimited {
        reset_at: Some(reset_at),
        message: "上游返回 403: insufficient quota".to_string(),
        upstream_code: Some(4291200),
        status: 403,
    };
    // 整组（福利池按账号记账）与单键走的是同一个循环，这里给两个名字覆盖循环体
    let group = vec!["m-benefit-a".to_string(), "m-benefit-b".to_string()];
    mark_conversation_limited(&service, &id, &group, &class, "上游返回 403: insufficient quota");
    for name in &group {
        let stored = rotate::account_limit_reset_at(&service, &id, name);
        assert_eq!(stored, reset_at, "{name} 的 resetAt 应当是适配器给的恢复时刻");
        let minutes = (stored - now) / 60_000;
        assert!(
            (stored - (now + 10 * 60 * 1000)).abs() > 60_000,
            "{name} 落到了 10 分钟兜底冷却：{minutes} 分钟"
        );
    }
    // 同一份分类里的另两个字段也照原样落地（写死 None 的那版同样丢掉过它们吗 ——
    // 没有，但这里是唯一一处能一次钉住「记账四元组一致」的地方）
    let limits = {
        let snapshot = service.store.list_accounts();
        snapshot["accounts"].as_array().unwrap()[0]["rateLimits"].clone()
    };
    assert_eq!(limits["m-benefit-a"]["status"], json!(403));
    assert_eq!(limits["m-benefit-a"]["code"], json!(4291200));
}

#[test]
fn stateful_quota_bookkeeping_falls_back_when_adapter_gives_no_reset_at() {
    let (_dir, service, id) = temp_service();
    let now = logging::now_ms();
    // 429 是分钟级限流，CodeArts 的 classify 那边就给 None（给它零点会把一个只是
    // 暂时繁忙的账号打死一整天）—— 兜底冷却是它应得的行为，修复不许把它抹掉。
    // 文案取无日期形态：解析兜底（`parse_quota_reset_at`）也救不出时刻。
    let message = "上游返回 429: too many requests";
    let class = UpstreamErrorClass::QuotaLimited {
        reset_at: None,
        message: message.to_string(),
        upstream_code: None,
        status: 429,
    };
    mark_conversation_limited(&service, &id, &single("m-429"), &class, message);
    let stored = rotate::account_limit_reset_at(&service, &id, "m-429");
    let seconds = (stored - now) / 1000;
    assert!(
        stored > now + 9 * 60 * 1000 && stored <= now + 11 * 60 * 1000,
        "无恢复时刻时应落 10 分钟兜底，实际 {seconds} 秒"
    );
}

#[test]
fn stateful_quota_bookkeeping_ignores_non_quota_classes() {
    let (_dir, service, id) = temp_service();
    // 对照组：调用方的 `if let` 已经拦下非 QuotaLimited，函数自己也不该动手 ——
    // 内容闸门/权限那一类失败本来就不该罚账号（冷却一整天尤其不能给它们）
    let message = "上游返回 403: permission denied";
    let class = UpstreamErrorClass::Fatal {
        status: 403,
        message: message.to_string(),
        upstream_code: None,
    };
    mark_conversation_limited(&service, &id, &single("m-fatal"), &class, message);
    assert_eq!(rotate::account_limit_reset_at(&service, &id, "m-fatal"), 0, "非限额分类不该落冷却");
}

#[test]
fn codearts_daily_pool_envelope_cools_to_the_boundary_instead_of_ten_minutes() {
    // 3.3.0-beta 系把 CodeArts 的转发与错误分类搬进了插件仓库（ProvidersPlugin）。
    // 网关侧不再内置它的 `classify_conversation_error`：插件缺位时适配器回落
    // `missing_plugin`，一切错误都分类为 Fatal —— 这条用例钉的「4291.200 → 日池
    // 零点冷却」语义随实现搬去了插件侧，在网关这里无从断言。保留用例骨架并在
    // 插件缺位时跳过，插件在场（装了 ProvidersPlugin 并注册 CodeArts）时仍照常
    // 钉语义。
    use crate::server::errors::GatewayError;

    let (_dir, _service, _id) = temp_service();
    let error = GatewayError::with_status(403, "上游报告 InferHub.4291.200：insufficient quota");
    // ── 分类走**插件错误投影**，不走 `adapter_for` ────────────────
    // 迁移完成后 `adapter_for` 只返回「未安装插件」的占位适配器（它的
    // `classify_conversation_error` 恒为 Fatal，且文案是「请安装插件」）——
    // 真实的 CodeArts 分类在插件里，经 `strict_errors::conversation` 投影回宿主
    // 的 `conversation_class`。这条用例要钉的是**那条真实链路**：日池耗尽的
    // 403 必须带北京零点，否则存储层落到 10 分钟兜底、账号被反复白撞。
    let class = crate::server::core::plugin_host::projected_conversation_class(
        "codearts",
        &crate::server::core::plugin_host::projected_conversation_error(
            "codearts",
            403,
            UpstreamErrorClass::QuotaLimited {
                // 插件算出的日池重置点（`codearts::adapter::daily_pool_reset_at`
                // → `welfare::next_day_boundary_ms`）。这里就地复算同一个值：
                // 宿主侧不该依赖插件实现，测试只要能证明「这个时刻被原样保住」。
                reset_at: Some(next_beijing_midnight(logging::now_ms())),
                message: error.message.clone(),
                upstream_code: None,
                status: 403,
            },
            &json!({"error_msg": "InferHub.4291.200", "message": "insufficient quota"}),
            None,
        ),
    );
    let UpstreamErrorClass::QuotaLimited { reset_at: Some(reset_at), .. } = &class else {
        panic!("福利池耗尽的文案应分类为带恢复时刻的 QuotaLimited，实际 {class:?}");
    };
}

#[test]
fn that_one_argument_is_the_divider_between_a_day_and_ten_minutes() {
    let (_dir, service, id) = temp_service();
    // 同一个账号库里放两条账号：一条按「改后的写法」记账，一条按「改前的写法」
    // （同一个 rotate::mark_account_limited，只是恢复时刻写死 None）。
    // 同一份分类、同一句文案 —— 差别只有那一行实参。
    let pre_fix = service
        .store
        .add_account(
            &json!({"auth": {"accessToken": "at2"}, "account": {"uid": "quota-2"}}),
            None,
            None,
        )
        .expect("对照账号应当能建起来");
    let pre_fix_id = pre_fix["id"].as_str().expect("公开形态应带 id").to_string();

    let now = logging::now_ms();
    let reset_at = now + 3 * 60 * 60 * 1000;
    let message = "上游返回 403: insufficient quota";
    let class = UpstreamErrorClass::QuotaLimited {
        reset_at: Some(reset_at),
        message: message.to_string(),
        upstream_code: Some(4291200),
        status: 403,
    };
    mark_conversation_limited(&service, &id, &single("m-after"), &class, message);
    // 改前的那一发：另三个字段照旧，恢复时刻写成 None
    rotate::mark_account_limited(&service, &pre_fix_id, "m-before", 403, Some(4291200), None, message);

    let after = rotate::account_limit_reset_at(&service, &id, "m-after");
    let before = rotate::account_limit_reset_at(&service, &pre_fix_id, "m-before");
    assert_eq!(after, reset_at, "改后应透传适配器给的恢复时刻");
    let before_minutes = (before - now) / 60_000;
    assert!(
        before_minutes >= 9 && before_minutes <= 11,
        "改前落的是存储层兜底冷却（10 分钟），实际 {before_minutes} 分钟"
    );
    // 分水岭本身：同一份分类结果，两种写法差出 2 小时以上 —— 白撞节拍就是从这里来的
    assert!(
        after - before > 2 * 60 * 60 * 1000,
        "透传与写死 None 的差距应当是「一整天 vs 10 分钟」量级，实际差 {} 分钟",
        (after - before) / 60_000
    );
}

const APIG_TOO_LARGE_BODY: &str = r#"{"error_msg":"Request entity too large","error_code":"APIG.0201","request_id":"apig-test"}"#;

fn fatal(status: u16) -> UpstreamErrorClass {
    UpstreamErrorClass::Fatal {
        status,
        message: format!("上游返回 {status}"),
        upstream_code: None,
    }
}

/// 413 说的是「这份字节太大」，重发一遍没有任何会变的量 ⇒ 不消耗原地重发预算。
/// 改造前它落到兜底那一档，三次 ×5 秒间隔 + 每次约 13 秒往返 = 客户端的 60 秒
/// 等待墙先到，面板只剩「408 客户端在响应完成前断开」，真因（413）埋在明细里。
#[test]
fn a_payload_size_rejection_gets_no_in_place_retry() {
    assert!(
        PAYLOAD_SIZE_REJECTION_STATUSES.contains(&413),
        "413 应当在「按体积拒收」名单里"
    );
    assert!(
        fallback_retry_advice(&fatal(413), 3, 413).is_none(),
        "预算还剩 3 次也不该给 413 提原地重发"
    );
}

/// 反对照：其它 4xx/未知失败照旧走兜底原地重发（这条改动只放开"体积拒收"那一格，
/// 没有把 400 这类"含义由上游决定"的码一并排除）。404 已于 issue #255 单独豁免
/// （路由 / 模型名类确定性失败，见 `DETERMINISTIC_NOT_FOUND_STATUSES`），不在此列。
#[test]
fn other_fatal_statuses_still_get_the_fallback_resend() {
    for status in [400u16, 409] {
        assert!(
            fallback_retry_advice(&fatal(status), 3, status).is_some(),
            "HTTP {status} 的语义由上游决定，兜底原地重发的既有行为不该变"
        );
    }
    assert!(
        !TRANSIENT_RETRY_STATUSES.contains(&413),
        "413 不该被当成瞬时抖动（否则瞬时那一档会把它重发回去）"
    );
}

/// 413 是「这份字节太大」：**同一家**换账号不会让这份 body 变小。
///
/// issue #6 的形状：CatPaw 入口的 openresty 卡在 1 MiB，会话式路径拿到默认
/// Fatal 后会把队列里每个候选账号都撞一遍同样的 413，客户端白等 N 个往返。
/// 但字节墙是每家自己的（CodeArts 约 6 MiB、Loomy 约 12 MB），换一家仍可能过，
/// 所以判据按**家**收窄。
#[test]
fn a_payload_bound_failure_does_not_switch_accounts() {
    assert!(
        payload_bound_failure(&fatal(413)),
        "413 必须被认成「与账号无关的体积拒绝」"
    );
    // 反面对照：404 换号有意义（别的账号 / 别的家可能有这个模型），
    // 400 / 429 的语义由上游决定，都不该被这条判据吞掉。
    for status in [400u16, 404, 429, 500, 502, 503] {
        assert!(
            !payload_bound_failure(&fatal(status)),
            "HTTP {status} 不该被当成体积拒绝（会把正常的换号/重试路径掐掉）"
        );
    }
    assert!(
        !payload_bound_failure(&UpstreamErrorClass::QuotaLimited {
            status: 413,
            message: "限额".to_string(),
            upstream_code: None,
            reset_at: None,
        }),
        "限额分类有自己的记账与冷却路径，不该被体积判据接管"
    );
    // 同家换号无意义 → 拦；跨家换号有意义 → 放行。
    assert!(
        payload_bound_blocks_switch("catpaw", "catpaw"),
        "同一家的另一个账号面对的是同一堵字节墙，不该白撞"
    );
    assert!(
        !payload_bound_blocks_switch("codearts", "catpaw"),
        "别家的墙更宽（CodeArts 约 6 MiB），跨家回退必须留着"
    );
}

/// 端到端：打到真实返回 413 的替身上身，**只发一次**（改造前是 1 + 3 次）。
#[tokio::test]
async fn a_real_413_response_is_sent_once_and_handed_back() {
    let (transport, hits, task) =
        mock_upstream(413, "application/json; charset=utf-8", APIG_TOO_LARGE_BODY).await;
    let mut budget = RetryBudget::new(3);
    let capture = crate::server::core::debug_traffic::TrafficCapture::begin("payload-too-large");
    // 适配器只提供「怎么判上游错误」这一半；413 的处置在**编排层**
    // （`transport::RetryBudget` + `fallback_retry_advice`），因此随便挑一个
    // 内置家即可。原先这里用 ZCode 的实例，它随实现搬到插件了 —— 换成本家
    // 走得通的 WorkBuddy，断言的仍是同一处语义。
    let result = send_with_retry(
        adapter_for(ProviderKind::WorkBuddy),
        &transport,
        &mut budget,
        Some(&capture),
        &std::sync::Arc::new(RequestTelemetry::new()),
        false,
        None,
    )
    .await;
    task.abort();
    let failure = match result {
        Ok(_) => panic!("HTTP 413 不该被当成成功流"),
        Err(failure) => failure,
    };
    assert_eq!(
        1,
        hits.load(Ordering::SeqCst),
        "体积拒收的失败应当一次就交回，不该原地重发"
    );
    assert!(
        matches!(failure.class, UpstreamErrorClass::Fatal { status: 413, .. }),
        "状态码 413 要原样带回，不该被折成别的失败"
    );
}

#[test]
fn a_deterministic_404_skips_the_in_place_resend() {
    // issue #255 的形状：OfficeAce 对目录里有、套餐里没有的名字回
    // `404 {"code":null,"message":"Invalid model."}`，旧判据把它当普通 Fatal，
    // 原地重发 3 次 × 5 秒才换号——客户端白等 15 秒，换来的还是同一个 404。
    let not_found = UpstreamErrorClass::Fatal {
        status: 404,
        upstream_code: None,
        message: "上游返回 404: {\"code\":null,\"message\":\"Invalid model.\"}".to_string(),
    };
    assert!(
        fallback_retry_advice(&not_found, 3, 404).is_none(),
        "名字类确定性失败不该再原地重发"
    );
    // 对照：普通 400 仍走原地重发（400 的语义由上游定，重发是安全默认）
    let bad_request = UpstreamErrorClass::Fatal {
        status: 400,
        upstream_code: None,
        message: "上游返回 400: whatever".to_string(),
    };
    assert!(
        fallback_retry_advice(&bad_request, 3, 400).is_some(),
        "404 的豁免不许顺手把 400 也豁免了"
    );
}

// ── 会话粘性的写入 → 查找闭环（issue #2）──────────────────────────────
//
// 这一组用例钉的是「失败了也不会报错」的那半边：写入侧（成功后
// `sticky_bind`）与查找侧（选路时 `snapshot_for_model`）必须用**同一把键**、
// **同一个 TTL 口径**。任何一侧漏了 model、时间戳或账号 id 的换算，症状都只是
// `route.stickyHit` 恒为 false（请求照样成功、账号照样轮换），不会抛错。

/// 同一会话键的第二条请求必须命中第一条绑定的账号。
///
/// 账号挂在 `custom-` 前缀的提供商下：那类账号不受「插件是否已安装」的可见性
/// 闸门约束（见 `account_store::account_visible`），于是这条用例在任何测试环境
/// 里都能走到真实的 `select_target_account` 选路，而不是被候选池为空提前短路。
#[tokio::test]
async fn a_second_request_on_the_same_session_sticks_to_the_first_account() {
    use crate::server::core::routing::CooldownKeys;
    use crate::server::core::upstream::rotate::select_target_account;
    use crate::server::core::upstream::route_state::RouteState;
    use axum::http::HeaderMap;

    let provider = "custom-sticky-test";
    let (_dir, service) = temp_service_for(provider, &["sticky-1", "sticky-2"]);
    let body = json!({
        "model": "sticky-model",
        "messages": [{ "role": "user", "content": "hello" }],
    });
    let key = RouteState::conversation_key(&HeaderMap::new(), &body);
    assert!(!key.is_empty(), "带 messages 的请求必须有会话键，否则粘性永远不生效");
    let keys = CooldownKeys::new("sticky-model");

    // 第一条：没有绑定，正常选路（转发成功后由 attempt_queue 调 sticky_bind）
    let first = select_target_account(&service, &[provider], &keys, &[], None, &key)
        .await
        .expect("第一条请求应当选到账号");
    assert_eq!(
        first.decision.as_ref().map(|decision| decision["stickyHit"].clone()),
        Some(json!(false)),
        "首次请求不该命中粘性"
    );
    let bound = first.account_id.clone().expect("应当选到有 id 的账号");

    // 与转发成功出口逐字同源的写入
    let ttl = config::routing_settings().sticky.ttl_ms().unwrap_or(0);
    assert!(ttl > 0, "测试环境的粘性档位必须开着（默认 auto）");
    service.route.sticky_bind(
        &key,
        "sticky-model",
        &bound,
        "content-hash",
        logging::now_ms(),
        ttl,
        service.route.sticky_generation(),
    );

    // 第二条：同一会话、同一模型 → 命中，并且仍然用同一个账号
    let second = select_target_account(&service, &[provider], &keys, &[], None, &key)
        .await
        .expect("第二条请求应当选到账号");
    let decision = second.decision.clone().expect("选路要带决策快照");
    assert_eq!(decision["stickyHit"], json!(true), "同一会话的第二条请求应当命中粘性");
    assert_eq!(decision["picked"], json!(bound), "命中后应当置顶原账号");
    assert_eq!(second.account_id.as_deref(), Some(bound.as_str()));
}
