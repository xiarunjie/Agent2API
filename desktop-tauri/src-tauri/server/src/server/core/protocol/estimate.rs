//! 输入 token 的**量级估算**（请求体 → token 数；不是精确分词）。
//!
//! ── 为什么网关要自己估 ──────────────────────────────────────
//! Anthropic 协议的流式响应必须以 `message_start` 开头，而这一帧里的
//! `usage.input_tokens` 是 Claude Code / Claude Desktop / Hermes 这类客户端
//! 判断「这一轮上下文用了多少」的锚点。但本网关的上游全是 Chat 系：
//! **真实的 prompt_tokens 只在上游的最后一帧 usage 里出现** ——
//!   · OpenAI 兼容上游：`stream_options.include_usage` 的收尾帧（空 choices）；
//!   · Anthropic 系上游：`message_start` 里是 0 占位，真值在收尾前的
//!     `message_delta`（见 `anthropic_outbound::ChatFromAnthropicStream::absorb_usage`）。
//! 也就是说流式首帧到达时，精确值**不可能**已经存在：网关没有上游的分词器，
//! 六家上游也都不提供 tokenizer 接口（`api::chat::count_tokens` 里记着同一件事）。
//!
//! 于是首帧只能给「量级正确的估算」：客户端拿到 5 万左右的锚点，就不会退回
//! 本地估算、把每轮输入一路累加成 2.6M（issue #1）。真实用量仍由收尾的
//! `message_delta` 给出，客户端在流结束时校正。
//!
//! ── 估算值不进请求统计 ──────────────────────────────────────
//! 只用于首帧的协议字段。请求日志/报表的 `prompt_tokens` 口径是「上游计费的
//! 读数」，只认上游 usage（`core::upstream::usage::report_usage`）——
//! 把估算值写进那一列会让统计与计费脱钩。
//!
//! ── 口径 ────────────────────────────────────────────────────
//! 按文本逐字符加权（与 Anthropic 官方「约 3.5 字符 / token」的量级一致）：
//!   · 中日韩字符（汉字、假名、谚文、全角标点）：约 1.5 字符 / token。
//!     主流分词器对 CJK 大多一字一 token 上下，按 ASCII 的 4 字符 / token 折
//!     会把中文请求低估 3 倍，正是 issue 里「估算不可用」的那一侧；
//!   · 其余字符（ASCII 字母数字、标点、空白）：约 4 字符 / token；
//!   · 内联图片（`data:…;base64,…`）：整段按一张图估固定值 —— base64 的长度
//!     与图片的 token 成本无关，按字符数折会把一张截图算成几万 token。
//! JSON 的键名同样计入（每个键在真实分词里也占 token）。
//!
//! 估算误差在两倍以内即可满足用途（给客户端一个量级锚点）；它**不是**计费值，
//! 也不承诺与上游的 prompt_tokens 逐 token 对齐。
//!
//! 用例在 `server/tests/anthropic_message_start.rs`（与流式帧的回归用例同居，
//! 理由见那里的注释：本基线的 lib 测试目标编译不过，单元测试跑不起来）。

use serde_json::Value;

/// 中日韩字符每个 token 覆盖的字符数（约 1.5 字符 / token）。
const WIDE_CHARS_PER_TOKEN: f64 = 1.5;
/// 其余字符每个 token 覆盖的字符数（约 4 字符 / token）。
const NARROW_CHARS_PER_TOKEN: f64 = 4.0;
/// 一张内联图片（base64 data URL）的估算 token 数。
///
/// Anthropic 的图片计费约为 `宽 × 高 / 750`（1024×768 的截图 ≈ 1050），
/// Claude Code 常见的 1568×800 截图 ≈ 1670 —— 取 1600 作代表值。
/// 精确值取决于上游的分词与缩放策略，这里只需要量级正确。
const INLINE_IMAGE_TOKENS: f64 = 1600.0;

/// 估算一个 Chat 形态请求体的输入 token 数（下限 1）。
///
/// 入参用 **Chat 形态**而不是 Anthropic 原始体：`/v1/messages` 的转发体就是
/// `chat_from_anthropic` 的结果，`count_tokens` 端点也用同一份，两处的读数因此
/// 同源（同一个请求在 context 预算与首帧 usage 上看到的数字一致）。
pub fn estimate_prompt_tokens(body: &Value) -> i64 {
    let mut score = 0.0f64;
    accumulate(body, &mut score);
    // 下限 1：空请求也至少算 1 —— 0 在客户端眼里是「没有输入」，
    // 而真实请求不可能真的没有 token
    (score.ceil() as i64).max(1)
}

/// 递归累加：字符串按字符加权，容器逐层下钻，数字/布尔忽略。
fn accumulate(value: &Value, score: &mut f64) {
    match value {
        Value::String(text) => *score += text_tokens(text),
        Value::Array(items) => {
            for item in items {
                accumulate(item, score);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                // 键名本身也占 token（`"role"` / `"content"` 各 1~2 个）
                *score += text_tokens(key);
                accumulate(item, score);
            }
        }
        _ => {}
    }
}

/// 一段文本的估算 token 数。
fn text_tokens(text: &str) -> f64 {
    if is_inline_media(text) {
        return INLINE_IMAGE_TOKENS;
    }
    let mut wide = 0usize;
    let mut narrow = 0usize;
    for ch in text.chars() {
        if is_wide_script(ch) {
            wide += 1;
        } else {
            narrow += 1;
        }
    }
    wide as f64 / WIDE_CHARS_PER_TOKEN + narrow as f64 / NARROW_CHARS_PER_TOKEN
}

/// 是否是内联媒体（`data:` URL 且带 base64 载荷）。
///
/// 只认前缀形态，不解析内容：图片、音频、PDF 一律按固定值估。真正的图片
/// token 成本取决于尺寸，与 base64 的字符数无关。
fn is_inline_media(text: &str) -> bool {
    // `find` 返回的是字符边界上的字节下标，按它比较不会切坏多字节字符
    text.starts_with("data:")
        && text
            .find(";base64,")
            .is_some_and(|offset| offset < 64)
        && text.len() > 32
}

/// 是否是「宽字符脚本」：汉字、假名、谚文、全角标点等（约 1.5 字符 / token）。
///
/// 区间取 Unicode 的常用块，不追求完备（生僻字少一个多一个对估算没有意义）：
///   · 1100–11FF   谚文字母
///   · 2E80–2FFF   CJK 部首 / 康熙部首
///   · 3000–30FF   CJK 标点、平假名、片假名
///   · 3400–4DBF   CJK 扩展 A
///   · 4E00–9FFF   CJK 基本区
///   · A960–A97F   谚文字母扩展 A
///   · AC00–D7FF   谚文音节
///   · F900–FAFF   CJK 兼容汉字
///   · FE30–FE4F   CJK 兼容形式（竖排标点）
///   · FF00–FFEF   全角 / 半角形式
///   · 20000–2FA1F CJK 扩展 B 及以后
fn is_wide_script(ch: char) -> bool {
    matches!(
        u32::from(ch),
        0x1100..=0x11FF
            | 0x2E80..=0x2FFF
            | 0x3000..=0x30FF
            | 0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xA960..=0xA97F
            | 0xAC00..=0xD7FF
            | 0xF900..=0xFAFF
            | 0xFE30..=0xFE4F
            | 0xFF00..=0xFFEF
            | 0x20000..=0x2FA1F
    )
}

