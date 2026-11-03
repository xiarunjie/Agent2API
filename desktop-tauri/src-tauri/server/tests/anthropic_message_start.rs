//! `/v1/messages` 流式 `message_start` 的输入 token 回归用例（issue #1）。
//!
//! 构造一段真实的 Chat SSE（首帧 delta、正文、收尾 finish_reason、末尾 usage、
//! `[DONE]`），喂给 `AnthropicStream`，逐帧解析并断言 usage。改造前的基线：
//!
//! ```text
//! event: message_start
//! data: {"message":{…,"usage":{"input_tokens":0,"output_tokens":0}},…}
//! event: message_delta
//! data: {"delta":{…},"usage":{"input_tokens":1412,…}}
//! ```
//!
//! 即整条流开头把输入报成 0 —— Claude Code / Claude Desktop 因此拿不到输入
//! 锚点，`/context` 退回本地估算把每轮输入累加（2.6M / 1M）。
//!
//! 改造后：`message_start` 带请求体估算值（真实值此时不可得，见
//! `core::protocol::estimate` 的模块头），`message_delta` 带上游计费的真值。

use agent2api_server::server::core::protocol::anthropic::{
    chat_from_anthropic, AnthropicCollector, AnthropicStream,
};
use agent2api_server::server::core::protocol::estimate::estimate_prompt_tokens;
use serde_json::{json, Value};

const MODEL: &str = "sn-deepseek-v4-1-flash-1017";

/// 上游最终计费的 prompt_tokens（本用例的「真值」）。
///
/// 是一个与请求量级相当的代表值：真实读数由上游分词器决定，用例只校验
/// 「口径与帧形状」——首帧带估算、收尾带上游真值。
const BILLED_PROMPT_TOKENS: i64 = 1412;
/// 上游最终计费的 completion_tokens。
const BILLED_COMPLETION_TOKENS: i64 = 56;

/// 一段 Claude Code 风格的 `/v1/messages` 请求（system + tools + 工具结果）。
fn request_body() -> Value {
    // 工具结果模拟「读了一个文件」：几千字符的代码，是 Claude Code 上下文里
    // 占比最大的一块，估算器必须把它按文本算进去
    let file_body = "pub fn bootstrap(port: u16) -> Result<ServerState, String> {\n    \
                     let config_dir = config::config_dir();\n    \
                     let db = Db::open(&config_dir.join(db::FILE_NAME))?;\n    \
                     config::init(db.clone());\n    \
                     Ok(ServerState::new(port, config_dir, db))\n}\n"
        .repeat(12);
    json!({
        "model": MODEL,
        "max_tokens": 1024,
        "stream": true,
        "system": [
            {"type": "text", "text": "You are Claude Code, Anthropic's official CLI tool for Claude."},
            {"type": "text", "text": "你是一个中文编程助手，回答要简洁，先给结论再给依据。"}
        ],
        "tools": [{
            "name": "Read",
            "description": "Read a file from the local filesystem.",
            "input_schema": {
                "type": "object",
                "properties": {"file_path": {"type": "string", "description": "The absolute path"}},
                "required": ["file_path"]
            }
        }],
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "帮我看看 src/server/mod.rs 里 bootstrap 做了什么"}
            ]},
            {"role": "assistant", "content": [
                {"type": "text", "text": "我先读一下这个文件。"},
                {"type": "tool_use", "id": "toolu_01", "name": "Read",
                 "input": {"file_path": "/Users/me/project/src/server/mod.rs"}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01", "content": file_body},
                {"type": "text", "text": "再解释一下 open_db 的顺序为什么重要。"}
            ]}
        ]
    })
}

/// 与 `messages_endpoint` 同一口径：先转成 Chat 形态，再估算。
fn estimate_for(anthropic_body: &Value) -> i64 {
    let chat = chat_from_anthropic(anthropic_body).expect("请求体应能转成 Chat 形态");
    estimate_prompt_tokens(&chat)
}

/// 上游 Chat SSE：`billed_prompt` 为 None 时整条流都不带 usage
/// （部分上游不返回 stream usage，网关必须仍给出可用的读数）。
fn upstream_chunks(billed_prompt: Option<i64>) -> Vec<String> {
    let mut chunks = vec![
        r#"data: {"id":"chatcmpl-9","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#
            .to_string(),
        r#"data: {"id":"chatcmpl-9","choices":[{"index":0,"delta":{"content":"读完了"}}]}"#
            .to_string(),
        r#"data: {"id":"chatcmpl-9","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#
            .to_string(),
    ];
    if let Some(prompt) = billed_prompt {
        let usage = json!({
            "id": "chatcmpl-9",
            "choices": [],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": BILLED_COMPLETION_TOKENS,
                "total_tokens": prompt + BILLED_COMPLETION_TOKENS,
            },
        });
        chunks.push(format!("data: {usage}"));
    }
    chunks.push("data: [DONE]".to_string());
    // SSE 帧以空行分隔；上面的 `data:` 行统一在这里补换行
    chunks.into_iter().map(|line| format!("{line}\n\n")).collect()
}

/// 喂一段上游字节，返回本次下发的帧文本。
fn feed(machine: &mut AnthropicStream, chunks: &[String]) -> String {
    let mut out = String::new();
    for chunk in chunks {
        for frame in machine.push(chunk.as_bytes()) {
            out.push_str(&String::from_utf8_lossy(&frame));
        }
    }
    out
}

/// 把 SSE 文本拆成 (事件名, data JSON) 列表。
fn events(text: &str) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for block in text.split("\n\n") {
        let mut event = String::new();
        let mut data: Option<Value> = None;
        for line in block.lines() {
            if let Some(name) = line.strip_prefix("event: ") {
                event = name.trim().to_string();
            } else if let Some(rest) = line.strip_prefix("data: ") {
                data = serde_json::from_str(rest).ok();
            }
        }
        if let (false, Some(value)) = (event.is_empty(), data) {
            out.push((event, value));
        }
    }
    out
}

fn usage_of(event: &(String, Value)) -> Value {
    if event.0 == "message_start" {
        event.1["message"]["usage"].clone()
    } else {
        event.1["usage"].clone()
    }
}

#[test]
fn message_start_reports_request_estimate_when_upstream_usage_is_late() {
    let hint = estimate_for(&request_body());
    assert!(hint > 200, "fixture 应估出可观的输入量，实际 {hint}");

    let mut machine = AnthropicStream::new(MODEL, hint);
    let chunks = upstream_chunks(Some(BILLED_PROMPT_TOKENS));

    // 首帧：上游还没给 usage —— 必须用估算值兜底，且 output_tokens 为 0
    let first = feed(&mut machine, &chunks[..1]);
    let first_events = events(&first);
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].0, "message_start");
    assert_eq!(usage_of(&first_events[0])["input_tokens"], json!(hint));
    assert_eq!(usage_of(&first_events[0])["output_tokens"], json!(0));
    // 缓存计数即使为 0 也要有键（上游 issue #424：缺键时部分客户端读不到）
    assert_eq!(usage_of(&first_events[0])["cache_read_input_tokens"], json!(0));

    // 收尾：message_delta 带上游计费的真值
    let rest = feed(&mut machine, &chunks[1..]);
    let rest_events = events(&rest);
    let delta = rest_events
        .iter()
        .find(|(name, _)| name == "message_delta")
        .expect("流必须有 message_delta");
    assert_eq!(usage_of(delta)["input_tokens"], json!(BILLED_PROMPT_TOKENS));
    assert_eq!(usage_of(delta)["output_tokens"], json!(BILLED_COMPLETION_TOKENS));
    assert_eq!(delta.1["delta"]["stop_reason"], json!("end_turn"));
    assert!(rest_events.iter().any(|(name, _)| name == "message_stop"));

    // 估算值必须与上游读数同量级（防止估退化成 0 或天文数字；
    // 它不是精确分词，允许相当宽的误差）
    let billed = BILLED_PROMPT_TOKENS;
    assert!(
        (hint - billed).abs() * 10 < billed * 6,
        "估算 {hint} 与计费 {billed} 相差过大"
    );
}

#[test]
fn message_start_prefers_upstream_usage_when_the_first_chunk_carries_it() {
    let hint = estimate_for(&request_body());
    let mut machine = AnthropicStream::new(MODEL, hint);
    // 少数上游会在首帧就带 usage：有真值就用真值，不能被估算值顶掉
    let chunk = format!(
        "data: {}\n\n",
        json!({
            "id": "chatcmpl-9",
            "choices": [{"index": 0, "delta": {"content": "hi"}}],
            "usage": {
                "prompt_tokens": 500,
                "completion_tokens": 1,
                "prompt_tokens_details": {"cached_tokens": 100},
            },
        })
    );
    let text = feed(&mut machine, &[chunk]);
    let frames = events(&text);
    let start = frames
        .iter()
        .find(|(name, _)| name == "message_start")
        .expect("首帧必须是 message_start");
    // Anthropic 口径：input_tokens 不含缓存命中，缓存单独报
    assert_eq!(usage_of(start)["input_tokens"], json!(400));
    assert_eq!(usage_of(start)["cache_read_input_tokens"], json!(100));
    assert_eq!(usage_of(start)["output_tokens"], json!(0));
}

#[test]
fn message_delta_keeps_the_estimate_when_upstream_never_reports_usage() {
    let hint = estimate_for(&request_body());
    let mut machine = AnthropicStream::new(MODEL, hint);
    let text = feed(&mut machine, &upstream_chunks(None));
    let frames = events(&text);
    let start = frames
        .iter()
        .find(|(name, _)| name == "message_start")
        .expect("首帧必须是 message_start");
    let delta = frames
        .iter()
        .find(|(name, _)| name == "message_delta")
        .expect("流必须有 message_delta");
    assert_eq!(usage_of(start)["input_tokens"], json!(hint));
    // 上游整轮没报输入：收尾沿用估算值，不能把首帧的锚点覆盖回 0
    assert_eq!(usage_of(delta)["input_tokens"], json!(hint));
    assert_eq!(usage_of(delta)["output_tokens"], json!(0));
    assert_eq!(usage_of(delta)["cache_read_input_tokens"], json!(0));
}

#[test]
fn non_stream_aggregation_still_reports_the_billed_usage() {
    // 非流式路径（Collector）能拿到上游真实 usage，行为不应被本次改动影响
    let mut collector = AnthropicCollector::new();
    for chunk in upstream_chunks(Some(BILLED_PROMPT_TOKENS)) {
        collector.push(chunk.as_bytes());
    }
    collector.finish();
    let response = collector.into_response(MODEL);
    assert_eq!(response["usage"]["input_tokens"], json!(BILLED_PROMPT_TOKENS));
    assert_eq!(response["usage"]["output_tokens"], json!(BILLED_COMPLETION_TOKENS));
    assert_eq!(response["usage"]["cache_read_input_tokens"], json!(0));
}


// ─── 估算器本身的用例 ────────────────────────────────────────
//
// 放在集成测试而不是 `estimate.rs` 的 `#[cfg(test)]` 里：本基线（v3.3.0-beta.6）
// 的 lib 测试目标有与本次改动无关的编译错误（`api/pipeline.rs` 的
// `RequestQuery` 少字段、`provider_loop/tests.rs` 的 `send_with_retry` 少一个实参），
// 单元测试跑不起来。集成测试目标能独立编译运行，用例才真正被执行。

#[test]
fn estimator_empty_body_still_reports_one_token() {
    assert_eq!(estimate_prompt_tokens(&json!({})), 1);
    // 只有一个键名的空请求：个位数（键名本身按 4 字符 / token 计）
    assert!(estimate_prompt_tokens(&json!({"messages": []})) <= 5);
}

#[test]
fn estimator_ascii_text_is_about_four_chars_per_token() {
    let text = "a".repeat(4000);
    let tokens = estimate_prompt_tokens(&json!({"messages": [{"role": "user", "content": text}]}));
    // 4000 字符 ≈ 1000 token；键名再加几个
    assert!((950..=1100).contains(&tokens), "ASCII 估算 {tokens} 超出预期区间");
}

#[test]
fn estimator_cjk_text_is_counted_denser_than_ascii() {
    let cjk = "中".repeat(1500);
    let ascii = "a".repeat(1500);
    let cjk_tokens = estimate_prompt_tokens(&json!({"content": cjk}));
    let ascii_tokens = estimate_prompt_tokens(&json!({"content": ascii}));
    assert!(
        cjk_tokens > ascii_tokens * 2,
        "中文应显著高于同长度 ASCII：cjk={cjk_tokens} ascii={ascii_tokens}"
    );
    // 1500 个汉字 ≈ 1000 token（1.5 字符 / token）
    assert!((900..=1200).contains(&cjk_tokens), "中文估算 {cjk_tokens} 超出预期区间");
}

#[test]
fn estimator_inline_base64_images_do_not_explode_the_estimate() {
    let data_url = format!("data:image/png;base64,{}", "A".repeat(200_000));
    let tokens = estimate_prompt_tokens(&json!({
        "messages": [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": data_url}}
        ]}]
    }));
    // 20 万字符的 base64 若按字符折算是 5 万 token；按一张图应是千级
    assert!((1500..=2500).contains(&tokens), "内联图片估算 {tokens} 不应按字符数放大");
}

#[test]
fn estimator_grows_with_content() {
    let small = estimate_prompt_tokens(&json!({"content": "hello"}));
    let large = estimate_prompt_tokens(&json!({"content": "hello ".repeat(1000)}));
    assert!(large > small);
}
