# Bug Report — Cache-Aware Rust Router

## Title
SSE stream corruption on **tool-call** responses: lost event framing + duplicated/misordered `[DONE]` terminal

## Severity
High — breaks all streaming tool-call (function-calling) responses end-to-end. Any spec-compliant SSE/OpenAI client (here: OpenCode) fails to parse the stream and the request errors out.

## Topology
```
OpenCode client → Python LLM proxy → [THIS] cache-aware Rust router → vLLM pods
```
The Rust router parses the SSE response stream (for token-load tracking / cache accounting) and **re-serializes** it before returning it upstream. The corruption is introduced at this re-serialization step.

## Isolation already performed (this is why the router is the suspect)
- **Direct to vLLM pod (router bypassed): stream is clean.** Correct `\n\n` framing, single `[DONE]` as the final line, tool calls parse fine.
- **Through the Rust router: stream is corrupted** (details below).
- The Python proxy in front does a clean reconstruction (`data: {json}\n\n` per event) and is *not* the source — and in fact would *drop* a malformed glued chunk, so the corruption that reaches the client originated at or before the router's output.

⇒ The delta that introduces the bug is the Rust router.

## Observed behavior (client-side failed-parse buffer)
The client tried to `JSON.parse` the following single blob and failed with `JSON Parse error: Unexpected identifier "DONE"`:

```
[DONE]data: [DONE]data: {"id": "chatcmpl-83d7a126e1955a6b", "object": "chat.completion.chunk", "created": 1782418812, "model": "deepseek-ai/DeepSeek-V4-Flash", "choices": [{"index": 0, "delta": {"content": ""}, "logprobs": null, "finish_reason": "tool_calls", "stop_reason": null, "token_ids": null}]}
```

Reproduced across multiple retries; identical shape each time. Model: `deepseek-ai/DeepSeek-V4-Flash` (on-prem prod).

Two distinct defects are visible in that blob:

1. **Lost event framing.** Multiple SSE events are concatenated with no `\n\n` (blank-line) separator between them. Per the SSE spec, every event MUST be terminated by a blank line; a client treats the run-together bytes as one event and parses garbage. Here three payloads (`[DONE]`, `[DONE]`, the final chunk) are glued onto effectively one `data:` line.

2. **Terminal `[DONE]` duplicated and emitted BEFORE the final content chunk.** `[DONE]` appears **twice**, and **ahead of** the `finish_reason: "tool_calls"` chunk. A correct OpenAI-style stream emits exactly **one** `data: [DONE]` and it is **strictly the last** line — never before a content/finish chunk. The vLLM pod confirms this ordering when hit directly.

## Expected behavior
A re-serialized stream byte-for-byte equivalent in framing/ordering to what the pod emits:
- Each event is `data: <payload>\n\n` (blank line terminator preserved).
- Exactly one `data: [DONE]\n\n`, as the final line.
- The `finish_reason: "tool_calls"` chunk (and any trailing usage chunk) appear **before** `[DONE]`, in pod order.

## Trigger conditions / scope
- **Tool-call responses only.** Plain text streaming appears unaffected — pointing at a tool-call/terminal-specific code path in the router, not the generic token relay path.
- The request carries `stream_options.continuous_usage_stats: true` (injected by the Python proxy to get per-chunk usage). This makes vLLM emit usage on the trailing chunks and an empty-`choices` usage chunk near the end. **The router bug may be triggered/affected by this usage-bearing tail — please test with and without it.**
- The tool-call tail from vLLM is typically: `…delta tool_call fragments… → {finish_reason:"tool_calls", delta:{content:""}} → {choices:[], usage:{…}} → [DONE]`. These trailing events are frequently flushed by vLLM in a **single network write**, so the router often receives several events in one read.

## Most probable mechanism (for the router agent to verify)
The symptom — glued events + a terminal flushed ahead of buffered content + a duplicated terminal — is consistent with one or more of:

1. **Re-serialization drops the `\n\n` event terminator.** When the router writes events back out, check it emits `data: <payload>\n\n` (double newline), not `data: <payload>\n` or bare concatenation. Confirm this on the *terminal/tool-call path* specifically, since text works.

2. **Per-read passthrough without SSE reframing.** If the router reads the pod socket in arbitrary byte chunks and re-emits per read, multiple pod events that arrived in one read get written back glued. The router must buffer and split on the `\n\n` event boundary (carrying a remainder across reads), then re-emit one framed event at a time. (We hit and fixed this exact class of bug in the Python layer.)

3. **Double terminal emission.** The router likely both **forwards the pod's `[DONE]`** and **emits its own synthesized `[DONE]`** on completion-detect. Search for where the router writes `[DONE]` and ensure it does so exactly once — either pass through the pod's, or synthesize one and swallow the pod's, never both.

4. **Terminal flushed before the final buffered chunk (ordering).** If completion is detected on the `finish_reason` chunk and `[DONE]` is written immediately while the actual `finish_reason`/usage chunk is still sitting in a buffer, the terminal races ahead of the content. Ensure the completion chunk(s) are flushed **before** the terminal, in arrival order.

## What to check in the router code
- The SSE response re-serialization path: is `\n\n` framing preserved on every emitted event, including the tool-call/usage/terminal tail?
- The read loop: does it preserve SSE event boundaries across pod-socket reads, or re-emit raw reads?
- Terminal handling: exactly one `[DONE]`? Pass-through vs synthesized — is there accidental double emission?
- Flush ordering on completion-detect: is `[DONE]` ever written before the last content/finish/usage chunk?
- Tool-call vs text divergence: why does text stream cleanly but tool calls don't? (Different buffering/branch for `tool_calls` / empty-delta / empty-`choices` chunks?)
- `stream_options.continuous_usage_stats: true` / `include_usage`: does the empty-`choices` usage chunk confuse completion detection or framing?

## Asks for the router agent
1. Capture the router's **raw output bytes** for one tool-call request (`curl -N … | cat -A` so `\n` / `$` are visible) and compare against the same request hit **direct-to-pod**. Attach both.
2. Confirm whether `[DONE]` is genuinely emitted twice and ahead of the finish chunk in the router's raw output (vs. a client-side display artifact).
3. Re-test with `stream_options.continuous_usage_stats` removed to determine whether the usage tail is part of the trigger.

## Definition of done
For a streaming tool-call request through the router, raw output framing/ordering matches direct-to-pod: every event `\n\n`-terminated, the `finish_reason:"tool_calls"` (+ usage) chunk(s) before the terminal, and exactly one final `data: [DONE]\n\n`.
