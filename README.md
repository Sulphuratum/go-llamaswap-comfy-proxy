# comfy-proxy

A lightweight reverse proxy for [ComfyUI](https://github.com/comfyanonymous/ComfyUI) that integrates with [llama-swap](https://github.com/mostlygeek/llama-swap) for VRAM sharing between ComfyUI and LLMs.

## Architecture

Two instances of the same binary, each with a distinct role:

```
client  →  frontend proxy (:8189)  →  llama-swap (:8080)  →  backend proxy (:8190)  →  ComfyUI (:8188)
                    │                                                                        ↑
                    └── /history ──────────────────────────────────────────────────────────┘
                        (bypasses llama-swap entirely)
```

**Frontend proxy** — permanent, never exits:
- Intercepts `/history` and forwards it directly to ComfyUI, returning `{}` if ComfyUI is down. This prevents llama-swap from waking ComfyUI just because the client is polling for results.
- Forwards all other requests through llama-swap, which handles VRAM coordination.

**Backend proxy** — managed by llama-swap:
- Sits between llama-swap and ComfyUI.
- Exposes `/comfy-proxy/shutdown`: waits for ComfyUI to go idle, calls `POST /api/free` to release VRAM, then exits.
- llama-swap uses this as the `cmd`/`cmdStop` pair to know when VRAM is free.

ComfyUI itself runs permanently and is never started or stopped by llama-swap.

## Build

Requires Go 1.18 or later. No external dependencies.

```powershell
go build -o comfy-proxy.exe .
```

Or for Linux/macOS:

```bash
go build -o comfy-proxy .
```

## Usage

```
comfy-proxy [flags]

Flags:
  -addr           string    Proxy listen address                           (default ":8189")
  -target         string    Upstream URL to forward requests to            (default "http://127.0.0.1:8080")
  -comfyui        string    ComfyUI direct URL (queue polling, /api/free)  (default "http://127.0.0.1:8188")
  -history-quiet  duration  Silence window before /history is considered   (default "5s")
                            idle (only relevant for the backend instance)
```

## llama-swap integration

Start ComfyUI and the frontend proxy manually (e.g. as services). llama-swap manages only the backend proxy:

**Start frontend proxy** (once, on boot):
```powershell
.\comfy-proxy.exe -target http://127.0.0.1:8080/upstream/comfyui -comfyui http://127.0.0.1:8188 -addr :8189
```

**llama-swap config** — backend proxy is the `cmd` object llama-swap controls:
```yaml
models:
  comfyui:
    cmd: "comfy-proxy.exe -target http://127.0.0.1:8188 -comfyui http://127.0.0.1:8188 -addr :${PORT}"
    proxy: "http://127.0.0.1:8190"
    cmdStop: "curl -sf \"http://127.0.0.1:${PORT}/comfy-proxy/shutdown?timeout=120s\""
```

Point your ComfyUI client at the frontend proxy (`http://localhost:8189`).

### VRAM lifecycle

When llama-swap needs to load an LLM:
1. Calls `cmdStop` → `curl` blocks on `/comfy-proxy/shutdown`
2. Backend proxy waits for active requests to finish
3. Polls ComfyUI `GET /queue` until empty
4. Calls `POST /api/free` on ComfyUI to release VRAM
5. Returns `200`, backend proxy exits → `curl` exits 0
6. llama-swap loads the LLM

When the next ComfyUI request arrives:
1. llama-swap runs `cmd`, starting a fresh backend proxy instance
2. Forwards the request through — ComfyUI is already running, no startup delay

## Endpoints

### `GET /comfy-proxy/health`

Returns the current idle state. `200` = ready; `503` = work in progress.

```json
{
  "ready": false,
  "active_requests": 1,
  "requests": [{"id": 3, "method": "POST", "path": "/prompt", "elapsed": "2.1s", "ws": false}],
  "queue_running": 1,
  "queue_pending": 0,
  "history_silent": true
}
```

### `GET /comfy-proxy/drain[?timeout=60s]`

Blocks until idle, calls `POST /api/free`, returns `200`. The proxy stays running. Default timeout: 60s.

```json
{"status": "drained"}
```

### `GET /comfy-proxy/shutdown[?timeout=60s]`

Same as drain, but exits the process after responding. Used as llama-swap's `cmdStop` target.

```json
{"status": "shutting down"}
```

## Logging

```
[+] #4 POST /prompt [http] (active: 1)
[prompt] queued prompt_id=abc-123 queue_pos=0
[-] #4 POST /prompt [http] 200 1.243s (active: 0)
[drain] polling ComfyUI queue (history quiet window: 5s)
[drain] waiting: queue: 1 running, 0 pending
[drain] ComfyUI idle (queue empty, history quiet) — proceeding
[free] calling POST http://127.0.0.1:8188/api/free
[free] ComfyUI VRAM freed
[shutdown] drain complete, exiting
```
