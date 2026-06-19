package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ── History idle tracker ─────────────────────────────────────────────────────

// historyTracker records when the last /history request was proxied so the
// drain logic can wait for clients to stop polling before proceeding.
type historyTracker struct {
	mu       sync.Mutex
	lastCall time.Time
}

func (h *historyTracker) record() {
	h.mu.Lock()
	h.lastCall = time.Now()
	h.mu.Unlock()
}

// sinceLastCall returns (elapsed, true) or (0, false) if never called.
func (h *historyTracker) sinceLastCall() (time.Duration, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastCall.IsZero() {
		return 0, false
	}
	return time.Since(h.lastCall), true
}

func (h *historyTracker) silentFor(d time.Duration) bool {
	elapsed, ok := h.sinceLastCall()
	return !ok || elapsed >= d
}

// ── Server ───────────────────────────────────────────────────────────────────

type server struct {
	target       *url.URL   // llama-swap: where client requests are forwarded
	comfyURL     *url.URL   // ComfyUI direct: used for /queue polling and /api/free
	proxy        *httputil.ReverseProxy
	history      *historyTracker
	historyQuiet time.Duration
	shutdown     chan struct{}
}

func newServer(target, comfyURL *url.URL, historyQuiet time.Duration) *server {
	s := &server{
		target:       target,
		comfyURL:     comfyURL,
		history:      &historyTracker{},
		historyQuiet: historyQuiet,
		shutdown:     make(chan struct{}),
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorLog = log.Default()
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.Request.Method == http.MethodPost && resp.Request.URL.Path == "/prompt" {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			resp.Body = io.NopCloser(bytes.NewReader(body))
			var result struct {
				PromptID string `json:"prompt_id"`
				Number   int    `json:"number"`
			}
			if json.Unmarshal(body, &result) == nil && result.PromptID != "" {
				log.Printf("[prompt] queued prompt_id=%s queue_pos=%d", result.PromptID, result.Number)
			}
		}
		return nil
	}

	s.proxy = proxy
	return s
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func (s *server) proxyWebSocket(w http.ResponseWriter, r *http.Request) {
	backendAddr := s.target.Host
	if !strings.Contains(backendAddr, ":") {
		if s.target.Scheme == "https" {
			backendAddr += ":443"
		} else {
			backendAddr += ":80"
		}
	}

	backendConn, err := net.DialTimeout("tcp", backendAddr, 10*time.Second)
	if err != nil {
		log.Printf("websocket: dial %s: %v", backendAddr, err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer backendConn.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket not supported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		log.Printf("websocket: hijack: %v", err)
		return
	}
	defer clientConn.Close()

	// Forward the upgrade request to the backend.
	r.URL.Host = s.target.Host
	r.URL.Scheme = s.target.Scheme
	r.Host = s.target.Host
	if err := r.Write(backendConn); err != nil {
		log.Printf("websocket: write upgrade request: %v", err)
		return
	}

	// Flush any pre-upgrade data buffered from the client.
	if clientBuf != nil && clientBuf.Reader.Buffered() > 0 {
		buf := make([]byte, clientBuf.Reader.Buffered())
		clientBuf.Read(buf)
		backendConn.Write(buf)
	}

	// Read the HTTP 101 response from the backend and forward to the client.
	backendBuf := bufio.NewReader(backendConn)
	var respBytes bytes.Buffer
	for {
		line, err := backendBuf.ReadString('\n')
		respBytes.WriteString(line)
		if err != nil || strings.TrimRight(line, "\r\n") == "" {
			break
		}
	}
	if _, err := clientConn.Write(respBytes.Bytes()); err != nil {
		log.Printf("websocket: write 101 to client: %v", err)
		return
	}

	tunnel := make(chan struct{}, 2)
	go func() {
		defer func() { tunnel <- struct{}{} }()
		io.Copy(backendConn, clientConn)
		if tc, ok := backendConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	go func() {
		defer func() { tunnel <- struct{}{} }()
		io.Copy(clientConn, backendBuf)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	<-tunnel
}

// hopByHop are headers that must not be forwarded between proxy hops.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailers":            true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// proxyDirect forwards r straight to comfyURL, bypassing the llama-swap proxy.
// Returns true on success. On failure nothing has been written to w yet.
func (s *server) proxyDirect(w http.ResponseWriter, r *http.Request) bool {
	ref := *r.URL
	ref.Scheme = s.comfyURL.Scheme
	ref.Host = s.comfyURL.Host

	req, err := http.NewRequestWithContext(r.Context(), r.Method, ref.String(), r.Body)
	if err != nil {
		log.Printf("[direct] %s %s: build request: %v", r.Method, r.URL.RequestURI(), err)
		return false
	}
	for k, v := range r.Header {
		if !hopByHop[k] {
			req.Header[k] = v
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[direct] %s %s: %v", r.Method, r.URL.RequestURI(), err)
		return false
	}
	defer resp.Body.Close()

	log.Printf("[direct] %s %s -> %d", r.Method, r.URL.RequestURI(), resp.StatusCode)
	for k, v := range resp.Header {
		if !hopByHop[k] {
			w.Header()[k] = v
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/comfy-proxy/health":
		s.handleHealth(w, r)
		return
	case "/comfy-proxy/drain":
		s.handleDrain(w, r)
		return
	case "/comfy-proxy/shutdown":
		s.handleShutdown(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/history") {
		if s.proxyDirect(w, r) {
			s.history.record()
		} else {
			// ComfyUI not running — return empty silently without touching llama-swap.
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{}"))
		}
		return
	}

	if strings.HasPrefix(r.URL.Path, "/view") {
		if !s.proxyDirect(w, r) {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}
		return
	}

	if isWebSocketUpgrade(r) {
		s.proxyWebSocket(w, r)
		return
	}

	s.proxy.ServeHTTP(w, r)
}

// ── Drain logic ──────────────────────────────────────────────────────────────

type queueState struct {
	Running int
	Pending int
}

func (s *server) queryQueue(ctx context.Context) (queueState, error) {
	ref, _ := url.Parse("/queue")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.comfyURL.ResolveReference(ref).String(), nil)
	if err != nil {
		return queueState{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return queueState{}, err
	}
	defer resp.Body.Close()

	var body struct {
		QueueRunning []json.RawMessage `json:"queue_running"`
		QueuePending []json.RawMessage `json:"queue_pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return queueState{}, err
	}
	return queueState{Running: len(body.QueueRunning), Pending: len(body.QueuePending)}, nil
}

// freeComfyUI calls ComfyUI's /api/free to unload models and release VRAM.
// Errors are logged but non-fatal.
func (s *server) freeComfyUI(ctx context.Context) {
	ref, _ := url.Parse("/api/free")
	freeURL := s.comfyURL.ResolveReference(ref).String()

	log.Printf("[free] calling POST %s", freeURL)
	body := strings.NewReader(`{"unload_models":true,"free_memory":true}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, freeURL, body)
	if err != nil {
		log.Printf("[free] build request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[free] POST /api/free failed: %v", err)
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Printf("[free] POST /api/free returned %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
	} else {
		log.Printf("[free] ComfyUI VRAM freed")
	}
}

func (s *server) doDrain(ctx context.Context) error {
	log.Printf("[drain] polling ComfyUI queue (history quiet window: %s)", s.historyQuiet)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var prevDesc string
	for {
		shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		q, err := s.queryQueue(shortCtx)
		cancel()

		var desc string
		if err != nil {
			desc = fmt.Sprintf("queue check error: %v", err)
		} else {
			queueEmpty := q.Running == 0 && q.Pending == 0
			historySilent := s.history.silentFor(s.historyQuiet)

			if queueEmpty && historySilent {
				log.Printf("[drain] ComfyUI idle (queue empty, history quiet) — proceeding")
				break
			}

			var reasons []string
			if !queueEmpty {
				reasons = append(reasons, fmt.Sprintf("queue: %d running, %d pending", q.Running, q.Pending))
			}
			if !historySilent {
				if d, ok := s.history.sinceLastCall(); ok {
					reasons = append(reasons, fmt.Sprintf("history called %s ago (need %s quiet)", d.Round(time.Millisecond), s.historyQuiet))
				}
			}
			desc = strings.Join(reasons, "; ")
		}

		// Only log when the description changes to avoid spamming identical lines.
		if desc != prevDesc {
			log.Printf("[drain] waiting: %s", desc)
			prevDesc = desc
		}

		select {
		case <-ctx.Done():
			log.Printf("[drain] timed out: %v", ctx.Err())
			shortCtx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			if q2, err2 := s.queryQueue(shortCtx2); err2 == nil {
				log.Printf("[drain]   queue at timeout: %d running, %d pending", q2.Running, q2.Pending)
			}
			cancel2()
			if d, ok := s.history.sinceLastCall(); ok {
				log.Printf("[drain]   last /history call: %s ago (quiet window: %s)", d.Round(time.Millisecond), s.historyQuiet)
			} else {
				log.Printf("[drain]   no /history calls recorded")
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}

	s.freeComfyUI(ctx)
	return nil
}

// drainTimeout parses an optional ?timeout= query param, defaulting to 60s.
func drainTimeout(r *http.Request) time.Duration {
	if q := r.URL.Query().Get("timeout"); q != "" {
		if d, err := time.ParseDuration(q); err == nil {
			return d
		}
	}
	return 60 * time.Second
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	shortCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	q, _ := s.queryQueue(shortCtx)

	historySilent := s.history.silentFor(s.historyQuiet)
	ready := q.Running == 0 && q.Pending == 0 && historySilent

	w.Header().Set("Content-Type", "application/json")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	resp := map[string]interface{}{
		"ready":          ready,
		"queue_running":  q.Running,
		"queue_pending":  q.Pending,
		"history_silent": historySilent,
	}
	if d, ok := s.history.sinceLastCall(); ok {
		resp["history_last_call"] = d.Round(time.Millisecond).String()
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *server) handleDrain(w http.ResponseWriter, r *http.Request) {
	timeout := drainTimeout(r)
	log.Printf("[drain] called (timeout=%s)", timeout)
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	if err := s.doDrain(ctx); err != nil {
		log.Printf("[drain] failed: %v", err)
		http.Error(w, "timeout waiting to drain: "+err.Error(), http.StatusRequestTimeout)
		return
	}
	log.Printf("[drain] done")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "drained"})
}

func (s *server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	timeout := drainTimeout(r)
	log.Printf("[shutdown] called (timeout=%s)", timeout)
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	if err := s.doDrain(ctx); err != nil {
		log.Printf("[shutdown] drain failed: %v", err)
		http.Error(w, "timeout waiting to drain: "+err.Error(), http.StatusRequestTimeout)
		return
	}
	log.Printf("[shutdown] drain complete, exiting")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "shutting down"})
	// Signal main to call http.Server.Shutdown() — this lets the response be
	// fully delivered before the process exits, avoiding curl error 56.
	go func() { close(s.shutdown) }()
}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	target := flag.String("target", "http://127.0.0.1:8080", "Upstream URL (llama-swap or direct to ComfyUI if not using llama-swap)")
	comfyui := flag.String("comfyui", "http://127.0.0.1:8188", "ComfyUI direct URL (for /queue polling and /api/free)")
	addr := flag.String("addr", ":8189", "Proxy listen address")
	historyQuiet := flag.Duration("history-quiet", 5*time.Second, "How long /history must be silent before drain considers ComfyUI idle")
	flag.Parse()

	targetURL, err := url.Parse(*target)
	if err != nil {
		log.Fatalf("invalid -target URL: %v", err)
	}
	comfyURL, err := url.Parse(*comfyui)
	if err != nil {
		log.Fatalf("invalid -comfyui URL: %v", err)
	}

	srv := newServer(targetURL, comfyURL, *historyQuiet)
	httpSrv := &http.Server{Addr: *addr, Handler: srv}

	go func() {
		<-srv.shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(ctx)
	}()

	log.Printf("comfy-proxy listening on %s", *addr)
	log.Printf("  upstream:      %s", *target)
	log.Printf("  comfyui:       %s", *comfyui)
	log.Printf("  health:        GET /comfy-proxy/health")
	log.Printf("  drain:         GET /comfy-proxy/drain[?timeout=60s]")
	log.Printf("  shutdown:      GET /comfy-proxy/shutdown[?timeout=60s]")
	log.Printf("  history-quiet: %s", *historyQuiet)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
