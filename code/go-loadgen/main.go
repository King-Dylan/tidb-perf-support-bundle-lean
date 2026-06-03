package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type Template struct {
	BundleID string `json:"bundle_id"`
	Group    string `json:"group"`
	SQL      string `json:"sql"`
}

type BundleRun struct {
	BundleID string        `json:"bundle_id"`
	Skip     bool          `json:"skip"`
	Params   []interface{} `json:"params"`
}

type WorkloadEvent struct {
	Index    int         `json:"index"`
	Event    string      `json:"event"`
	Kind     string      `json:"kind"`
	HotField interface{} `json:"hot_field"`
	Bundles  []BundleRun `json:"bundles"`
}

type Workload struct {
	GeneratedAtUnix  float64         `json:"generated_at_unix"`
	SourceEventsJSON string          `json:"source_events_json"`
	Mode             string          `json:"mode"`
	EventCount       int             `json:"event_count"`
	BundleCount      int             `json:"bundle_count"`
	Templates        []Template      `json:"templates"`
	Events           []WorkloadEvent `json:"events"`
}

type Task struct {
	EventIdx    int
	TemplateIdx int
	Params      []interface{}
	Skip        bool
	QueuedAt    time.Time
}

type EventState struct {
	StartNs   int64
	Remaining int64
	Successes int64
	Errors    int64
	Score60Ns int64
}

type EventResult struct {
	EventIdx    int     `json:"event_idx"`
	MS          float64 `json:"ms"`
	Score60MS   float64 `json:"score60_ms"`
	Full65MS    float64 `json:"full65_ms"`
	Successes   int64   `json:"successes"`
	Errors      int64   `json:"errors"`
	CompletedAt int64   `json:"completed_at_unix_nano"`
}

type WorkerMetrics struct {
	QueryMS         []float64
	QueueMS         []float64
	QueryByTemplate map[int][]float64
	Errors          int64
	FirstErrors     []string
}

type WorkerReady struct {
	ID    int
	OK    bool
	Error string
}

type QueryOutcome struct {
	TemplateIdx int
	QueryMS     float64
	QueueMS     float64
	Success     bool
	Error       string
}

type FanoutMetrics struct {
	mu              sync.Mutex
	QueryMS         []float64
	QueueMS         []float64
	QueryByTemplate map[int][]float64
	Errors          int64
	FirstErrors     []string
}

type RunStats struct {
	Started          time.Time
	Elapsed          time.Duration
	EventResults     []EventResult
	QueryMS          []float64
	QueueMS          []float64
	QueryByTemplate  map[int][]float64
	TotalErrors      int64
	FirstQueryErrors []string
	ReadyWorkers     int
	SetupErrors      []string
}

type ConnSlot struct {
	ID    int
	Conn  *sql.Conn
	Stmts []*sql.Stmt
}

type Summary struct {
	N       int     `json:"n"`
	P50     float64 `json:"p50,omitempty"`
	P95     float64 `json:"p95,omitempty"`
	P99     float64 `json:"p99,omitempty"`
	Avg     float64 `json:"avg,omitempty"`
	Min     float64 `json:"min,omitempty"`
	Max     float64 `json:"max,omitempty"`
	Over350 int     `json:"over_350,omitempty"`
	Over500 int     `json:"over_500,omitempty"`
}

type Result struct {
	StartedAtUnix      float64            `json:"started_at_unix"`
	ElapsedSeconds     float64            `json:"elapsed_seconds"`
	Connections        int                `json:"connections"`
	EventsSubmitted    int                `json:"events_submitted"`
	BundleCount        int                `json:"bundle_count"`
	TargetBundleQPS    int                `json:"target_bundle_qps"`
	TargetEventEPS     float64            `json:"target_event_eps,omitempty"`
	DurationSeconds    float64            `json:"duration_seconds,omitempty"`
	MaxPendingEvents   int                `json:"max_pending_events,omitempty"`
	CompletedEventEPS  float64            `json:"completed_event_eps"`
	Full65EventEPS     float64            `json:"full65_event_eps"`
	TotalTasks         int                `json:"total_tasks"`
	TotalErrors        int64              `json:"total_errors"`
	ReadTimeout        string             `json:"read_timeout"`
	QueryTimeout       string             `json:"query_timeout"`
	MaxExecutionTimeMS int                `json:"max_execution_time_ms"`
	PrepareAll         bool               `json:"prepare_all"`
	ExecutionMode      string             `json:"execution_mode"`
	StartAtUnixMS      int64              `json:"start_at_unix_ms,omitempty"`
	SetupTimeout       string             `json:"setup_timeout"`
	ReadyWorkers       int                `json:"ready_workers"`
	SetupErrors        []string           `json:"setup_errors,omitempty"`
	FirstQueryErrors   []string           `json:"first_query_errors,omitempty"`
	Summaries          map[string]Summary `json:"summaries"`
	BundleSummaries    map[string]Summary `json:"bundle_summaries,omitempty"`
	EventResults       []EventResult      `json:"event_results,omitempty"`
}

func readJSON(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	idx := int(math.Ceil(float64(len(cp))*p/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func summarize(vals []float64) Summary {
	if len(vals) == 0 {
		return Summary{N: 0}
	}
	minV, maxV, sum := vals[0], vals[0], 0.0
	over350, over500 := 0, 0
	for _, v := range vals {
		sum += v
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
		if v > 350 {
			over350++
		}
		if v > 500 {
			over500++
		}
	}
	return Summary{
		N: len(vals), P50: percentile(vals, 50), P95: percentile(vals, 95),
		P99: percentile(vals, 99), Avg: sum / float64(len(vals)),
		Min: minV, Max: maxV, Over350: over350, Over500: over500,
	}
}

func mysqlDSN(cfg DBConfig, timeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration, isolation string, maxExecMS int) string {
	c := mysql.NewConfig()
	c.User = cfg.User
	c.Passwd = cfg.Password
	c.Net = "tcp"
	c.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	c.DBName = cfg.Database
	c.AllowNativePasswords = true
	c.Timeout = timeout
	c.ReadTimeout = readTimeout
	c.WriteTimeout = writeTimeout
	c.Params = map[string]string{
		"charset":   "utf8mb4",
		"parseTime": "true",
		// Generic Params are sent as SET statements when each physical
		// connection is established, so replacement pool connections inherit
		// the same TiDB session knobs as pre-warmed ones.
		"tidb_opt_force_inline_cte": "0",
	}
	if isolation != "" {
		c.Params["tidb_isolation_read_engines"] = fmt.Sprintf("'%s'", strings.ReplaceAll(isolation, "'", "''"))
	}
	if maxExecMS > 0 {
		c.Params["max_execution_time"] = fmt.Sprintf("%d", maxExecMS)
	}
	return c.FormatDSN()
}

func configureSession(ctx context.Context, conn *sql.Conn, isolation string, maxExecMS int) error {
	if isolation != "" {
		if _, err := conn.ExecContext(ctx, "SET SESSION tidb_isolation_read_engines = ?", isolation); err != nil {
			return err
		}
	}
	if maxExecMS > 0 {
		stmt := fmt.Sprintf("SET SESSION max_execution_time = %d", maxExecMS)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION tidb_opt_force_inline_cte = 0"); err != nil {
		return err
	}
	return nil
}

func fetchRows(rows *sql.Rows) error {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]interface{}, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func mysqlPlaceholders(sqlText string) string {
	var out strings.Builder
	out.Grow(len(sqlText))
	inString := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if ch == '\'' {
			out.WriteByte(ch)
			if inString && i+1 < len(sqlText) && sqlText[i+1] == '\'' {
				i++
				out.WriteByte(sqlText[i])
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '%' && i+1 < len(sqlText) && sqlText[i+1] == 's' {
			out.WriteByte('?')
			i++
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

func worker(
	setupCtx context.Context,
	runCtx context.Context,
	id int,
	db *sql.DB,
	templates []Template,
	tasks <-chan Task,
	eventStates []EventState,
	eventDone chan<- EventResult,
	metricsCh chan<- WorkerMetrics,
	readyCh chan<- WorkerReady,
	prepareAll bool,
	isolation string,
	maxExecMS int,
	queryTimeout time.Duration,
) {
	metrics := WorkerMetrics{
		QueryMS:         make([]float64, 0, 256),
		QueueMS:         make([]float64, 0, 256),
		QueryByTemplate: make(map[int][]float64),
	}
	defer func() { metricsCh <- metrics }()

	conn, err := db.Conn(setupCtx)
	if err != nil {
		metrics.Errors++
		metrics.FirstErrors = append(metrics.FirstErrors, err.Error())
		readyCh <- WorkerReady{ID: id, OK: false, Error: err.Error()}
		return
	}
	defer conn.Close()
	if err := configureSession(setupCtx, conn, isolation, maxExecMS); err != nil {
		metrics.Errors++
		metrics.FirstErrors = append(metrics.FirstErrors, err.Error())
		readyCh <- WorkerReady{ID: id, OK: false, Error: err.Error()}
		return
	}

	stmts := make([]*sql.Stmt, len(templates))
	defer func() {
		for _, stmt := range stmts {
			if stmt != nil {
				_ = stmt.Close()
			}
		}
	}()

	prepare := func(ctx context.Context, idx int) (*sql.Stmt, error) {
		if stmts[idx] != nil {
			return stmts[idx], nil
		}
		sqlText := mysqlPlaceholders(templates[idx].SQL)
		stmt, err := conn.PrepareContext(ctx, sqlText)
		if err != nil {
			return nil, err
		}
		stmts[idx] = stmt
		return stmt, nil
	}

	if prepareAll {
		for idx := range templates {
			if _, err := prepare(setupCtx, idx); err != nil {
				metrics.Errors++
				metrics.FirstErrors = append(metrics.FirstErrors, err.Error())
				readyCh <- WorkerReady{ID: id, OK: false, Error: err.Error()}
				return
			}
		}
	}
	readyCh <- WorkerReady{ID: id, OK: true}

	for task := range tasks {
		now := time.Now()
		queueMS := float64(now.Sub(task.QueuedAt).Microseconds()) / 1000.0
		metrics.QueueMS = append(metrics.QueueMS, queueMS)
		state := &eventStates[task.EventIdx]
		success := false
		queryMS := 0.0
		if task.Skip {
			success = true
		} else {
			stmt, err := prepare(runCtx, task.TemplateIdx)
			start := time.Now()
			if err == nil {
				qctx := runCtx
				cancel := func() {}
				if queryTimeout > 0 {
					qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
				}
				rows, qerr := stmt.QueryContext(qctx, task.Params...)
				cancel()
				if qerr == nil {
					qerr = fetchRows(rows)
				}
				if qerr == nil {
					success = true
				} else {
					metrics.Errors++
					if len(metrics.FirstErrors) < 5 {
						metrics.FirstErrors = append(metrics.FirstErrors, qerr.Error())
					}
				}
			} else {
				metrics.Errors++
				if len(metrics.FirstErrors) < 5 {
					metrics.FirstErrors = append(metrics.FirstErrors, err.Error())
				}
			}
			queryMS = float64(time.Since(start).Microseconds()) / 1000.0
		}
		metrics.QueryMS = append(metrics.QueryMS, queryMS)
		metrics.QueryByTemplate[task.TemplateIdx] = append(metrics.QueryByTemplate[task.TemplateIdx], queryMS)
		completedNs := time.Now().UnixNano()
		if success {
			if atomic.AddInt64(&state.Successes, 1) == 60 {
				atomic.CompareAndSwapInt64(&state.Score60Ns, 0, completedNs)
			}
		} else {
			atomic.AddInt64(&state.Errors, 1)
		}
		if atomic.AddInt64(&state.Remaining, -1) == 0 {
			startNs := atomic.LoadInt64(&state.StartNs)
			score60Ns := atomic.LoadInt64(&state.Score60Ns)
			successes := atomic.LoadInt64(&state.Successes)
			errorsN := atomic.LoadInt64(&state.Errors)
			score60MS := -1.0
			if score60Ns > 0 {
				score60MS = float64(score60Ns-startNs) / 1e6
			}
			full65MS := -1.0
			if successes == int64(len(templates)) {
				full65MS = float64(completedNs-startNs) / 1e6
			}
			eventDone <- EventResult{
				EventIdx:    task.EventIdx,
				MS:          float64(completedNs-startNs) / 1e6,
				Score60MS:   score60MS,
				Full65MS:    full65MS,
				Successes:   successes,
				Errors:      errorsN,
				CompletedAt: completedNs,
			}
		}
	}
}

func (m *FanoutMetrics) recordBatch(outcomes []QueryOutcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, out := range outcomes {
		m.QueryMS = append(m.QueryMS, out.QueryMS)
		m.QueueMS = append(m.QueueMS, out.QueueMS)
		m.QueryByTemplate[out.TemplateIdx] = append(m.QueryByTemplate[out.TemplateIdx], out.QueryMS)
		if !out.Success {
			m.Errors++
			if out.Error != "" && len(m.FirstErrors) < 20 {
				m.FirstErrors = append(m.FirstErrors, out.Error)
			}
		}
	}
}

func prewarmPool(ctx context.Context, db *sql.DB, connections int) (int, []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	held := make([]*sql.Conn, 0, connections)
	errorsOut := make([]string, 0)

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err == nil {
				err = conn.PingContext(ctx)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if len(errorsOut) < 20 {
					errorsOut = append(errorsOut, fmt.Sprintf("worker %d: %s", workerID, err.Error()))
				}
				if conn != nil {
					_ = conn.Close()
				}
				return
			}
			held = append(held, conn)
		}(i)
	}
	wg.Wait()
	ready := len(held)
	for _, conn := range held {
		_ = conn.Close()
	}
	return ready, errorsOut
}

func runOneEventFanout(
	runCtx context.Context,
	eventIdx int,
	sourceEvent WorkloadEvent,
	templateCount int,
	templateIdx map[string]int,
	prepare func(context.Context, int) (*sql.Stmt, error),
	queryTimeout time.Duration,
	metrics *FanoutMetrics,
	eventDone chan<- EventResult,
) {
	eventStart := time.Now()
	eventStartNs := eventStart.UnixNano()
	outcomes := make([]QueryOutcome, len(sourceEvent.Bundles))
	var successes int64
	var errorsN int64
	var score60Ns int64
	var wg sync.WaitGroup

	for pos, bundle := range sourceEvent.Bundles {
		idx, ok := templateIdx[bundle.BundleID]
		if !ok {
			outcomes[pos] = QueryOutcome{TemplateIdx: -1, Success: false, Error: fmt.Sprintf("unknown bundle_id %s", bundle.BundleID)}
			atomic.AddInt64(&errorsN, 1)
			continue
		}
		outcomes[pos].TemplateIdx = idx
		wg.Add(1)
		go func(pos int, idx int, bundle BundleRun) {
			defer wg.Done()
			queueMS := float64(time.Since(eventStart).Microseconds()) / 1000.0
			success := false
			queryMS := 0.0
			errText := ""
			if bundle.Skip {
				success = true
			} else {
				stmt, err := prepare(runCtx, idx)
				start := time.Now()
				if err == nil {
					qctx := runCtx
					cancel := func() {}
					if queryTimeout > 0 {
						qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
					}
					rows, qerr := stmt.QueryContext(qctx, bundle.Params...)
					cancel()
					if qerr == nil {
						qerr = fetchRows(rows)
					}
					if qerr == nil {
						success = true
					} else {
						errText = qerr.Error()
					}
				} else {
					errText = err.Error()
				}
				queryMS = float64(time.Since(start).Microseconds()) / 1000.0
			}
			completedNs := time.Now().UnixNano()
			if success {
				if atomic.AddInt64(&successes, 1) == 60 {
					atomic.CompareAndSwapInt64(&score60Ns, 0, completedNs)
				}
			} else {
				atomic.AddInt64(&errorsN, 1)
			}
			outcomes[pos] = QueryOutcome{
				TemplateIdx: idx,
				QueryMS:     queryMS,
				QueueMS:     queueMS,
				Success:     success,
				Error:       errText,
			}
		}(pos, idx, bundle)
	}

	wg.Wait()
	metrics.recordBatch(outcomes)
	completedNs := time.Now().UnixNano()
	score60MS := -1.0
	if scoreNs := atomic.LoadInt64(&score60Ns); scoreNs > 0 {
		score60MS = float64(scoreNs-eventStartNs) / 1e6
	}
	successesN := atomic.LoadInt64(&successes)
	full65MS := -1.0
	if successesN == int64(templateCount) {
		full65MS = float64(completedNs-eventStartNs) / 1e6
	}
	eventDone <- EventResult{
		EventIdx:    eventIdx,
		MS:          float64(completedNs-eventStartNs) / 1e6,
		Score60MS:   score60MS,
		Full65MS:    full65MS,
		Successes:   successesN,
		Errors:      atomic.LoadInt64(&errorsN),
		CompletedAt: completedNs,
	}
}

func runEventFanout(
	runCtx context.Context,
	db *sql.DB,
	workload Workload,
	templateIdx map[string]int,
	eventsToRun int,
	connections int,
	setupTimeout time.Duration,
	prepareAll bool,
	startAtUnixMS int64,
	targetEventEPS float64,
	maxPending int,
	queryTimeout time.Duration,
) RunStats {
	setupStarted := time.Now()
	setupCtx, cancelSetup := context.WithTimeout(runCtx, setupTimeout)
	defer cancelSetup()
	readyWorkers, setupErrors := prewarmPool(setupCtx, db, connections)
	fmt.Printf("Workers ready=%d/%d in %.3fs\n", readyWorkers, connections, time.Since(setupStarted).Seconds())
	if readyWorkers == 0 {
		fmt.Fprintln(os.Stderr, "no ready workers")
		os.Exit(1)
	}

	stmts := make([]*sql.Stmt, len(workload.Templates))
	var stmtMu sync.Mutex
	defer func() {
		for _, stmt := range stmts {
			if stmt != nil {
				_ = stmt.Close()
			}
		}
	}()
	prepare := func(ctx context.Context, idx int) (*sql.Stmt, error) {
		stmtMu.Lock()
		defer stmtMu.Unlock()
		if stmts[idx] != nil {
			return stmts[idx], nil
		}
		sqlText := mysqlPlaceholders(workload.Templates[idx].SQL)
		stmt, err := db.PrepareContext(ctx, sqlText)
		if err != nil {
			return nil, err
		}
		stmts[idx] = stmt
		return stmt, nil
	}
	if prepareAll {
		for idx := range workload.Templates {
			if _, err := prepare(setupCtx, idx); err != nil {
				fmt.Fprintf(os.Stderr, "prepare template %d: %v\n", idx, err)
				os.Exit(1)
			}
		}
	}
	cancelSetup()

	if startAtUnixMS > 0 {
		startAt := time.UnixMilli(startAtUnixMS)
		wait := time.Until(startAt)
		if wait > 0 {
			fmt.Printf("Waiting for global start_at=%s wait=%s\n", startAt.Format(time.RFC3339Nano), wait.String())
			time.Sleep(wait)
		} else {
			fmt.Printf("Global start_at=%s already passed by %s\n", startAt.Format(time.RFC3339Nano), (-wait).String())
		}
	}

	started := time.Now()
	eventDone := make(chan EventResult, eventsToRun)
	metrics := &FanoutMetrics{
		QueryMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
		QueueMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
		QueryByTemplate: make(map[int][]float64, len(workload.Templates)),
	}
	eventResults := make([]EventResult, 0, eventsToRun)
	var completedCount int64
	var eventMu sync.Mutex
	collectorDone := make(chan struct{})
	go func() {
		for i := 0; i < eventsToRun; i++ {
			result := <-eventDone
			eventMu.Lock()
			eventResults = append(eventResults, result)
			eventMu.Unlock()
			atomic.AddInt64(&completedCount, 1)
		}
		close(collectorDone)
	}()

	steadyMode := targetEventEPS > 0
	startNs := started.UnixNano()
	for eventIdx := 0; eventIdx < eventsToRun; eventIdx++ {
		if steadyMode && maxPending > 0 {
			for int64(eventIdx)-atomic.LoadInt64(&completedCount) >= int64(maxPending) {
				time.Sleep(time.Millisecond)
			}
		}
		if steadyMode {
			dueNs := startNs + int64(float64(time.Second)*float64(eventIdx)/targetEventEPS)
			if wait := time.Until(time.Unix(0, dueNs)); wait > 0 {
				time.Sleep(wait)
			}
		}
		sourceEvent := workload.Events[eventIdx%len(workload.Events)]
		go runOneEventFanout(runCtx, eventIdx, sourceEvent, len(workload.Templates), templateIdx, prepare, queryTimeout, metrics, eventDone)
	}
	<-collectorDone
	eventMu.Lock()
	eventMu.Unlock()
	elapsed := time.Since(started)

	metrics.mu.Lock()
	stats := RunStats{
		Started:          started,
		Elapsed:          elapsed,
		EventResults:     eventResults,
		QueryMS:          append([]float64(nil), metrics.QueryMS...),
		QueueMS:          append([]float64(nil), metrics.QueueMS...),
		QueryByTemplate:  make(map[int][]float64, len(metrics.QueryByTemplate)),
		TotalErrors:      metrics.Errors,
		FirstQueryErrors: append([]string(nil), metrics.FirstErrors...),
		ReadyWorkers:     readyWorkers,
		SetupErrors:      setupErrors,
	}
	for idx, vals := range metrics.QueryByTemplate {
		stats.QueryByTemplate[idx] = append([]float64(nil), vals...)
	}
	metrics.mu.Unlock()
	return stats
}

func newConnSlot(
	ctx context.Context,
	id int,
	db *sql.DB,
	templates []Template,
	isolation string,
	maxExecMS int,
	prepareAll bool,
) (*ConnSlot, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	slot := &ConnSlot{ID: id, Conn: conn, Stmts: make([]*sql.Stmt, len(templates))}
	if err := configureSession(ctx, conn, isolation, maxExecMS); err != nil {
		slot.Close()
		return nil, err
	}
	if prepareAll {
		for idx := range templates {
			if _, err := slot.Stmt(ctx, idx, templates); err != nil {
				slot.Close()
				return nil, err
			}
		}
	}
	return slot, nil
}

func (s *ConnSlot) Stmt(ctx context.Context, idx int, templates []Template) (*sql.Stmt, error) {
	if s.Stmts[idx] != nil {
		return s.Stmts[idx], nil
	}
	sqlText := mysqlPlaceholders(templates[idx].SQL)
	stmt, err := s.Conn.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	s.Stmts[idx] = stmt
	return stmt, nil
}

func (s *ConnSlot) Close() {
	for _, stmt := range s.Stmts {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	if s.Conn != nil {
		_ = s.Conn.Close()
	}
}

func prewarmConnSlots(
	ctx context.Context,
	db *sql.DB,
	connections int,
	templates []Template,
	isolation string,
	maxExecMS int,
	prepareAll bool,
) ([]*ConnSlot, []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	slots := make([]*ConnSlot, 0, connections)
	errorsOut := make([]string, 0)

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(slotID int) {
			defer wg.Done()
			slot, err := newConnSlot(ctx, slotID, db, templates, isolation, maxExecMS, prepareAll)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if len(errorsOut) < 20 {
					errorsOut = append(errorsOut, fmt.Sprintf("worker %d: %s", slotID, err.Error()))
				}
				return
			}
			slots = append(slots, slot)
		}(i)
	}
	wg.Wait()
	return slots, errorsOut
}

func runOneEventConnFanout(
	runCtx context.Context,
	eventIdx int,
	sourceEvent WorkloadEvent,
	templateCount int,
	templateIdx map[string]int,
	templates []Template,
	readySlots chan *ConnSlot,
	queryTimeout time.Duration,
	metrics *FanoutMetrics,
	eventDone chan<- EventResult,
) {
	eventStart := time.Now()
	eventStartNs := eventStart.UnixNano()
	outcomes := make([]QueryOutcome, len(sourceEvent.Bundles))
	var successes int64
	var errorsN int64
	var score60Ns int64
	var wg sync.WaitGroup

	for pos, bundle := range sourceEvent.Bundles {
		idx, ok := templateIdx[bundle.BundleID]
		if !ok {
			outcomes[pos] = QueryOutcome{TemplateIdx: -1, Success: false, Error: fmt.Sprintf("unknown bundle_id %s", bundle.BundleID)}
			atomic.AddInt64(&errorsN, 1)
			continue
		}
		outcomes[pos].TemplateIdx = idx
		wg.Add(1)
		go func(pos int, idx int, bundle BundleRun) {
			defer wg.Done()
			success := false
			queryMS := 0.0
			errText := ""
			queueStart := time.Now()
			queueMS := 0.0
			if bundle.Skip {
				success = true
				queueMS = float64(time.Since(eventStart).Microseconds()) / 1000.0
			} else {
				slot := <-readySlots
				queueMS = float64(time.Since(queueStart).Microseconds()) / 1000.0
				start := time.Now()
				stmt, err := slot.Stmt(runCtx, idx, templates)
				if err == nil {
					qctx := runCtx
					cancel := func() {}
					if queryTimeout > 0 {
						qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
					}
					rows, qerr := stmt.QueryContext(qctx, bundle.Params...)
					cancel()
					if qerr == nil {
						qerr = fetchRows(rows)
					}
					if qerr == nil {
						success = true
					} else {
						errText = qerr.Error()
					}
				} else {
					errText = err.Error()
				}
				queryMS = float64(time.Since(start).Microseconds()) / 1000.0
				readySlots <- slot
			}
			completedNs := time.Now().UnixNano()
			if success {
				if atomic.AddInt64(&successes, 1) == 60 {
					atomic.CompareAndSwapInt64(&score60Ns, 0, completedNs)
				}
			} else {
				atomic.AddInt64(&errorsN, 1)
			}
			outcomes[pos] = QueryOutcome{
				TemplateIdx: idx,
				QueryMS:     queryMS,
				QueueMS:     queueMS,
				Success:     success,
				Error:       errText,
			}
		}(pos, idx, bundle)
	}

	wg.Wait()
	metrics.recordBatch(outcomes)
	completedNs := time.Now().UnixNano()
	score60MS := -1.0
	if scoreNs := atomic.LoadInt64(&score60Ns); scoreNs > 0 {
		score60MS = float64(scoreNs-eventStartNs) / 1e6
	}
	successesN := atomic.LoadInt64(&successes)
	full65MS := -1.0
	if successesN == int64(templateCount) {
		full65MS = float64(completedNs-eventStartNs) / 1e6
	}
	eventDone <- EventResult{
		EventIdx:    eventIdx,
		MS:          float64(completedNs-eventStartNs) / 1e6,
		Score60MS:   score60MS,
		Full65MS:    full65MS,
		Successes:   successesN,
		Errors:      atomic.LoadInt64(&errorsN),
		CompletedAt: completedNs,
	}
}

func runConnFanout(
	runCtx context.Context,
	db *sql.DB,
	workload Workload,
	templateIdx map[string]int,
	eventsToRun int,
	connections int,
	setupTimeout time.Duration,
	prepareAll bool,
	startAtUnixMS int64,
	targetEventEPS float64,
	maxPending int,
	queryTimeout time.Duration,
	isolation string,
	maxExecMS int,
) RunStats {
	setupStarted := time.Now()
	setupCtx, cancelSetup := context.WithTimeout(runCtx, setupTimeout)
	defer cancelSetup()
	slots, setupErrors := prewarmConnSlots(setupCtx, db, connections, workload.Templates, isolation, maxExecMS, prepareAll)
	readyWorkers := len(slots)
	fmt.Printf("Workers ready=%d/%d in %.3fs\n", readyWorkers, connections, time.Since(setupStarted).Seconds())
	if readyWorkers == 0 {
		fmt.Fprintln(os.Stderr, "no ready workers")
		os.Exit(1)
	}
	cancelSetup()
	defer func() {
		for _, slot := range slots {
			slot.Close()
		}
	}()
	readySlots := make(chan *ConnSlot, readyWorkers)
	for _, slot := range slots {
		readySlots <- slot
	}

	if startAtUnixMS > 0 {
		startAt := time.UnixMilli(startAtUnixMS)
		wait := time.Until(startAt)
		if wait > 0 {
			fmt.Printf("Waiting for global start_at=%s wait=%s\n", startAt.Format(time.RFC3339Nano), wait.String())
			time.Sleep(wait)
		} else {
			fmt.Printf("Global start_at=%s already passed by %s\n", startAt.Format(time.RFC3339Nano), (-wait).String())
		}
	}

	started := time.Now()
	eventDone := make(chan EventResult, eventsToRun)
	metrics := &FanoutMetrics{
		QueryMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
		QueueMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
		QueryByTemplate: make(map[int][]float64, len(workload.Templates)),
	}
	eventResults := make([]EventResult, 0, eventsToRun)
	var completedCount int64
	var eventMu sync.Mutex
	collectorDone := make(chan struct{})
	go func() {
		for i := 0; i < eventsToRun; i++ {
			result := <-eventDone
			eventMu.Lock()
			eventResults = append(eventResults, result)
			eventMu.Unlock()
			atomic.AddInt64(&completedCount, 1)
		}
		close(collectorDone)
	}()

	steadyMode := targetEventEPS > 0
	startNs := started.UnixNano()
	for eventIdx := 0; eventIdx < eventsToRun; eventIdx++ {
		if steadyMode && maxPending > 0 {
			for int64(eventIdx)-atomic.LoadInt64(&completedCount) >= int64(maxPending) {
				time.Sleep(time.Millisecond)
			}
		}
		if steadyMode {
			dueNs := startNs + int64(float64(time.Second)*float64(eventIdx)/targetEventEPS)
			if wait := time.Until(time.Unix(0, dueNs)); wait > 0 {
				time.Sleep(wait)
			}
		}
		sourceEvent := workload.Events[eventIdx%len(workload.Events)]
		go runOneEventConnFanout(runCtx, eventIdx, sourceEvent, len(workload.Templates), templateIdx, workload.Templates, readySlots, queryTimeout, metrics, eventDone)
	}
	<-collectorDone
	eventMu.Lock()
	eventMu.Unlock()
	elapsed := time.Since(started)

	metrics.mu.Lock()
	stats := RunStats{
		Started:          started,
		Elapsed:          elapsed,
		EventResults:     eventResults,
		QueryMS:          append([]float64(nil), metrics.QueryMS...),
		QueueMS:          append([]float64(nil), metrics.QueueMS...),
		QueryByTemplate:  make(map[int][]float64, len(metrics.QueryByTemplate)),
		TotalErrors:      metrics.Errors,
		FirstQueryErrors: append([]string(nil), metrics.FirstErrors...),
		ReadyWorkers:     readyWorkers,
		SetupErrors:      setupErrors,
	}
	for idx, vals := range metrics.QueryByTemplate {
		stats.QueryByTemplate[idx] = append([]float64(nil), vals...)
	}
	metrics.mu.Unlock()
	return stats
}

func emitResult(
	outputPath string,
	stats RunStats,
	templates []Template,
	connections int,
	eventsToRun int,
	totalTasks int,
	targetEventEPS float64,
	duration time.Duration,
	maxPending int,
	readTimeout time.Duration,
	queryTimeout time.Duration,
	maxExecMS int,
	prepareAll bool,
	executionMode string,
	startAtUnixMS int64,
	setupTimeout time.Duration,
	omitEvents bool,
) {
	eventMS := make([]float64, 0, len(stats.EventResults))
	score60MS := make([]float64, 0, len(stats.EventResults))
	full65MS := make([]float64, 0, len(stats.EventResults))
	for _, result := range stats.EventResults {
		eventMS = append(eventMS, result.MS)
		if result.Score60MS >= 0 {
			score60MS = append(score60MS, result.Score60MS)
		}
		if result.Full65MS >= 0 {
			full65MS = append(full65MS, result.Full65MS)
		}
	}
	full65EPS := 0.0
	if stats.Elapsed.Seconds() > 0 {
		full65EPS = float64(len(full65MS)) / stats.Elapsed.Seconds()
	}
	bundleSummaries := make(map[string]Summary, len(templates))
	for idx, tmpl := range templates {
		bundleSummaries[tmpl.BundleID] = summarize(stats.QueryByTemplate[idx])
	}
	completedEPS := 0.0
	if stats.Elapsed.Seconds() > 0 {
		completedEPS = float64(len(stats.EventResults)) / stats.Elapsed.Seconds()
	}
	result := Result{
		StartedAtUnix:      float64(stats.Started.UnixNano()) / 1e9,
		ElapsedSeconds:     stats.Elapsed.Seconds(),
		Connections:        connections,
		EventsSubmitted:    eventsToRun,
		BundleCount:        len(templates),
		TargetBundleQPS:    totalTasks,
		TargetEventEPS:     targetEventEPS,
		DurationSeconds:    duration.Seconds(),
		MaxPendingEvents:   maxPending,
		CompletedEventEPS:  completedEPS,
		Full65EventEPS:     full65EPS,
		TotalTasks:         totalTasks,
		TotalErrors:        stats.TotalErrors,
		ReadTimeout:        readTimeout.String(),
		QueryTimeout:       queryTimeout.String(),
		MaxExecutionTimeMS: maxExecMS,
		PrepareAll:         prepareAll,
		ExecutionMode:      executionMode,
		StartAtUnixMS:      startAtUnixMS,
		SetupTimeout:       setupTimeout.String(),
		ReadyWorkers:       stats.ReadyWorkers,
		SetupErrors:        stats.SetupErrors,
		FirstQueryErrors:   stats.FirstQueryErrors,
		Summaries: map[string]Summary{
			"event_completion":     summarize(eventMS),
			"score_ready_60_of_65": summarize(score60MS),
			"full_65_of_65":        summarize(full65MS),
			"query_runtime":        summarize(stats.QueryMS),
			"task_queue":           summarize(stats.QueueMS),
		},
		BundleSummaries: bundleSummaries,
	}
	if !omitEvents {
		result.EventResults = stats.EventResults
	}

	fmt.Println()
	fmt.Println("GO LOADGEN RESULTS")
	fmt.Printf("elapsed=%.3fs completed_eps=%.1f full65_eps=%.1f errors=%d ready_workers=%d/%d\n",
		result.ElapsedSeconds, result.CompletedEventEPS, result.Full65EventEPS, result.TotalErrors, result.ReadyWorkers, result.Connections)
	if len(stats.FirstQueryErrors) > 0 {
		fmt.Printf("first_error=%s\n", stats.FirstQueryErrors[0])
	}
	for _, name := range []string{"event_completion", "full_65_of_65", "score_ready_60_of_65", "query_runtime", "task_queue"} {
		s := result.Summaries[name]
		fmt.Printf("%-22s n=%d p50=%.1f p95=%.1f p99=%.1f max=%.1f >350=%d >500=%d\n",
			name, s.N, s.P50, s.P95, s.P99, s.Max, s.Over350, s.Over500)
	}
	type bundleRank struct {
		ID      string
		Summary Summary
	}
	ranked := make([]bundleRank, 0, len(templates))
	for _, tmpl := range templates {
		ranked = append(ranked, bundleRank{ID: tmpl.BundleID, Summary: bundleSummaries[tmpl.BundleID]})
	}
	sort.Slice(ranked, func(i, j int) bool {
		left, right := ranked[i].Summary, ranked[j].Summary
		if left.P95 != right.P95 {
			return left.P95 > right.P95
		}
		if left.P99 != right.P99 {
			return left.P99 > right.P99
		}
		return left.Max > right.Max
	})
	fmt.Println("top_bundle_query_runtime_by_p95")
	limit := 10
	if len(ranked) < limit {
		limit = len(ranked)
	}
	for i := 0; i < limit; i++ {
		s := ranked[i].Summary
		fmt.Printf("%2d %-20s n=%d p50=%.1f p95=%.1f p99=%.1f max=%.1f >350=%d >500=%d\n",
			i+1, ranked[i].ID, s.N, s.P50, s.P95, s.P99, s.Max, s.Over350, s.Over500)
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal result: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(outputPath, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write result: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Saved: %s\n", outputPath)
}

func ensureResultPath(path string) error {
	if path == "" {
		return errors.New("empty output path")
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func main() {
	var (
		workloadPath   = flag.String("workload", "results/go_workload_1000.json", "workload JSON generated by generate_go_workload.py")
		dbConfigPath   = flag.String("db-config", "../.db_config.json", "database config JSON")
		outputPath     = flag.String("output", "", "result JSON path")
		connections    = flag.Int("connections", 1300, "number of dedicated DB connections/workers")
		eventsLimit    = flag.Int("events", 0, "events to run; 0 means all events in workload")
		connectTimeout = flag.Duration("connect-timeout", 10*time.Second, "mysql connect timeout")
		readTimeout    = flag.Duration("read-timeout", 5*time.Second, "mysql read timeout")
		writeTimeout   = flag.Duration("write-timeout", 5*time.Second, "mysql write timeout")
		setupTimeout   = flag.Duration("setup-timeout", 60*time.Second, "maximum time to wait for workers to connect/configure/prepare")
		queryTimeout   = flag.Duration("query-timeout", 0, "per-query context timeout; 0 disables")
		maxExecMS      = flag.Int("max-execution-time-ms", 0, "SET SESSION max_execution_time; 0 disables")
		isolation      = flag.String("isolation-read-engines", "tikv,tidb", "SET SESSION tidb_isolation_read_engines")
		prepareAll     = flag.Bool("prepare-all", false, "prepare every bundle statement on every connection before timing")
		executionMode  = flag.String("execution-mode", "event-fanout", "execution mode: worker-pool, event-fanout, or conn-fanout")
		startAtUnixMS  = flag.Int64("start-at-unix-ms", 0, "wait until this Unix epoch millisecond after workers are ready before submitting tasks")
		targetEventEPS = flag.Float64("target-event-eps", 0, "steady-state event submission rate; requires --duration")
		duration       = flag.Duration("duration", 0, "steady-state submission duration; requires --target-event-eps")
		maxPending     = flag.Int("max-pending-events", 0, "cap submitted-but-not-completed events in steady mode; 0 disables")
		omitEvents     = flag.Bool("omit-event-results", true, "omit per-event results from JSON")
	)
	flag.Parse()

	var cfg DBConfig
	if err := readJSON(*dbConfigPath, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "read db config: %v\n", err)
		os.Exit(1)
	}
	if cfg.Port == 0 {
		cfg.Port = 4000
	}
	var workload Workload
	if err := readJSON(*workloadPath, &workload); err != nil {
		fmt.Fprintf(os.Stderr, "read workload: %v\n", err)
		os.Exit(1)
	}
	steadyMode := *targetEventEPS > 0 || *duration > 0
	if (*targetEventEPS > 0) != (*duration > 0) {
		fmt.Fprintln(os.Stderr, "--target-event-eps and --duration must be set together")
		os.Exit(1)
	}
	eventsToRun := *eventsLimit
	if steadyMode {
		eventsToRun = int(math.Round((*targetEventEPS) * duration.Seconds()))
		if eventsToRun < 1 {
			eventsToRun = 1
		}
	} else if eventsToRun <= 0 {
		eventsToRun = len(workload.Events)
	}
	if *outputPath == "" {
		*outputPath = fmt.Sprintf("results/go_loadgen_%d.json", time.Now().Unix())
	}
	if err := ensureResultPath(*outputPath); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}

	templateIdx := make(map[string]int, len(workload.Templates))
	for idx, tmpl := range workload.Templates {
		templateIdx[tmpl.BundleID] = idx
	}
	totalTasks := eventsToRun * len(workload.Templates)
	if *executionMode != "worker-pool" && *executionMode != "event-fanout" && *executionMode != "conn-fanout" {
		fmt.Fprintf(os.Stderr, "unknown --execution-mode %q; expected worker-pool, event-fanout, or conn-fanout\n", *executionMode)
		os.Exit(1)
	}
	fmt.Printf("Go loadgen: events=%d bundles/event=%d tasks=%d connections=%d prepare_all=%v steady=%v mode=%s\n",
		eventsToRun, len(workload.Templates), totalTasks, *connections, *prepareAll, steadyMode, *executionMode)
	fmt.Printf("DB: %s:%d db=%s user=%s read_timeout=%s query_timeout=%s max_exec_ms=%d\n",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, readTimeout.String(), queryTimeout.String(), *maxExecMS)

	db, err := sql.Open("mysql", mysqlDSN(cfg, *connectTimeout, *readTimeout, *writeTimeout, *isolation, *maxExecMS))
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(*connections)
	db.SetMaxIdleConns(*connections)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	runCtx := context.Background()
	if *executionMode == "conn-fanout" {
		stats := runConnFanout(
			runCtx,
			db,
			workload,
			templateIdx,
			eventsToRun,
			*connections,
			*setupTimeout,
			*prepareAll,
			*startAtUnixMS,
			*targetEventEPS,
			*maxPending,
			*queryTimeout,
			*isolation,
			*maxExecMS,
		)
		emitResult(
			*outputPath,
			stats,
			workload.Templates,
			*connections,
			eventsToRun,
			totalTasks,
			*targetEventEPS,
			*duration,
			*maxPending,
			*readTimeout,
			*queryTimeout,
			*maxExecMS,
			*prepareAll,
			*executionMode,
			*startAtUnixMS,
			*setupTimeout,
			*omitEvents,
		)
		return
	}
	if *executionMode == "event-fanout" {
		stats := runEventFanout(
			runCtx,
			db,
			workload,
			templateIdx,
			eventsToRun,
			*connections,
			*setupTimeout,
			*prepareAll,
			*startAtUnixMS,
			*targetEventEPS,
			*maxPending,
			*queryTimeout,
		)
		emitResult(
			*outputPath,
			stats,
			workload.Templates,
			*connections,
			eventsToRun,
			totalTasks,
			*targetEventEPS,
			*duration,
			*maxPending,
			*readTimeout,
			*queryTimeout,
			*maxExecMS,
			*prepareAll,
			*executionMode,
			*startAtUnixMS,
			*setupTimeout,
			*omitEvents,
		)
		return
	}

	setupCtx, cancelSetup := context.WithTimeout(runCtx, *setupTimeout)
	defer cancelSetup()
	taskBuffer := totalTasks
	if steadyMode {
		pendingForBuffer := *maxPending
		if pendingForBuffer <= 0 {
			pendingForBuffer = *connections
		}
		taskBuffer = pendingForBuffer * len(workload.Templates)
		if taskBuffer < len(workload.Templates) {
			taskBuffer = len(workload.Templates)
		}
	}
	tasks := make(chan Task, taskBuffer)
	eventDone := make(chan EventResult, eventsToRun)
	metricsCh := make(chan WorkerMetrics, *connections)
	readyCh := make(chan WorkerReady, *connections)
	eventStates := make([]EventState, eventsToRun)

	setupStarted := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(setupCtx, runCtx, workerID, db, workload.Templates, tasks, eventStates, eventDone, metricsCh, readyCh, *prepareAll, *isolation, *maxExecMS, *queryTimeout)
		}(i)
	}
	readyWorkers := 0
	setupErrors := make([]string, 0)
readyLoop:
	for i := 0; i < *connections; i++ {
		select {
		case ready := <-readyCh:
			if ready.OK {
				readyWorkers++
			} else if len(setupErrors) < 20 {
				setupErrors = append(setupErrors, fmt.Sprintf("worker %d: %s", ready.ID, ready.Error))
			}
		case <-setupCtx.Done():
			if len(setupErrors) < 20 {
				setupErrors = append(setupErrors, fmt.Sprintf("setup timeout after %s: %s", setupTimeout.String(), setupCtx.Err()))
			}
			break readyLoop
		}
	}
	cancelSetup()
	fmt.Printf("Workers ready=%d/%d in %.3fs\n", readyWorkers, *connections, time.Since(setupStarted).Seconds())
	if readyWorkers == 0 {
		close(tasks)
		wg.Wait()
		fmt.Fprintln(os.Stderr, "no ready workers")
		os.Exit(1)
	}
	if *startAtUnixMS > 0 {
		startAt := time.UnixMilli(*startAtUnixMS)
		wait := time.Until(startAt)
		if wait > 0 {
			fmt.Printf("Waiting for global start_at=%s wait=%s\n", startAt.Format(time.RFC3339Nano), wait.String())
			time.Sleep(wait)
		} else {
			fmt.Printf("Global start_at=%s already passed by %s\n", startAt.Format(time.RFC3339Nano), (-wait).String())
		}
	}

	started := time.Now()
	submitEvent := func(eventIdx int) {
		eventStart := time.Now()
		eventStates[eventIdx].StartNs = eventStart.UnixNano()
		eventStates[eventIdx].Remaining = int64(len(workload.Templates))
		sourceEvent := workload.Events[eventIdx%len(workload.Events)]
		for _, bundle := range sourceEvent.Bundles {
			idx, ok := templateIdx[bundle.BundleID]
			if !ok {
				continue
			}
			tasks <- Task{
				EventIdx:    eventIdx,
				TemplateIdx: idx,
				Params:      bundle.Params,
				Skip:        bundle.Skip,
				QueuedAt:    time.Now(),
			}
		}
	}

	eventResults := make([]EventResult, 0, eventsToRun)
	if steadyMode {
		var completedCount int64
		var eventMu sync.Mutex
		collectorDone := make(chan struct{})
		go func() {
			for i := 0; i < eventsToRun; i++ {
				result := <-eventDone
				eventMu.Lock()
				eventResults = append(eventResults, result)
				eventMu.Unlock()
				atomic.AddInt64(&completedCount, 1)
			}
			close(collectorDone)
		}()
		startNs := started.UnixNano()
		for eventIdx := 0; eventIdx < eventsToRun; eventIdx++ {
			if *maxPending > 0 {
				for int64(eventIdx)-atomic.LoadInt64(&completedCount) >= int64(*maxPending) {
					time.Sleep(time.Millisecond)
				}
			}
			dueNs := startNs + int64(float64(time.Second)*float64(eventIdx)/(*targetEventEPS))
			if wait := time.Until(time.Unix(0, dueNs)); wait > 0 {
				time.Sleep(wait)
			}
			submitEvent(eventIdx)
		}
		<-collectorDone
		eventMu.Lock()
		eventMu.Unlock()
	} else {
		for eventIdx := 0; eventIdx < eventsToRun; eventIdx++ {
			submitEvent(eventIdx)
		}
		for len(eventResults) < eventsToRun {
			eventResults = append(eventResults, <-eventDone)
		}
	}
	elapsed := time.Since(started)
	close(tasks)
	wg.Wait()
	close(metricsCh)

	queryMS := make([]float64, 0, totalTasks)
	queueMS := make([]float64, 0, totalTasks)
	queryByTemplate := make(map[int][]float64, len(workload.Templates))
	totalErrors := int64(0)
	firstQueryErrors := make([]string, 0)
	for wm := range metricsCh {
		queryMS = append(queryMS, wm.QueryMS...)
		queueMS = append(queueMS, wm.QueueMS...)
		for idx, vals := range wm.QueryByTemplate {
			queryByTemplate[idx] = append(queryByTemplate[idx], vals...)
		}
		totalErrors += wm.Errors
		for _, msg := range wm.FirstErrors {
			if len(firstQueryErrors) < 20 {
				firstQueryErrors = append(firstQueryErrors, msg)
			}
		}
	}

	stats := RunStats{
		Started:          started,
		Elapsed:          elapsed,
		EventResults:     eventResults,
		QueryMS:          queryMS,
		QueueMS:          queueMS,
		QueryByTemplate:  queryByTemplate,
		TotalErrors:      totalErrors,
		FirstQueryErrors: firstQueryErrors,
		ReadyWorkers:     readyWorkers,
		SetupErrors:      setupErrors,
	}
	emitResult(
		*outputPath,
		stats,
		workload.Templates,
		*connections,
		eventsToRun,
		totalTasks,
		*targetEventEPS,
		*duration,
		*maxPending,
		*readTimeout,
		*queryTimeout,
		*maxExecMS,
		*prepareAll,
		*executionMode,
		*startAtUnixMS,
		*setupTimeout,
		*omitEvents,
	)
}
