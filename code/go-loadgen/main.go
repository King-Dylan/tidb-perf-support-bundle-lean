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
	Index         int                    `json:"index"`
	Event         string                 `json:"event"`
	Kind          string                 `json:"kind"`
	HotField      interface{}            `json:"hot_field"`
	ReferenceTime string                 `json:"reference_time,omitempty"`
	Bindings      map[string]interface{} `json:"bindings,omitempty"`
	Bundles       []BundleRun            `json:"bundles"`
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
	StartNs      int64
	Remaining    int64
	Successes    int64
	Errors       int64
	Score60Ns    int64
	SQLMu        sync.Mutex
	SQLSuccessMS []float64
}

type EventResult struct {
	EventIdx     int     `json:"event_idx"`
	MS           float64 `json:"ms"`
	Score60MS    float64 `json:"score60_ms"`
	Full65MS     float64 `json:"full65_ms"`
	SQLScore60MS float64 `json:"sql_score60_ms"`
	SQLFull65MS  float64 `json:"sql_full65_ms"`
	Successes    int64   `json:"successes"`
	Errors       int64   `json:"errors"`
	BundlesBy350 int64   `json:"bundles_by_350_ms"`
	BundlesBy500 int64   `json:"bundles_by_500_ms"`
	CompletedAt  int64   `json:"completed_at_unix_nano"`
}

type WorkerMetrics struct {
	QueryMS         []float64
	QueueMS         []float64
	PrepareMS       []float64
	ExecMS          []float64
	DrainMS         []float64
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
	PrepareMS   float64
	ExecMS      float64
	DrainMS     float64
	Success     bool
	Error       string
}

type FanoutMetrics struct {
	mu              sync.Mutex
	QueryMS         []float64
	QueueMS         []float64
	PrepareMS       []float64
	ExecMS          []float64
	DrainMS         []float64
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
	PrepareMS        []float64
	ExecMS           []float64
	DrainMS          []float64
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
	P999    float64 `json:"p999,omitempty"`
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
	CustomerReport     CustomerReport     `json:"customer_report,omitempty"`
	EventResults       []EventResult      `json:"event_results,omitempty"`
}

type CountRate struct {
	Events  int     `json:"events"`
	Percent float64 `json:"percent"`
	EPS     float64 `json:"eps"`
}

type LatencyReport struct {
	Summary   Summary        `json:"summary"`
	Histogram map[string]int `json:"histogram"`
}

type BindingFieldStats struct {
	Distinct  int    `json:"distinct"`
	MaxRepeat int    `json:"max_repeat"`
	MaxValue  string `json:"max_value,omitempty"`
}

type RealismReport struct {
	CompletedEvents        int                          `json:"completed_events"`
	GeneratedWorkloadRows  int                          `json:"generated_workload_rows"`
	UniqueSourceEvents     int                          `json:"unique_source_events"`
	CycledWorkloadSample   bool                         `json:"cycled_workload_sample"`
	WorkloadRowReuseMin    int                          `json:"workload_row_reuse_min,omitempty"`
	WorkloadRowReuseMax    int                          `json:"workload_row_reuse_max,omitempty"`
	EventMix               map[string]int               `json:"event_mix"`
	HotFieldMix            map[string]int               `json:"hot_field_mix"`
	BindingFieldsAvailable bool                         `json:"binding_fields_available"`
	BindingFields          map[string]BindingFieldStats `json:"binding_fields,omitempty"`
}

type BundleTailReport struct {
	BundleID string  `json:"bundle_id"`
	N        int     `json:"n"`
	P95      float64 `json:"p95"`
	P99      float64 `json:"p99"`
	P999     float64 `json:"p999"`
	Max      float64 `json:"max"`
	Over350  int     `json:"over_350"`
	Over500  int     `json:"over_500"`
}

type CustomerReport struct {
	LatencyScope      string                          `json:"latency_scope,omitempty"`
	CompletedEvents   int                             `json:"completed_events,omitempty"`
	SQLOnlySLA        map[string]map[string]CountRate `json:"sql_only_sla,omitempty"`
	SQLOnlyLatency    map[string]LatencyReport        `json:"sql_only_latency,omitempty"`
	AvgBundlesBy350MS float64                         `json:"avg_bundles_by_350_ms,omitempty"`
	AvgBundlesBy500MS float64                         `json:"avg_bundles_by_500_ms,omitempty"`
	Realism           RealismReport                   `json:"test_realism,omitempty"`
	TailDrivers       []BundleTailReport              `json:"tail_drivers,omitempty"`
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
		P99: percentile(vals, 99), P999: percentile(vals, 99.9), Avg: sum / float64(len(vals)),
		Min: minV, Max: maxV, Over350: over350, Over500: over500,
	}
}

func countLE(vals []float64, threshold float64) int {
	count := 0
	for _, v := range vals {
		if v >= 0 && v <= threshold {
			count++
		}
	}
	return count
}

func countRate(vals []float64, threshold float64, totalEvents int, elapsed time.Duration) CountRate {
	events := countLE(vals, threshold)
	percent := 0.0
	if totalEvents > 0 {
		percent = 100.0 * float64(events) / float64(totalEvents)
	}
	eps := 0.0
	if elapsed.Seconds() > 0 {
		eps = float64(events) / elapsed.Seconds()
	}
	return CountRate{Events: events, Percent: percent, EPS: eps}
}

func latencyHistogram(vals []float64, totalEvents int) map[string]int {
	hist := map[string]int{
		"0-50ms":     0,
		"50-100ms":   0,
		"100-150ms":  0,
		"150-200ms":  0,
		"200-350ms":  0,
		"350-500ms":  0,
		">500/error": 0,
	}
	valid := 0
	for _, v := range vals {
		if v < 0 {
			continue
		}
		valid++
		switch {
		case v <= 50:
			hist["0-50ms"]++
		case v <= 100:
			hist["50-100ms"]++
		case v <= 150:
			hist["100-150ms"]++
		case v <= 200:
			hist["150-200ms"]++
		case v <= 350:
			hist["200-350ms"]++
		case v <= 500:
			hist["350-500ms"]++
		default:
			hist[">500/error"]++
		}
	}
	if totalEvents > valid {
		hist[">500/error"] += totalEvents - valid
	}
	return hist
}

func valueKey(v interface{}) string {
	if v == nil {
		return "<null>"
	}
	return fmt.Sprintf("%v", v)
}

func incrementCounter(counter map[string]int, key string) {
	if key == "" {
		key = "<empty>"
	}
	counter[key]++
}

func bindingStats(values map[string]map[string]int) map[string]BindingFieldStats {
	out := make(map[string]BindingFieldStats, len(values))
	for field, counts := range values {
		maxValue := ""
		maxRepeat := 0
		for value, count := range counts {
			if count > maxRepeat || (count == maxRepeat && value < maxValue) {
				maxValue = value
				maxRepeat = count
			}
		}
		out[field] = BindingFieldStats{
			Distinct:  len(counts),
			MaxRepeat: maxRepeat,
			MaxValue:  maxValue,
		}
	}
	return out
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

func elapsedMS(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
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
		PrepareMS:       make([]float64, 0, 256),
		ExecMS:          make([]float64, 0, 256),
		DrainMS:         make([]float64, 0, 256),
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
		prepareMS := 0.0
		execMS := 0.0
		drainMS := 0.0
		if task.Skip {
			success = true
		} else {
			prepareStarted := time.Now()
			stmt, err := prepare(runCtx, task.TemplateIdx)
			prepareMS = elapsedMS(prepareStarted)
			if err == nil {
				qctx := runCtx
				cancel := func() {}
				if queryTimeout > 0 {
					qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
				}
				execStarted := time.Now()
				rows, qerr := stmt.QueryContext(qctx, task.Params...)
				cancel()
				execMS = elapsedMS(execStarted)
				if qerr == nil {
					drainStarted := time.Now()
					qerr = fetchRows(rows)
					drainMS = elapsedMS(drainStarted)
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
			queryMS = execMS + drainMS
		}
		metrics.QueryMS = append(metrics.QueryMS, queryMS)
		metrics.QueryByTemplate[task.TemplateIdx] = append(metrics.QueryByTemplate[task.TemplateIdx], queryMS)
		metrics.PrepareMS = append(metrics.PrepareMS, prepareMS)
		metrics.ExecMS = append(metrics.ExecMS, execMS)
		metrics.DrainMS = append(metrics.DrainMS, drainMS)
		completedNs := time.Now().UnixNano()
		startNs := atomic.LoadInt64(&state.StartNs)
		if success {
			state.recordSQLSuccess(queryMS)
			if atomic.AddInt64(&state.Successes, 1) == 60 {
				atomic.CompareAndSwapInt64(&state.Score60Ns, 0, completedNs)
			}
		} else {
			atomic.AddInt64(&state.Errors, 1)
		}
		if atomic.AddInt64(&state.Remaining, -1) == 0 {
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
			sqlScore60MS, sqlFull65MS, sqlBundlesBy350, sqlBundlesBy500 := state.sqlOnlyTimings(len(templates))
			eventDone <- EventResult{
				EventIdx:     task.EventIdx,
				MS:           float64(completedNs-startNs) / 1e6,
				Score60MS:    score60MS,
				Full65MS:     full65MS,
				SQLScore60MS: sqlScore60MS,
				SQLFull65MS:  sqlFull65MS,
				Successes:    successes,
				Errors:       errorsN,
				BundlesBy350: sqlBundlesBy350,
				BundlesBy500: sqlBundlesBy500,
				CompletedAt:  completedNs,
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
		m.PrepareMS = append(m.PrepareMS, out.PrepareMS)
		m.ExecMS = append(m.ExecMS, out.ExecMS)
		m.DrainMS = append(m.DrainMS, out.DrainMS)
		m.QueryByTemplate[out.TemplateIdx] = append(m.QueryByTemplate[out.TemplateIdx], out.QueryMS)
		if !out.Success {
			m.Errors++
			if out.Error != "" && len(m.FirstErrors) < 20 {
				m.FirstErrors = append(m.FirstErrors, out.Error)
			}
		}
	}
}

func (state *EventState) recordSQLSuccess(queryMS float64) {
	state.SQLMu.Lock()
	state.SQLSuccessMS = append(state.SQLSuccessMS, queryMS)
	state.SQLMu.Unlock()
}

func (state *EventState) sqlOnlyTimings(templateCount int) (float64, float64, int64, int64) {
	state.SQLMu.Lock()
	successMS := append([]float64(nil), state.SQLSuccessMS...)
	state.SQLMu.Unlock()
	return sqlOnlyTimingsFromSuccessMS(successMS, templateCount)
}

func sqlOnlyTimingsFromSuccessMS(successMS []float64, templateCount int) (float64, float64, int64, int64) {
	bundlesBy350 := int64(countLE(successMS, 350))
	bundlesBy500 := int64(countLE(successMS, 500))
	score60MS := -1.0
	full65MS := -1.0
	if len(successMS) >= 60 {
		sort.Float64s(successMS)
		score60MS = successMS[59]
	}
	if len(successMS) == templateCount {
		full65MS = successMS[len(successMS)-1]
	}
	return score60MS, full65MS, bundlesBy350, bundlesBy500
}

func sqlOnlyEventTimings(outcomes []QueryOutcome, templateCount int) (float64, float64, int64, int64) {
	successMS := make([]float64, 0, len(outcomes))
	for _, out := range outcomes {
		if out.Success {
			successMS = append(successMS, out.QueryMS)
		}
	}
	return sqlOnlyTimingsFromSuccessMS(successMS, templateCount)
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
	var bundlesBy350 int64
	var bundlesBy500 int64
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
			prepareMS := 0.0
			execMS := 0.0
			drainMS := 0.0
			errText := ""
			if bundle.Skip {
				success = true
			} else {
				prepareStarted := time.Now()
				stmt, err := prepare(runCtx, idx)
				prepareMS = elapsedMS(prepareStarted)
				if err == nil {
					qctx := runCtx
					cancel := func() {}
					if queryTimeout > 0 {
						qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
					}
					execStarted := time.Now()
					rows, qerr := stmt.QueryContext(qctx, bundle.Params...)
					cancel()
					execMS = elapsedMS(execStarted)
					if qerr == nil {
						drainStarted := time.Now()
						qerr = fetchRows(rows)
						drainMS = elapsedMS(drainStarted)
					}
					if qerr == nil {
						success = true
					} else {
						errText = qerr.Error()
					}
				} else {
					errText = err.Error()
				}
				queryMS = execMS + drainMS
			}
			completedNs := time.Now().UnixNano()
			eventElapsedMS := float64(completedNs-eventStartNs) / 1e6
			if success {
				if eventElapsedMS <= 350 {
					atomic.AddInt64(&bundlesBy350, 1)
				}
				if eventElapsedMS <= 500 {
					atomic.AddInt64(&bundlesBy500, 1)
				}
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
				PrepareMS:   prepareMS,
				ExecMS:      execMS,
				DrainMS:     drainMS,
				Success:     success,
				Error:       errText,
			}
		}(pos, idx, bundle)
	}

	wg.Wait()
	metrics.recordBatch(outcomes)
	completedNs := time.Now().UnixNano()
	sqlScore60MS, sqlFull65MS, sqlBundlesBy350, sqlBundlesBy500 := sqlOnlyEventTimings(outcomes, templateCount)
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
		EventIdx:     eventIdx,
		MS:           float64(completedNs-eventStartNs) / 1e6,
		Score60MS:    score60MS,
		Full65MS:     full65MS,
		SQLScore60MS: sqlScore60MS,
		SQLFull65MS:  sqlFull65MS,
		Successes:    successesN,
		Errors:       atomic.LoadInt64(&errorsN),
		BundlesBy350: sqlBundlesBy350,
		BundlesBy500: sqlBundlesBy500,
		CompletedAt:  completedNs,
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
		PrepareMS:       make([]float64, 0, eventsToRun*len(workload.Templates)),
		ExecMS:          make([]float64, 0, eventsToRun*len(workload.Templates)),
		DrainMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
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
		PrepareMS:        append([]float64(nil), metrics.PrepareMS...),
		ExecMS:           append([]float64(nil), metrics.ExecMS...),
		DrainMS:          append([]float64(nil), metrics.DrainMS...),
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
	var bundlesBy350 int64
	var bundlesBy500 int64
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
			prepareMS := 0.0
			execMS := 0.0
			drainMS := 0.0
			errText := ""
			queueStart := time.Now()
			queueMS := 0.0
			if bundle.Skip {
				success = true
				queueMS = float64(time.Since(eventStart).Microseconds()) / 1000.0
			} else {
				slot := <-readySlots
				queueMS = float64(time.Since(queueStart).Microseconds()) / 1000.0
				prepareStarted := time.Now()
				stmt, err := slot.Stmt(runCtx, idx, templates)
				prepareMS = elapsedMS(prepareStarted)
				if err == nil {
					qctx := runCtx
					cancel := func() {}
					if queryTimeout > 0 {
						qctx, cancel = context.WithTimeout(runCtx, queryTimeout)
					}
					execStarted := time.Now()
					rows, qerr := stmt.QueryContext(qctx, bundle.Params...)
					cancel()
					execMS = elapsedMS(execStarted)
					if qerr == nil {
						drainStarted := time.Now()
						qerr = fetchRows(rows)
						drainMS = elapsedMS(drainStarted)
					}
					if qerr == nil {
						success = true
					} else {
						errText = qerr.Error()
					}
				} else {
					errText = err.Error()
				}
				queryMS = execMS + drainMS
				readySlots <- slot
			}
			completedNs := time.Now().UnixNano()
			eventElapsedMS := float64(completedNs-eventStartNs) / 1e6
			if success {
				if eventElapsedMS <= 350 {
					atomic.AddInt64(&bundlesBy350, 1)
				}
				if eventElapsedMS <= 500 {
					atomic.AddInt64(&bundlesBy500, 1)
				}
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
				PrepareMS:   prepareMS,
				ExecMS:      execMS,
				DrainMS:     drainMS,
				Success:     success,
				Error:       errText,
			}
		}(pos, idx, bundle)
	}

	wg.Wait()
	metrics.recordBatch(outcomes)
	completedNs := time.Now().UnixNano()
	sqlScore60MS, sqlFull65MS, sqlBundlesBy350, sqlBundlesBy500 := sqlOnlyEventTimings(outcomes, templateCount)
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
		EventIdx:     eventIdx,
		MS:           float64(completedNs-eventStartNs) / 1e6,
		Score60MS:    score60MS,
		Full65MS:     full65MS,
		SQLScore60MS: sqlScore60MS,
		SQLFull65MS:  sqlFull65MS,
		Successes:    successesN,
		Errors:       atomic.LoadInt64(&errorsN),
		BundlesBy350: sqlBundlesBy350,
		BundlesBy500: sqlBundlesBy500,
		CompletedAt:  completedNs,
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
		PrepareMS:       make([]float64, 0, eventsToRun*len(workload.Templates)),
		ExecMS:          make([]float64, 0, eventsToRun*len(workload.Templates)),
		DrainMS:         make([]float64, 0, eventsToRun*len(workload.Templates)),
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
		PrepareMS:        append([]float64(nil), metrics.PrepareMS...),
		ExecMS:           append([]float64(nil), metrics.ExecMS...),
		DrainMS:          append([]float64(nil), metrics.DrainMS...),
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

func buildRealismReport(workload Workload, eventsToRun int, completedEvents int) RealismReport {
	report := RealismReport{
		CompletedEvents:       completedEvents,
		GeneratedWorkloadRows: len(workload.Events),
		EventMix:              make(map[string]int),
		HotFieldMix:           make(map[string]int),
		BindingFields:         make(map[string]BindingFieldStats),
	}
	if len(workload.Events) == 0 || eventsToRun <= 0 {
		return report
	}
	uniqueEvents := make(map[string]struct{})
	bindingValues := make(map[string]map[string]int)
	reuseCounts := make([]int, len(workload.Events))
	for idx := 0; idx < eventsToRun; idx++ {
		workloadIdx := idx % len(workload.Events)
		reuseCounts[workloadIdx]++
		event := workload.Events[workloadIdx]
		if event.Event != "" {
			uniqueEvents[event.Event] = struct{}{}
		}
		incrementCounter(report.EventMix, event.Kind)
		incrementCounter(report.HotFieldMix, valueKey(event.HotField))
		if len(event.Bindings) > 0 {
			report.BindingFieldsAvailable = true
			for field, value := range event.Bindings {
				if _, ok := bindingValues[field]; !ok {
					bindingValues[field] = make(map[string]int)
				}
				bindingValues[field][valueKey(value)]++
			}
		}
	}
	report.UniqueSourceEvents = len(uniqueEvents)
	report.CycledWorkloadSample = eventsToRun > len(workload.Events)
	report.WorkloadRowReuseMin = reuseCounts[0]
	report.WorkloadRowReuseMax = reuseCounts[0]
	for _, count := range reuseCounts {
		if count < report.WorkloadRowReuseMin {
			report.WorkloadRowReuseMin = count
		}
		if count > report.WorkloadRowReuseMax {
			report.WorkloadRowReuseMax = count
		}
	}
	if report.BindingFieldsAvailable {
		report.BindingFields = bindingStats(bindingValues)
	}
	return report
}

func topTailDrivers(bundleSummaries map[string]Summary, limit int) []BundleTailReport {
	type item struct {
		ID      string
		Summary Summary
	}
	items := make([]item, 0, len(bundleSummaries))
	for id, summary := range bundleSummaries {
		items = append(items, item{ID: id, Summary: summary})
	}
	sort.Slice(items, func(i, j int) bool {
		left, right := items[i].Summary, items[j].Summary
		if left.P999 != right.P999 {
			return left.P999 > right.P999
		}
		if left.P99 != right.P99 {
			return left.P99 > right.P99
		}
		if left.P95 != right.P95 {
			return left.P95 > right.P95
		}
		return left.Max > right.Max
	})
	if len(items) < limit {
		limit = len(items)
	}
	out := make([]BundleTailReport, 0, limit)
	for _, item := range items[:limit] {
		s := item.Summary
		out = append(out, BundleTailReport{
			BundleID: item.ID,
			N:        s.N,
			P95:      s.P95,
			P99:      s.P99,
			P999:     s.P999,
			Max:      s.Max,
			Over350:  s.Over350,
			Over500:  s.Over500,
		})
	}
	return out
}

func buildCustomerReport(
	workload Workload,
	stats RunStats,
	sqlScore60MS []float64,
	sqlFull65MS []float64,
	bundlesBy350 []float64,
	bundlesBy500 []float64,
	bundleSummaries map[string]Summary,
	eventsToRun int,
) CustomerReport {
	totalEvents := len(stats.EventResults)
	return CustomerReport{
		LatencyScope:    "sql_only_query_runtime_ms: per-bundle query_runtime = db_exec + result_drain; excludes client queue, prepare, scheduling, and event fan-in wall time",
		CompletedEvents: totalEvents,
		SQLOnlySLA: map[string]map[string]CountRate{
			"score_ready_60_of_65": {
				"350ms": countRate(sqlScore60MS, 350, totalEvents, stats.Elapsed),
				"500ms": countRate(sqlScore60MS, 500, totalEvents, stats.Elapsed),
			},
			"full_65_of_65": {
				"350ms": countRate(sqlFull65MS, 350, totalEvents, stats.Elapsed),
				"500ms": countRate(sqlFull65MS, 500, totalEvents, stats.Elapsed),
			},
		},
		SQLOnlyLatency: map[string]LatencyReport{
			"score_ready_60_of_65": {
				Summary:   summarize(sqlScore60MS),
				Histogram: latencyHistogram(sqlScore60MS, totalEvents),
			},
			"full_65_of_65": {
				Summary:   summarize(sqlFull65MS),
				Histogram: latencyHistogram(sqlFull65MS, totalEvents),
			},
		},
		AvgBundlesBy350MS: summarize(bundlesBy350).Avg,
		AvgBundlesBy500MS: summarize(bundlesBy500).Avg,
		Realism:           buildRealismReport(workload, eventsToRun, totalEvents),
		TailDrivers:       topTailDrivers(bundleSummaries, 10),
	}
}

func printCountRate(label string, count CountRate) {
	fmt.Printf("  %-28s events=%d pct=%.2f%% eps=%.2f\n", label, count.Events, count.Percent, count.EPS)
}

func printHistogram(label string, hist map[string]int) {
	order := []string{"0-50ms", "50-100ms", "100-150ms", "150-200ms", "200-350ms", "350-500ms", ">500/error"}
	fmt.Printf("  %s", label)
	for _, bucket := range order {
		fmt.Printf(" %s=%d", bucket, hist[bucket])
	}
	fmt.Println()
}

func printCustomerReport(report CustomerReport) {
	fmt.Println()
	fmt.Println("CUSTOMER SQL-ONLY EVENT REPORT")
	fmt.Printf("latency_scope=%s\n", report.LatencyScope)
	fmt.Printf("completed_events=%d\n", report.CompletedEvents)
	fmt.Println("sql_only_sla")
	printCountRate("score_ready_60 <=350ms", report.SQLOnlySLA["score_ready_60_of_65"]["350ms"])
	printCountRate("score_ready_60 <=500ms", report.SQLOnlySLA["score_ready_60_of_65"]["500ms"])
	printCountRate("full_65 <=350ms", report.SQLOnlySLA["full_65_of_65"]["350ms"])
	printCountRate("full_65 <=500ms", report.SQLOnlySLA["full_65_of_65"]["500ms"])
	fmt.Printf("avg_bundles_by_350ms=%.2f avg_bundles_by_500ms=%.2f\n", report.AvgBundlesBy350MS, report.AvgBundlesBy500MS)
	fmt.Println("sql_only_latency")
	for _, name := range []string{"score_ready_60_of_65", "full_65_of_65"} {
		s := report.SQLOnlyLatency[name].Summary
		fmt.Printf("  %-22s n=%d p50=%.1f p95=%.1f p99=%.1f p999=%.1f max=%.1f\n",
			name, s.N, s.P50, s.P95, s.P99, s.P999, s.Max)
		printHistogram(name+"_histogram", report.SQLOnlyLatency[name].Histogram)
	}
	r := report.Realism
	fmt.Println("test_realism")
	fmt.Printf("  generated_workload_rows=%d unique_source_events=%d cycled=%v row_reuse_min=%d row_reuse_max=%d\n",
		r.GeneratedWorkloadRows, r.UniqueSourceEvents, r.CycledWorkloadSample, r.WorkloadRowReuseMin, r.WorkloadRowReuseMax)
	fmt.Printf("  event_mix=%v hot_field_mix=%v\n", r.EventMix, r.HotFieldMix)
	if r.BindingFieldsAvailable {
		fmt.Println("  binding_fields")
		fields := make([]string, 0, len(r.BindingFields))
		for field := range r.BindingFields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			s := r.BindingFields[field]
			fmt.Printf("    %-36s distinct=%d max_repeat=%d max_value=%s\n", field, s.Distinct, s.MaxRepeat, s.MaxValue)
		}
	} else {
		fmt.Println("  binding_fields=unavailable_in_workload_json")
	}
	fmt.Println("tail_drivers_by_bundle_p999")
	for i, item := range report.TailDrivers {
		fmt.Printf("  %2d %-20s n=%d p95=%.1f p99=%.1f p999=%.1f max=%.1f >350=%d >500=%d\n",
			i+1, item.BundleID, item.N, item.P95, item.P99, item.P999, item.Max, item.Over350, item.Over500)
	}
}

func emitResult(
	outputPath string,
	workload Workload,
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
	sqlScore60MS := make([]float64, 0, len(stats.EventResults))
	sqlFull65MS := make([]float64, 0, len(stats.EventResults))
	bundlesBy350 := make([]float64, 0, len(stats.EventResults))
	bundlesBy500 := make([]float64, 0, len(stats.EventResults))
	for _, result := range stats.EventResults {
		eventMS = append(eventMS, result.MS)
		if result.BundlesBy350 >= 0 {
			bundlesBy350 = append(bundlesBy350, float64(result.BundlesBy350))
		}
		if result.BundlesBy500 >= 0 {
			bundlesBy500 = append(bundlesBy500, float64(result.BundlesBy500))
		}
		if result.Score60MS >= 0 {
			score60MS = append(score60MS, result.Score60MS)
		}
		if result.Full65MS >= 0 {
			full65MS = append(full65MS, result.Full65MS)
		}
		if result.SQLScore60MS >= 0 {
			sqlScore60MS = append(sqlScore60MS, result.SQLScore60MS)
		}
		if result.SQLFull65MS >= 0 {
			sqlFull65MS = append(sqlFull65MS, result.SQLFull65MS)
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
	customerReport := buildCustomerReport(workload, stats, sqlScore60MS, sqlFull65MS, bundlesBy350, bundlesBy500, bundleSummaries, eventsToRun)
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
			"event_completion":              summarize(eventMS),
			"score_ready_60_of_65":          summarize(score60MS),
			"full_65_of_65":                 summarize(full65MS),
			"sql_only_score_ready_60_of_65": summarize(sqlScore60MS),
			"sql_only_full_65_of_65":        summarize(sqlFull65MS),
			"query_runtime":                 summarize(stats.QueryMS),
			"task_queue":                    summarize(stats.QueueMS),
			"prepare_runtime":               summarize(stats.PrepareMS),
			"db_exec":                       summarize(stats.ExecMS),
			"result_drain":                  summarize(stats.DrainMS),
			"bundles_by_350ms":              summarize(bundlesBy350),
			"bundles_by_500ms":              summarize(bundlesBy500),
		},
		BundleSummaries: bundleSummaries,
		CustomerReport:  customerReport,
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
	for _, name := range []string{"event_completion", "full_65_of_65", "score_ready_60_of_65", "sql_only_full_65_of_65", "sql_only_score_ready_60_of_65", "query_runtime", "task_queue", "prepare_runtime", "db_exec", "result_drain"} {
		s := result.Summaries[name]
		fmt.Printf("%-22s n=%d p50=%.1f p95=%.1f p99=%.1f p999=%.1f max=%.1f >350=%d >500=%d\n",
			name, s.N, s.P50, s.P95, s.P99, s.P999, s.Max, s.Over350, s.Over500)
	}
	for _, name := range []string{"bundles_by_350ms", "bundles_by_500ms"} {
		s := result.Summaries[name]
		fmt.Printf("%-22s n=%d avg=%.2f p50=%.1f p95=%.1f p99=%.1f max=%.1f\n",
			name, s.N, s.Avg, s.P50, s.P95, s.P99, s.Max)
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
		fmt.Printf("%2d %-20s n=%d p50=%.1f p95=%.1f p99=%.1f p999=%.1f max=%.1f >350=%d >500=%d\n",
			i+1, ranked[i].ID, s.N, s.P50, s.P95, s.P99, s.P999, s.Max, s.Over350, s.Over500)
	}
	printCustomerReport(customerReport)

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
			workload,
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
			workload,
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
	prepareMS := make([]float64, 0, totalTasks)
	execMS := make([]float64, 0, totalTasks)
	drainMS := make([]float64, 0, totalTasks)
	queryByTemplate := make(map[int][]float64, len(workload.Templates))
	totalErrors := int64(0)
	firstQueryErrors := make([]string, 0)
	for wm := range metricsCh {
		queryMS = append(queryMS, wm.QueryMS...)
		queueMS = append(queueMS, wm.QueueMS...)
		prepareMS = append(prepareMS, wm.PrepareMS...)
		execMS = append(execMS, wm.ExecMS...)
		drainMS = append(drainMS, wm.DrainMS...)
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
		PrepareMS:        prepareMS,
		ExecMS:           execMS,
		DrainMS:          drainMS,
		QueryByTemplate:  queryByTemplate,
		TotalErrors:      totalErrors,
		FirstQueryErrors: firstQueryErrors,
		ReadyWorkers:     readyWorkers,
		SetupErrors:      setupErrors,
	}
	emitResult(
		*outputPath,
		workload,
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
