package admin

import (
	"context"
	"fmt"
	"strings"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/webui/pkg/webui"
)

// --- workers ---------------------------------------------------------------------

type workerRow struct {
	ID, Endpoint, Implementation, ABIs, State, Load, Labels string
	Heartbeat                                               int
}

var (
	workerEndpoint = webui.String[workerRow]{Label: "Endpoint", Load: func(r workerRow) string { return r.Endpoint }}
	workerImpl     = webui.String[workerRow]{Label: "Implementation", Load: func(r workerRow) string { return r.Implementation }}
	workerABIs     = webui.String[workerRow]{Label: "ABIs", Load: func(r workerRow) string { return r.ABIs }}
	workerState    = webui.Select[workerRow]{Label: "State", Options: []webui.Option{
		{Value: "ready", Label: "Ready", Tone: webui.ToneOK},
		{Value: "draining", Label: "Draining", Tone: webui.ToneWarning},
		{Value: "unresponsive", Label: "Unresponsive", Tone: webui.ToneCritical},
	}, Load: func(r workerRow) []string { return []string{r.State} }}
	workerLoad      = webui.String[workerRow]{Label: "In flight", NoFilter: true, Load: func(r workerRow) string { return r.Load }}
	workerLabels    = webui.String[workerRow]{Label: "Labels", Load: func(r workerRow) string { return r.Labels }}
	workerHeartbeat = webui.Timestamp[workerRow]{Label: "Last heartbeat", Load: func(r workerRow) int { return r.Heartbeat }, Format: when}
)

func workerState_(w *controlv1.WorkerStatus) string {
	switch {
	case !w.Alive:
		return "unresponsive"
	case w.State == workerv1.WorkerState_WORKER_STATE_DRAINING:
		return "draining"
	}
	return "ready"
}

func (u *ui) workersTable(title string) webui.Table[workerRow] {
	return webui.Table[workerRow]{
		Title: title,
		Desc:  "Workers register themselves and heartbeat; one that stops heartbeating is unresponsive and receives no work.",
		Rows: func(ctx context.Context) ([]workerRow, error) {
			ws, err := u.c.Workers(ctx)
			if err != nil {
				return nil, err
			}
			rows := make([]workerRow, len(ws))
			for i, w := range ws {
				impl := w.GetInfo().GetImplementation().GetName()
				if v := w.GetInfo().GetImplementation().GetVersion(); v != "" {
					impl += " " + v
				}
				var labels []string
				for k, v := range w.GetInfo().GetLabels() {
					labels = append(labels, k+"="+v)
				}
				rows[i] = workerRow{ID: w.Id, Endpoint: w.Endpoint, Implementation: impl, State: workerState_(w),
					ABIs: strings.Join(w.GetInfo().GetAbis(), ", "), Labels: strings.Join(labels, ", "),
					Load:      fmt.Sprintf("%d / %d", w.InFlight, w.GetInfo().GetMaxConcurrency()),
					Heartbeat: int(w.LastHeartbeatTime.AsTime().Unix())}
			}
			return rows, nil
		},
		Columns: []webui.Accessor[workerRow]{workerEndpoint, workerImpl, workerABIs, workerState, workerLoad, workerLabels, workerHeartbeat},
	}
}

func (u *ui) workersPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: WorkersPath,
		Nav:  webui.Nav{Label: "Workers", Icon: "server"},
		Body: u.workersTable("Workers"),
	}
}

// --- network services ----------------------------------------------------------------

type netRow struct {
	Endpoint, Implementation, Covers, State string
	Heartbeat                               int
}

var (
	netEndpoint = webui.String[netRow]{Label: "Endpoint", Load: func(r netRow) string { return r.Endpoint }}
	netImpl     = webui.String[netRow]{Label: "Implementation", Load: func(r netRow) string { return r.Implementation }}
	netCovers   = webui.String[netRow]{Label: "Carries", NoSort: true, Load: func(r netRow) string { return r.Covers }}
	netState    = webui.Select[netRow]{Label: "State", Options: []webui.Option{
		{Value: "ready", Label: "Ready", Tone: webui.ToneOK},
		{Value: "draining", Label: "Draining", Tone: webui.ToneWarning},
		{Value: "unresponsive", Label: "Unresponsive", Tone: webui.ToneCritical},
	}, Load: func(r netRow) []string { return []string{r.State} }}
	netHeartbeat = webui.Timestamp[netRow]{Label: "Last heartbeat", Load: func(r netRow) int { return r.Heartbeat }, Format: when}
)

func netServiceState(n *controlv1.NetworkServiceStatus) string {
	switch {
	case !n.Alive:
		return "unresponsive"
	case n.State == egressv1.ServiceState_SERVICE_STATE_DRAINING:
		return "draining"
	}
	return "ready"
}

func (u *ui) networkTable(title string) webui.Table[netRow] {
	return webui.Table[netRow]{
		Title: title,
		Desc: "Network services register themselves and say which destinations they carry: one may sit inside a private network, " +
			"another on the public internet. A function's allowed destination goes to the most specific service that carries it. " +
			"One that stops heartbeating is unresponsive and is given nothing.",
		Rows: func(ctx context.Context) ([]netRow, error) {
			ns, err := u.c.NetworkServices(ctx)
			if err != nil {
				return nil, err
			}
			rows := make([]netRow, len(ns))
			for i, n := range ns {
				impl := n.GetInfo().GetImplementation().GetName()
				if v := n.GetInfo().GetImplementation().GetVersion(); v != "" {
					impl += " " + v
				}
				rows[i] = netRow{Endpoint: n.Endpoint, Implementation: impl, State: netServiceState(n),
					Covers: strings.Join(n.GetInfo().GetCovers(), ", "), Heartbeat: int(n.LastHeartbeatTime.AsTime().Unix())}
			}
			return rows, nil
		},
		Columns: []webui.Accessor[netRow]{netEndpoint, netImpl, netCovers, netState, netHeartbeat},
	}
}

func (u *ui) networkPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: NetworkPath,
		Nav:  webui.Nav{Label: "Network", Icon: "globe"},
		Body: u.networkTable("Network services"),
	}
}

// --- invocations -------------------------------------------------------------------

type invRow struct {
	ID, Function, Method, Path, Result, Failure, Worker, Log string
	Status, Duration, Memory, When                           int
}

var (
	invWhen     = webui.Timestamp[invRow]{Label: "Time", Load: func(r invRow) int { return r.When }, Format: when, NoSort: true, NoFilter: true}
	invFunction = webui.String[invRow]{Label: "Function", NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Function }}
	invMethod   = webui.String[invRow]{Label: "Method", NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Method }}
	invPath     = webui.String[invRow]{Label: "Path", NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Path }}
	invStatus   = webui.Int[invRow]{Label: "Status", NoSort: true, NoFilter: true, Load: func(r invRow) int { return r.Status },
		Format: func(n int) string {
			if n == 0 {
				return "—"
			}
			return fmt.Sprint(n)
		}}
	invResult = webui.Select[invRow]{Label: "Result", NoSort: true, NoFilter: true, Options: []webui.Option{
		{Value: "2xx", Label: "2xx", Tone: webui.ToneOK},
		{Value: "3xx", Label: "3xx"},
		{Value: "4xx", Label: "4xx", Tone: webui.ToneWarning},
		{Value: "5xx", Label: "5xx", Tone: webui.ToneCritical},
		{Value: "failed", Label: "Failed", Tone: webui.ToneCritical},
		{Value: "no response", Label: "No response", Tone: webui.ToneWarning},
	}, Load: func(r invRow) []string { return []string{r.Result} }}
	invFailure  = webui.String[invRow]{Label: "Failure", NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Failure }}
	invDuration = webui.Int[invRow]{Label: "Duration", NoSort: true, NoFilter: true, Load: func(r invRow) int { return r.Duration }, Format: humanMicros}
	invMemory   = webui.Int[invRow]{Label: "Peak memory", NoSort: true, NoFilter: true, Load: func(r invRow) int { return r.Memory }, Format: humanBytes}
	invWorker   = webui.String[invRow]{Label: "Worker", NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Worker }}
	invLog      = webui.String[invRow]{Label: "Log", Multiline: true, NoSort: true, NoFilter: true, Load: func(r invRow) string { return r.Log }}
)

// invocationsTable lists invocation records, newest first, paged by the
// orchestrator's cursor. onlyFunction picks one function; nil means all.
func (u *ui) invocationsTable(title string, pageSize int, onlyFunction func(context.Context) string) webui.Table[invRow] {
	return webui.Table[invRow]{
		Title:    title,
		PageSize: pageSize,
		Desc:     "Newest first. A failed result means the platform could not run the function; a 5xx means the function itself answered with an error.",
		Feed: func(ctx context.Context, after string, limit int) ([]invRow, string, error) {
			only := ""
			if onlyFunction != nil {
				only = onlyFunction(ctx)
			}
			recs, next, err := u.c.Invocations(ctx, only, after, limit)
			if err != nil {
				return nil, "", err
			}
			names := u.c.FunctionNames(ctx)
			rows := make([]invRow, len(recs))
			for i, r := range recs {
				fn := names[r.FunctionId]
				if fn == "" && r.FunctionId != "" {
					fn = "(deleted) " + shortID(r.FunctionId)
				}
				rows[i] = invRow{ID: r.Id, Function: fn, Method: r.Method, Path: r.Path, Status: int(r.Status),
					Result: resultClass(r.Status, r.ErrorReason), Failure: reasonLabel(r.ErrorReason),
					Duration: int(r.DurationMicros), Memory: int(r.PeakMemoryBytes), Worker: shortID(r.WorkerId),
					Log: string(r.Stderr), When: int(r.StartTime.AsTime().Unix())}
			}
			return rows, next, nil
		},
		Columns: []webui.Accessor[invRow]{invWhen, invFunction, invMethod, invPath, invStatus, invResult, invFailure, invDuration, invMemory, invWorker, invLog},
	}
}

func (u *ui) invocationsPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: InvocationsPath,
		Nav:  webui.Nav{Label: "Invocations", Icon: "clock-rotate-left"},
		Body: u.invocationsTable("Invocations", 50, nil),
	}
}

// --- overview -----------------------------------------------------------------------

type overview struct {
	Orchestrator string
	Functions    int
	Routes       int
	Workers      string
	Network      string
}

var (
	ovOrchestrator = webui.String[overview]{Label: "Orchestrator", Load: func(o overview) string { return o.Orchestrator }}
	ovFunctions    = webui.Int[overview]{Label: "Functions", Load: func(o overview) int { return o.Functions }}
	ovRoutes       = webui.Int[overview]{Label: "Routes", Load: func(o overview) int { return o.Routes }}
	ovWorkers      = webui.String[overview]{Label: "Workers", Load: func(o overview) string { return o.Workers }}
	ovNetwork      = webui.String[overview]{Label: "Network services", Load: func(o overview) string { return o.Network }}
)

func (u *ui) overviewPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: OverviewPath,
		Nav:  webui.Nav{Label: "Overview", Icon: "gauge-high"},
		Body: webui.Stack{
			webui.Form[overview]{
				Title: "System",
				Load: func(ctx context.Context) (overview, error) {
					info, err := u.c.Info(ctx)
					if err != nil {
						return overview{}, err
					}
					fns, err := u.c.Functions(ctx)
					if err != nil {
						return overview{}, err
					}
					rts, err := u.c.Routes(ctx)
					if err != nil {
						return overview{}, err
					}
					ws, err := u.c.Workers(ctx)
					if err != nil {
						return overview{}, err
					}
					nets, err := u.c.NetworkServices(ctx)
					if err != nil {
						return overview{}, err
					}
					netAlive := 0
					for _, n := range nets {
						if n.Alive {
							netAlive++
						}
					}
					alive := 0
					for _, w := range ws {
						if w.Alive {
							alive++
						}
					}
					impl := info.GetImplementation()
					return overview{
						Orchestrator: strings.TrimSpace(impl.GetName() + " " + impl.GetVersion()),
						Functions:    len(fns), Routes: len(rts), Workers: fmt.Sprintf("%d alive of %d registered", alive, len(ws)),
						Network: fmt.Sprintf("%d alive of %d registered", netAlive, len(nets)),
					}, nil
				},
				Fields: []webui.Accessor[overview]{
					webui.Group[overview]{ovOrchestrator, ovWorkers},
					webui.Group[overview]{ovFunctions, ovRoutes, ovNetwork},
				},
			},
			u.workersTable("Workers"),
			u.invocationsTable("Recent invocations", 10, nil),
		},
	}
}
