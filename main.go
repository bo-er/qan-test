package main

import (
	"bytes"
	"context"
	_ "expvar" // register /debug/vars
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // register /debug/pprof
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	grpc_gateway "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"github.com/percona/pmm/api/qanpb"
	"github.com/percona/pmm/utils/sqlmetrics"
	"github.com/percona/pmm/version"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	channelz "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/go-sql-driver/mysql"
	"github.com/percona/qan-api2/models"
	aservice "github.com/percona/qan-api2/services/analytics"
	rservice "github.com/percona/qan-api2/services/receiver"
	"github.com/percona/qan-api2/utils/interceptors"
	"github.com/percona/qan-api2/utils/logger"
)

const shutdownTimeout = 3 * time.Second

// runGRPCServer runs gRPC server until context is canceled, then gracefully stops it.
func runGRPCServer(ctx context.Context, db *sqlx.DB, mbm *models.MetricsBucket, bind string) {
	l := logrus.WithField("component", "gRPC")
	lis, err := net.Listen("tcp", bind)
	if err != nil {
		l.Fatalf("Cannot start gRPC server on: %v", err)
	}
	l.Infof("Starting server on http://%s/ ...", bind)

	rm := models.NewReporter(db)
	mm := models.NewMetrics(db)
	grpcServer := grpc.NewServer(
		// Do not increase that value. If larger requests are required (there are errors in logs),
		// implement request slicing on pmm-managed side:
		// send B/N requests with N buckets in each instead of 1 huge request with B buckets.
		grpc.MaxRecvMsgSize(20*1024*1024), //nolint:gomnd

		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			interceptors.Unary,
			grpc_validator.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			interceptors.Stream,
			grpc_validator.StreamServerInterceptor(),
		)),
	)

	aserv := aservice.NewService(rm, mm)
	qanpb.RegisterCollectorServer(grpcServer, rservice.NewService(mbm))
	qanpb.RegisterProfileServer(grpcServer, aserv)
	qanpb.RegisterObjectDetailsServer(grpcServer, aserv)
	qanpb.RegisterMetricsNamesServer(grpcServer, aserv)
	qanpb.RegisterFiltersServer(grpcServer, aserv)
	reflection.Register(grpcServer)

	if l.Logger.GetLevel() >= logrus.DebugLevel {
		l.Debug("Reflection and channelz are enabled.")
		reflection.Register(grpcServer)
		channelz.RegisterChannelzServiceToServer(grpcServer)

		l.Debug("RPC response latency histogram enabled.")
		grpc_prometheus.EnableHandlingTimeHistogram()
	}

	grpc_prometheus.Register(grpcServer)

	// run server until it is stopped gracefully or not
	go func() {
		for {
			err = grpcServer.Serve(lis)
			if err == nil || err == grpc.ErrServerStopped {
				break
			}
			l.Errorf("Failed to serve: %s", err)
		}
		l.Info("Server stopped.")
	}()

	<-ctx.Done()

	// try to stop server gracefully, then not
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()
	grpcServer.GracefulStop()
	cancel()
}

// runJSONServer runs gRPC-gateway until context is canceled, then gracefully stops it.
func runJSONServer(ctx context.Context, grpcBindF, jsonBindF string) {
	l := logrus.WithField("component", "JSON")
	l.Infof("Starting server on http://%s/ ...", jsonBindF)

	proxyMux := grpc_gateway.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	type registrar func(context.Context, *grpc_gateway.ServeMux, string, []grpc.DialOption) error
	for _, r := range []registrar{
		qanpb.RegisterObjectDetailsHandlerFromEndpoint,
		qanpb.RegisterProfileHandlerFromEndpoint,
		qanpb.RegisterMetricsNamesHandlerFromEndpoint,
		qanpb.RegisterFiltersHandlerFromEndpoint,
	} {
		if err := r(ctx, proxyMux, grpcBindF, opts); err != nil {
			l.Panic(err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/", proxyMux)

	server := &http.Server{
		Addr:     jsonBindF,
		ErrorLog: log.New(os.Stderr, "runJSONServer: ", 0),
		Handler:  mux,
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Println("Server stopped.")
	}()

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := server.Shutdown(ctx); err != nil {
		l.Errorf("Failed to shutdown gracefully: %s \n", err)
		server.Close()
	}
	cancel()
}

// runDebugServer runs debug server until context is canceled, then gracefully stops it.
func runDebugServer(ctx context.Context, debugBindF string) {
	handler := promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{
		ErrorLog:      logrus.WithField("component", "metrics"),
		ErrorHandling: promhttp.ContinueOnError,
	})
	http.Handle("/debug/metrics", promhttp.InstrumentMetricHandler(prom.DefaultRegisterer, handler))

	l := logrus.WithField("component", "debug")

	handlers := []string{
		"/debug/metrics",  // by http.Handle above
		"/debug/vars",     // by expvar
		"/debug/requests", // by golang.org/x/net/trace imported by google.golang.org/grpc
		"/debug/events",   // by golang.org/x/net/trace imported by google.golang.org/grpc
		"/debug/pprof",    // by net/http/pprof
	}
	for i, h := range handlers {
		handlers[i] = "http://" + debugBindF + h
	}

	var buf bytes.Buffer
	err := template.Must(template.New("debug").Parse(`
	<html>
	<body>
	<ul>
	{{ range . }}
		<li><a href="{{ . }}">{{ . }}</a></li>
	{{ end }}
	</ul>
	</body>
	</html>
	`)).Execute(&buf, handlers)
	if err != nil {
		l.Panic(err)
	}
	http.HandleFunc("/debug", func(rw http.ResponseWriter, req *http.Request) {
		rw.Write(buf.Bytes())
	})
	l.Infof("Starting server on http://%s/debug\nRegistered handlers:\n\t%s", debugBindF, strings.Join(handlers, "\n\t"))

	server := &http.Server{
		Addr:     debugBindF,
		ErrorLog: log.New(os.Stderr, "runDebugServer: ", 0),
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Info("Server stopped.")
	}()

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := server.Shutdown(ctx); err != nil {
		l.Errorf("Failed to shutdown gracefully: %s", err)
	}
	cancel()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	kingpin.Version(version.ShortInfo())
	kingpin.HelpFlag.Short('h')
	grpcBindF := kingpin.Flag("grpc-bind", "GRPC bind address and port").Default("127.0.0.1:9911").String()
	jsonBindF := kingpin.Flag("json-bind", "JSON bind address and port").Default("127.0.0.1:9922").String()
	debugBindF := kingpin.Flag("listen-debug-addr", "Debug server listen address").Default("127.0.0.1:9933").String()
	dataRetentionF := kingpin.Flag("data-retention", "QAN data Retention (in days)").Default("30").Uint()
	// dsnF := kingpin.Flag("dsn", "ClickHouse database DSN").Default("mysql://127.0.0.1:5690?username=root&password=rylJIL7&database=slow_log&block_size=10000&pool_size=2").String()
	debugF := kingpin.Flag("debug", "Enable debug logging").Bool()
	traceF := kingpin.Flag("trace", "Enable trace logging (implies debug)").Bool()

	kingpin.Parse()

	log.Printf("%s.", version.ShortInfo())

	logrus.SetFormatter(&logrus.TextFormatter{
		// Enable multiline-friendly formatter in both development (with terminal) and production (without terminal):
		// https://github.com/sirupsen/logrus/blob/839c75faf7f98a33d445d181f3018b5c3409a45e/text_formatter.go#L176-L178
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000-07:00",

		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			_, function = filepath.Split(f.Function)

			// keep a single directory name as a compromise between brevity and unambiguity
			var dir string
			dir, file = filepath.Split(f.File)
			dir = filepath.Base(dir)
			file = fmt.Sprintf("%s/%s:%d", dir, file, f.Line)

			return
		},
	})

	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}
	if *traceF {
		logrus.SetLevel(logrus.TraceLevel)
		grpclog.SetLoggerV2(&logger.GRPC{Entry: logrus.WithField("component", "grpclog")})
		logrus.SetReportCaller(true)
	}
	logrus.Infof("Log level: %s.", logrus.GetLevel())

	l := logrus.WithField("component", "main")
	ctx, cancel := context.WithCancel(context.Background())
	ctx = logger.Set(ctx, "main")
	defer l.Info("Done.")

	dsn := "root:123@tcp(127.0.0.1:5690)/slow_log?charset=utf8mb4&parseTime=true&loc=Local"
	// dsn = strings.TrimSpace(dsn)
	// db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	// if err != nil {
	// 	log.Fatalf("Failed to open database:%v", err.Error())
	// }
	// db.Debug()

	db := NewDB(dsn, 5, 10)
	prom.MustRegister(sqlmetrics.NewCollector("mysql", "qan-api2", db.DB))
	// handle termination signals
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, unix.SIGTERM, unix.SIGINT)
	go func() {
		s := <-signals
		signal.Stop(signals)
		log.Printf("Got %s, shutting down...\n", unix.SignalName(s.(unix.Signal)))
		cancel()
	}()

	var wg sync.WaitGroup

	// run ingestion in a separate goroutine
	mbm := models.NewMetricsBucket(db)
	prom.MustRegister(mbm)
	mbmCtx, mbmCancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		mbm.Run(mbmCtx)
	}()

	wg.Add(1)
	go func() {
		defer func() {
			// stop ingestion only after gRPC server is fully stopped to properly insert the last batch
			mbmCancel()
			wg.Done()
		}()
		runGRPCServer(ctx, db, mbm, *grpcBindF)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runJSONServer(ctx, *grpcBindF, *jsonBindF)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runDebugServer(ctx, *debugBindF)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runClient()
	}()

	ticker := time.NewTicker(24 * time.Hour)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Drop old partitions once in 24h.
			DropOldPartition(db, *dataRetentionF)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// nothing
			}
		}
	}()
	wg.Wait()
}
