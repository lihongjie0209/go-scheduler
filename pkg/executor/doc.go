// Package executor implements the Go Scheduler executor protocol.
//
// A process typically creates one Server, registers named handlers, exposes it
// through net/http, and runs a Registrar with the same process context:
//
//	server, _ := executor.NewServer(executor.Options{
//		SchedulerURL: "https://scheduler.example.com",
//	})
//	_ = server.Handle("invoiceHandler", func(ctx context.Context, task executor.Task) error {
//		_ = task.Logger.Info("processing " + task.Input)
//		return processInvoice(ctx, task.Input)
//	})
//	go http.ListenAndServe(":9999", server)
//
//	registrar, _ := executor.NewRegistrar(executor.RegistrarOptions{
//		APIURL: "https://scheduler.example.com",
//		Token: os.Getenv("SCHEDULER_TOKEN"),
//		GroupID: os.Getenv("EXECUTOR_GROUP_ID"),
//		NodeID: "invoice-worker-1",
//		Address: "https://invoice-worker-1.example.com",
//		TTL: 30 * time.Second,
//	})
//	_ = registrar.Run(ctx)
//
// Handle executes synchronously. HandleAsync returns HTTP 202 immediately and
// reports the final outcome through the one-time callback token. Both modes can
// upload rolling stdout and stderr entries through Task.Logger.
package executor
