package perfbench

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestXXLJobLoaderLoginCreateAndStart(t *testing.T) {
	t.Parallel()
	const sessionCookie = "benchmark-session"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		switch r.URL.Path {
		case "/xxl-job-admin/auth/doLogin":
			if r.Form.Get("userName") != "admin" || r.Form.Get("password") != "secret" {
				t.Errorf("login form = %v", r.Form)
			}
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: sessionCookie, Path: "/xxl-job-admin"})
			_, _ = w.Write([]byte(`{"code":200,"msg":null,"data":null}`))
		case "/xxl-job-admin/jobinfo/insert":
			assertXXLSession(t, r, sessionCookie)
			want := url.Values{"jobGroup": {"2"}, "scheduleType": {"CRON"}, "scheduleConf": {"5 4 3 2 1 ?"}, "executorHandler": {"benchmarkHandler"}, "executorParam": {"event-1"}}
			for key, values := range want {
				if r.Form.Get(key) != values[0] {
					t.Errorf("form[%s] = %q, want %q", key, r.Form.Get(key), values[0])
				}
			}
			_, _ = w.Write([]byte(`{"code":200,"msg":null,"data":"42"}`))
		case "/xxl-job-admin/jobinfo/start":
			assertXXLSession(t, r, sessionCookie)
			if r.Form.Get("ids[]") != "42" {
				t.Errorf("start form = %v", r.Form)
			}
			_, _ = w.Write([]byte(`{"code":200,"msg":null,"data":null}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	loader := &XXLJobLoader{BaseURL: server.URL + "/xxl-job-admin", Username: "admin", Password: "secret", ExecutorGroupID: 2, ExecutorHandler: "benchmarkHandler"}
	if err := loader.Login(t.Context()); err != nil {
		t.Fatal(err)
	}
	jobID, err := loader.CreateScheduledJob(t.Context(), ScheduledJob{Name: "benchmark", EventID: "event-1", ScheduledAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), SinkURL: "https://sink.example/execute"})
	if err != nil {
		t.Fatal(err)
	}
	if jobID != "42" {
		t.Fatalf("job ID = %q", jobID)
	}
}

func assertXXLSession(t *testing.T, request *http.Request, want string) {
	t.Helper()
	cookie, err := request.Cookie("XXL_JOB_LOGIN_IDENTITY")
	if err != nil || cookie.Value != want {
		t.Errorf("session cookie = %v, %v", cookie, err)
	}
}
