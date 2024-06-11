package comm

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"restrelay-bench/server/logic"
	"restrelay-bench/server/version"
)

type fastURIArgs struct {
	Args *fasthttp.Args
}

func (args fastURIArgs) Get(key string) (res []string) {
	arr := args.Args.PeekMulti(key)
	res = make([]string, len(arr))
	for i := range arr {
		res[i] = string(arr[i])
	}

	return
}

type fastHTTPServer struct{}

func (srv *fastHTTPServer) ServerListenAndServe(addr string, baseURL string, nodeName string) error {
	var cb = func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/api/restrelay_bench_server/":
			tb := logic.NewTimestampBuilder(nodeName)

			tb.AddTimestamp()

			correlationID := string(ctx.Request.Header.Peek("X-Correlation-ID"))
			if correlationID == "" {
				correlationID = newCorrelationID()
			}

			requestWrapper := logic.RequestWrapper{
				Args: fastURIArgs{
					Args: ctx.Request.URI().QueryArgs(),
				},
				URI:              string(ctx.Request.RequestURI()),
				ConnectionHeader: string(ctx.Request.Header.Peek("Connection")),
			}

			reader, headers, status, err := logic.PerformActions(baseURL, requestWrapper, nodeName, &tb, correlationID)
			if err != nil {
				ctx.Error(err.Error(), fasthttp.StatusInternalServerError)

				return
			}

			ctx.SetStatusCode(status)
			for key, value := range headers {
				ctx.Response.Header.Set(key, value)
			}
			if _, err = reader.WriteTo(ctx); err != nil {
				fmt.Printf("failed writing error: %v\n", err)
			}
		case "/api/restrelay_bench_server/version":
			ctx.SetStatusCode(fasthttp.StatusOK)
			fmt.Fprint(ctx, version.Version)
		case "/api/restrelay_bench_server/type":
			ctx.SetStatusCode(fasthttp.StatusOK)
			fmt.Fprint(ctx, version.Type)
		}
	}

	return fasthttp.ListenAndServe(addr, cb) //nolint:wrapcheck
}
