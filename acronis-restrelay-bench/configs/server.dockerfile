FROM alpine:3.7

EXPOSE 8080

ADD restrelay-bench-server /go/bin/server

ENTRYPOINT /go/bin/server --postgres-host="postgresql-service"