FROM public.ecr.aws/amazonlinux/amazonlinux:latest

RUN yum install -y go
RUN go get github.com/mattn/go-sqlite3
RUN go install github.com/mattn/go-sqlite3

ENV CGO_ENABLED=1

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go .
RUN go build -ldflags="-s -w" --tags sqlite_fts5
