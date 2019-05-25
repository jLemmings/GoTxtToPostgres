FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git


WORKDIR $GOPATH/go/src/app
ENV GOBIN=/usr/local/bin
COPY . .

RUN go get github.com/lib/pq


RUN go build -o /go/bin/Import

FROM scratch

COPY --from=builder /go/bin/Import /go/bin/Import

ENTRYPOINT ["/go/bin/HelloGo"]