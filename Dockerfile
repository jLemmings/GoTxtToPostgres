FROM golang:alpine

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/go/src/app
ENV GOBIN=/usr/local/bin
COPY . .

RUN go get github.com/lib/pq
RUN go get 	gopkg.in/cheggaaa/pb.v1
RUN go get github.com/google/pprof

RUN go build -o /go/bin/Import

EXPOSE 8080/tcp

ENTRYPOINT ["/go/bin/Import"]
