FROM golang:1.26.0-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/chronos ./cmd/chronos

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/chronos /usr/local/bin/chronos

EXPOSE 7420

ENTRYPOINT ["chronos"]
