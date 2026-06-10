FROM golang:1.21-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o openrelay ./cmd/openrelay

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/openrelay .
EXPOSE 7777
EXPOSE 7778
EXPOSE 7779/udp
ENV OPENRELAY_TRANSPORT=both
ENV OPENRELAY_ADDR=:7777
ENV OPENRELAY_UDP_ADDR=:7779
ENV OPENRELAY_API_ADDR=:7778
ENV OPENRELAY_PUBLIC_HOST=localhost:7777
ENV OPENRELAY_UDP_PUBLIC_ADDR=localhost:7779
ENTRYPOINT ["./openrelay"]
