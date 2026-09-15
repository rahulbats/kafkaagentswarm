FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o mcp-server main.go

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /app/mcp-server .
EXPOSE 8000
ENTRYPOINT ["/mcp-server"]
