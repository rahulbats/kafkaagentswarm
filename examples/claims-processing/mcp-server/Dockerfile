# Build from the repo root (see README's docker build command) - this
# package is part of the root Go module (mark3labs/mcp-go lives in the root
# go.mod/go.sum), not a standalone module, so the build context has to be
# the repo root, not this directory.
ARG BASE_IMAGE=golang:1.26
FROM ${BASE_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o mcp-server ./examples/claims-processing/mcp-server

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/mcp-server .
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/mcp-server"]
