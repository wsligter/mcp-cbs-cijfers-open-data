# --- build stage ---
FROM golang:1.22 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o mcp-cbs ./cmd/mcp-cbs

# --- runtime stage ---
FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=build /app/mcp-cbs /app/mcp-cbs
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/mcp-cbs","--stdio=false","--sse"]
