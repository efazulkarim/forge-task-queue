# ---- Build Stage ----
FROM golang:1.24-alpine AS builder

ARG BINARY_NAME=buraq

WORKDIR /src

# Cache dependency downloads in a separate layer
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source and build a statically-linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/${BINARY_NAME} .

# ---- Runtime Stage ----
FROM alpine:3.19

ARG BINARY_NAME=buraq

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /bin/${BINARY_NAME} /usr/local/bin/${BINARY_NAME}

# API port and Prometheus metrics port
EXPOSE 8080 2112

ENTRYPOINT ["/usr/local/bin/buraq"]
