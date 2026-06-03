# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.22 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Build the static controller binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/controller ./cmd

# ---- runtime stage ----
# Distroless static image, runs as the non-root user 65532.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/controller /controller
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/controller"]
