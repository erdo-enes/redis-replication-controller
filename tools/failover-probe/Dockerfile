# Build a tiny static failover-probe image.
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/failover-probe .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/failover-probe /failover-probe
USER 65532:65532
ENTRYPOINT ["/failover-probe"]
