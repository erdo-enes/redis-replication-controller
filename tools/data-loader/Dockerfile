FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/data-loader .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/data-loader /data-loader
USER 65532:65532
ENTRYPOINT ["/data-loader"]
