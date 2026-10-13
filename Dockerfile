# syntax=docker/dockerfile:1
FROM golang:1.27.2-trixie@sha256:e58d6f83b3416618d8bcac2b3dde1b7f7e3c4a77d25e88637f8bbae81536c48d AS build
ARG SERVICE
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. CGO_ENABLED=1 go build -trimpath -ldflags="-s" -o /out/app ./$SERVICE
RUN mkdir /data-empty /out/duckdb-ext
# httpfs is fetched here so the runtime image never reaches extensions.duckdb.org
RUN if [ "$SERVICE" = pm-build ]; then /out/app ext /out/duckdb-ext; fi

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:e792ab3d241a468a4fd7519ddbbebe66b49b5f365771716ea688ad40b6c6f1c2
ENV PM_DUCKDB_EXT_DIR=/duckdb-ext
COPY --from=build --chown=65532:65532 /data-empty /data
COPY --from=build /out/duckdb-ext /duckdb-ext
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
