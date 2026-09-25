# Multi-stage build shared by every Go service in this repo.
#
# The SERVICE build arg selects which cmd/ binary to build, so one Dockerfile
# produces all five services (command-service, query-service, projector, relay,
# saga). The final image is a tiny distroless-style scratch-plus-certs image.

# ---- build stage ----
FROM golang:1.23 AS build

WORKDIR /src

# Cache dependencies first: this layer only rebuilds when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Then copy the source and build the selected service.
COPY . .
ARG SERVICE
# CGO disabled + static build so the binary runs on a minimal base image.
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o /out/app ./cmd/${SERVICE}

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
# Migrations are read at startup by the services that own a schema.
COPY migrations /migrations
ENTRYPOINT ["/app"]
