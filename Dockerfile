# syntax=docker/dockerfile:1

# --- browser SDK: built here so /sdk/ (and the web/example-sdk demo) works
#     out of the box, with no host-side `npm run build` needed ---
FROM node:20-alpine AS sdk
WORKDIR /sdk
COPY sdk/package.json sdk/package-lock.json ./
RUN npm ci
COPY sdk/tsconfig.json sdk/tsup.config.ts ./
COPY sdk/src ./src
RUN npm run build

# --- server binary ---
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/pulsertc ./cmd/server

# --- runtime ---
FROM alpine:3.20
WORKDIR /app
RUN adduser -D -u 10001 pulse
COPY --from=build /out/pulsertc /app/pulsertc
COPY web /app/web
COPY --from=sdk /sdk/dist /app/sdk/dist
ENV PORT=8090
ENV WEB_DIR=/app/web
# SDK_DIST_DIR defaults to "sdk/dist", resolved from WORKDIR /app -> /app/sdk/dist.
EXPOSE 8090
USER pulse
ENTRYPOINT ["/app/pulsertc"]
