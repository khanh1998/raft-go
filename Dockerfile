# syntax=docker/dockerfile:1

# Build the application from source
FROM golang:1.22 AS build-stage

ARG EXT_NAME
WORKDIR /app

COPY . ./
RUN go mod download
RUN go mod verify

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/${EXT_NAME} ./extensions/${EXT_NAME}

RUN chmod -R 755 /app

# Deploy the application binary into a lean image
FROM alpine:latest AS build-release-stage

ARG EXT_NAME
WORKDIR /app

COPY --from=build-stage /app/${EXT_NAME} .
