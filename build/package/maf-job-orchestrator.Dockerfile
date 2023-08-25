# syntax=docker/dockerfile:1.2

FROM golang:1.19 AS builder
ARG VERSION
RUN test -n "$VERSION"

# Must add to known hosts in order for git clone commands to succeed
RUN mkdir ~/.ssh
RUN ssh-keyscan -H github.comcast.com > ~/.ssh/known_hosts

COPY . /app
WORKDIR /app

# used for reading mongoDB with TLS
RUN wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem

# The -ldflags "-X main.version=$VERSION" portion of the build command fills in the version variable in the main package with the given version.
# see: https://www.digitalocean.com/community/tutorials/using-ldflags-to-set-version-information-for-go-applications
RUN git config --global --add url."git@github.comcast.com:".insteadOf "https://github.comcast.com/"
ENV GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOPRIVATE=github.comcast.com/mesa
RUN --mount=type=ssh go build -ldflags "-X main.version=$VERSION" -o . ./...

# Multi-stage builds: https://docs.docker.com/develop/develop-images/multistage-build/
FROM alpine
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/maf-job-orchestrator /usr/local/bin
COPY --from=builder /app/rds-combined-ca-bundle.pem /
RUN cat /rds-combined-ca-bundle.pem >> /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["maf-job-orchestrator"]
