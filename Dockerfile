FROM --platform=linux/amd64 nimlang/nim:2.0.2-alpine AS builder
WORKDIR /tmp
COPY src src
COPY respite.nimble .
RUN nimble install -y
RUN nim c -d:useMalloc -d:release -o:respite src/respite.nim

FROM --platform=linux/amd64 alpine:latest
RUN apk update && apk upgrade
RUN apk add --no-cache sqlite-libs
WORKDIR /respite
COPY --from=builder /tmp/respite ./
EXPOSE 6379
CMD ["./respite"]
