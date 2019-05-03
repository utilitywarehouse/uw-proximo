FROM golang:1.12-alpine AS build
RUN apk update && apk add make git gcc musl-dev
WORKDIR /proximo
ADD . /proximo/

RUN go mod download
RUN make build

FROM alpine:3.9
RUN apk add --no-cache ca-certificates
COPY --from=build /proximo/proximo-server /bin/proximo-server

ENTRYPOINT [ "proximo-server" ]
