ARG REGISTRY=docker.io
FROM --platform=$BUILDPLATFORM ${REGISTRY}/golang:1.25-alpine AS builder

ARG APP_RELATIVE_PATH
ARG TARGETOS
ARG TARGETARCH

WORKDIR /data/app
COPY go.mod go.sum ./
RUN go mod download

COPY . /data/app
RUN rm -rf /data/app/bin/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o ./bin/tokenlive-gateway ${APP_RELATIVE_PATH}
RUN mv config /data/app/bin/


FROM ${REGISTRY}/alpine:3.16


RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && apk del tzdata


ARG APP_ENV
ENV APP_ENV=${APP_ENV}
ENV APP_CONF=config/prod.yml

WORKDIR /data/app
COPY --from=builder /data/app/bin /data/app

EXPOSE 8000
ENTRYPOINT [ "./tokenlive-gateway" ]

#docker build -t  1.1.1.1:5000/demo-api:v1 --build-arg APP_CONF=config/prod.yml --build-arg  APP_RELATIVE_PATH=./cmd/server/...  .
#docker run -it --rm --entrypoint=ash 1.1.1.1:5000/demo-api:v1
