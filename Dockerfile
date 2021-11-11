FROM golang:1.17.3-alpine3.14

RUN mkdir -p /usr/src/app

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN go mod download
RUN go build -o distributa cmd/node/main.go

EXPOSE 5000

CMD [ "./distributa" ]
