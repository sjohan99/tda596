FROM golang:1.23

WORKDIR /app

COPY . .

RUN go build -o bin/http_server ./cmd/httpserver

EXPOSE 80

CMD ["./bin/http_server", "80"]