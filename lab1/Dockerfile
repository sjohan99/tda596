FROM golang:1.23

WORKDIR /app

# Uncomment if we add external dependencies
# COPY go.mod go.sum ./
# RUN go mod download

COPY . .

RUN go build -o bin/http_server ./cmd/httpserver

EXPOSE 80

CMD ["./bin/http_server", "80"]