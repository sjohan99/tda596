FROM golang:1.23

WORKDIR /app

# Uncomment if we add external dependencies
# COPY go.mod go.sum ./
# RUN go mod download

COPY . .

RUN go build ./cmd/http_server

EXPOSE 8080

CMD ["./http_server", "8080"]