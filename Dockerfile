FROM golang:1.25.6-alpine AS build

WORKDIR /src/examples/ai_crm

COPY examples/ai_crm/go.mod examples/ai_crm/go.sum ./
RUN go mod download

COPY examples/ai_crm/*.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server .

FROM alpine:3.19

WORKDIR /app/examples/ai_crm

COPY --from=build /out/server ./server
COPY examples/ai_crm/pb_public ./pb_public
COPY Denicx_Logo.jpg /app/Denicx_Logo.jpg

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "mkdir -p /data && if [ -n \"${PB_SUPERUSER_EMAIL:-}\" ] && [ -n \"${PB_SUPERUSER_PASSWORD:-}\" ]; then ./server --dir=/data superuser upsert \"$PB_SUPERUSER_EMAIL\" \"$PB_SUPERUSER_PASSWORD\" || true; fi; ./server --dir=/data serve --http=0.0.0.0:${PORT:-8080}"]
