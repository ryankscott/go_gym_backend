FROM alpine:latest

ADD gym_backend /gym_backend

COPY . .
COPY build/ /build/

RUN apk add --update ca-certificates
RUN apk update && apk add tzdata

ENV TZ="Pacific/Auckland"
EXPOSE 9000 


CMD ["/gym_backend"]
