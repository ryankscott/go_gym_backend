FROM golang:latest 
RUN mkdir /app 
ADD . /app/ 
WORKDIR /app 
RUN go build -o main . 

# Bundle app source
COPY . .
COPY build/ /build/

ENV TZ="Pacific/Auckland"
EXPOSE 3000 

CMD ["/app/main"]
