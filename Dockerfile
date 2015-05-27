FROM golang
RUN go get github.com/quipo/statsd
ADD . /go/src/go-chord
RUN go install go-chord/sim

# expose ports
EXPOSE 9000-9200

# Run the sim command by default when the container starts.
CMD /go/bin/sim -type distributed
