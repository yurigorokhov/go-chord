all: sim

run: sim
	./sim.out

cache: sim
	./sim.out -usecache

build:
	go build

test:
	go test .

cov:
	gocov test github.com/armon/go-chord | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

sim:
	go build -o sim.out go-chord/sim

.PHONY: sim
