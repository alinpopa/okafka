.PHONY: clean build utop main consumer producer
.PHONY: opam-deps

all: build main consumer producer

clean:
	-rm -rf _build
	-rm -rf src/.merlin
	-rm -rf *.install

build:
	dune build --only-packages=okafka @install

main: build
	dune build bin/main.exe

consumer: build
	dune build bin/consumer.exe

producer: build
	dune build bin/producer.exe

utop:
	dune exec utop

opam-deps:
	opam install \
		dune \
		lwt \
		lwt.unix \
		crc \
		conduit \
		conduit-lwt-unix \
		core
