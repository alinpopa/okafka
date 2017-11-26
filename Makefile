.PHONY: clean build utop main consumer producer
.PHONY: opam-deps

all: build main consumer producer

clean:
	-rm -rf _build
	-rm -rf src/.merlin
	-rm -rf *.install

build:
	jbuilder build --only-packages=okafka @install

main: build
	jbuilder build bin/main.exe

consumer: build
	jbuilder build bin/consumer.exe

producer: build
	jbuilder build bin/producer.exe

utop:
	jbuilder exec utop

opam-deps:
	opam install \
		jbuilder \
		lwt \
		crc \
		conduit \
		conduit-lwt-unix \
		core
