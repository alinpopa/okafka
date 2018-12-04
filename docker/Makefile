.PHONY: clean build run shell

APP_DIR = $(shell cd ../ && pwd)

all: run

build:
	docker build -t okafka -f Dockerfile.build .

run: build
	docker run --rm -v $(APP_DIR):/home/opam/app -w /home/opam/app okafka make

shell: run
	docker run -it --rm -v $(APP_DIR):/home/opam/app -w /home/opam/app okafka /bin/bash
