.PHONY: clean build run

APP_DIR = $(shell cd ../ && pwd)

all: run

build:
	docker build -t okafka -f Dockerfile.build .

run: build
	docker run -v $(APP_DIR):/home/opam/app -w /home/opam/app okafka make
