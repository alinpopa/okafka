.PHONY: clean build utop lwt

all: build

clean:
	-rm -rf _build
	-rm -rf src/.merlin
	-rm -rf *.install

build:
	jbuilder build --only-packages=okafka @install

utop:
	jbuilder exec utop
