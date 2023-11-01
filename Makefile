.PHONY: all lint test

all: lint test

lint:
	mypy .

test:
	python3 -m unittest