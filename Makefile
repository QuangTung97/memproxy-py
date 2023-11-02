.PHONY: all lint test coverage html

all: lint test

lint:
	mypy .

test:
	coverage run --omit="*/dist-packages/*" -m unittest

coverage:
	coverage report -m

html:
	coverage html