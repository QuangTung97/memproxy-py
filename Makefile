.PHONY: all lint test coverage html item-profile

all: lint test

lint:
	mypy .

test:
	coverage run --omit="*/dist-packages/*" -m unittest

coverage:
	coverage report -m

html:
	coverage html

item-profile:
	python3 -m pstats test/item.stats