.PHONY: all lint test coverage html item-profile build-dist upload

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

build-dist:
	python3 setup.py sdist

upload:
	twine upload dist/*
