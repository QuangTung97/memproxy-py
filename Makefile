.PHONY: all lint test coverage html item-profile build-dist upload install-tools requirements

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
	rm -rf ./dist
	python3 setup.py sdist

upload:
	twine upload -r pypi dist/*

install-tools:
	pip3 install mypy
	pip3 install coverage

requirements:
	pip3 freeze >requirements.txt
