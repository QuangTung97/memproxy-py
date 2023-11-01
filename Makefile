.PHONY: lint test

lint:
	mypy .

test:
	python3 -m unittest