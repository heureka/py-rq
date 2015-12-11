.PHONY: build clean test

build: .venv

.venv:
	virtualenv -p `which python3` $(CURDIR)/.venv
	$(CURDIR)/.venv/bin/pip install -r requirements-dev.txt

clean:
	rm -rf $(CURDIR)/.venv

test: build
	$(CURDIR)/.venv/bin/python -m unittest discover