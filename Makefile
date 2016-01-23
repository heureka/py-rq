.PHONY: build clean test

build: .venv
	$(CURDIR)/.venv/bin/pip install -r requirements.txt

.venv:
	virtualenv -p `which python3` $(CURDIR)/.venv

clean:
	rm -rf $(CURDIR)/.venv

test: build
	$(CURDIR)/.venv/bin/python -m unittest discover
