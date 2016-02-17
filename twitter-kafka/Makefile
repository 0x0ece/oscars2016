SHELL := /bin/bash

.PHONY: pre-check
pre-check:
	@command -v pyvenv >/dev/null 2>&1 || { echo >&2 "$$DEPS_TEXT Missing package: pyvenv"; exit 1; }

.PHONY: deps
deps: pre-check env/bin/activate
	test -f local_settings.py || touch local_settings.py

.PHONY: env
env: env/bin/activate
env/bin/activate: requirements.txt
	test -d env || pyvenv env
	. env/bin/activate; pip install -r requirements.txt

