PYTHON ?= python3
SOURCE ?= examples/source
OUTPUT ?= artifacts

.PHONY: install test demo clean

install:
	$(PYTHON) -m pip install -e .

test:
	$(PYTHON) -m unittest discover -s tests -v

demo:
	$(PYTHON) -m quality_lineage run --source $(SOURCE) --output $(OUTPUT) --max-error-rate 0.8

clean:
	rm -rf $(OUTPUT) build dist
