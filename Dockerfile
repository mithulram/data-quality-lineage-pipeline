FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY examples ./examples

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["quality-lineage"]
CMD ["run", "--source", "examples/source", "--output", "artifacts", "--max-error-rate", "0.8"]
