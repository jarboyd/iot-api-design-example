# IoT API Design Example (Bonusaufgabe 1)

This repository contains the solution for exercise 1 in the API design and system integration lecture at the Ostfalia University of Applied Sciences in 2026.

# Running the project

Make sure a local RabbitMQ server is running, for example by spooling up the provided docker compose script.

```bash
docker compose up -d
```

If you choose to run the server this way, you can tear it down with

```bash
docker compose down
```

and if you later want to clean up the volumes created (will delete any persisted data), run

```bash
docker compose down -v
```

When either the dockerized server or another RabbitMQ instance is running, make sure the dependencies are installed by running

```bash
uv sync
```

and then run the project with

```bash
uv run iot-design-example
```

If you choose to not use uv, the required dependencies can be found in `pyproject.toml`. Running the project may then require some other steps, but the main file is in `src/iot_design_example/__init__.py`.
