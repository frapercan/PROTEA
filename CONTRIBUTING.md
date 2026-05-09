# Contributing to PROTEA

Thank you for your interest in contributing! PROTEA welcomes contributions from research institutions and individual developers.

## Branch Strategy

```
main        ← stable releases only (protected, requires PR + review + CI)
develop     ← integration branch (protected, requires PR + CI)
feature/*   ← new features
fix/*       ← bug fixes
docs/*      ← documentation improvements
```

All contributions go through `develop` first. `develop` is merged into `main` for releases.

## Workflow

1. **Fork** the repository
2. **Create a branch** from `develop`:
   ```bash
   git checkout develop
   git checkout -b feature/my-feature
   ```
3. **Make your changes** — follow the code style (ruff + mypy enforced in CI)
4. **Run checks locally** before pushing:
   ```bash
   poetry run task lint       # ruff (incl. flake8-bugbear B-rules)
   poetry run mypy protea     # type checking
   poetry run pytest          # unit tests
   ```
5. **Open a Pull Request** targeting `develop`
6. CI must pass and at least one review approval is required

## Development Setup

```bash
# Install runtime + the dev groups you actually need.
# Groups are opt-in (`optional = true`); `poetry install` alone gets only
# main runtime dependencies.
poetry install --with lint,test,docs

# (GPU embedding worker only) flip torch from CPU to CUDA build:
bash scripts/install_gpu_torch.sh

# Start the full stack (requires PostgreSQL and RabbitMQ)
bash scripts/manage.sh start

# Run tests
poetry run pytest

# Run integration tests (requires Docker)
poetry run pytest --with-postgres
```

## Code Style

- **Python**: ruff (formatter + linter, incl. ported flake8-bugbear B-rules) + mypy
- **Line length**: 100 characters (ruff ignores E501; flake8 with 130-char limit was retired)
- **Type hints**: required for all public functions
- **Docstrings**: NumPy style preferred

## Adding a New Operation

1. Create `protea/core/operations/my_operation.py` implementing `name` + `execute(session, payload, *, emit)`
2. Register it in `scripts/worker.py`
3. Add tests in `tests/`
4. Document it in `docs/source/architecture/operations.rst`

## Questions

Open an issue on GitHub or reach out via the repository discussions.
