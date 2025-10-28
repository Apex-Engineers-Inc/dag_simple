# Contributing to DAG Simple

Thank you for your interest in contributing to DAG Simple! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager (recommended)

### Setting Up Your Development Environment

1. **Fork and clone the repository:**

   ```
   git clone https://github.com/yourusername/dag-simple.git
   cd dag-simple
   ```

2. **Install uv (if not already installed):**

   ```
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies:**

   ```
   uv sync --dev
   ```

This will install all development dependencies including pytest, mypy, and ruff.

## Development Workflow

### Running Tests

Run the full test suite:

```
uv run pytest
```

Run tests with coverage:

```
uv run pytest --cov=dag_simple --cov-report=html
```

Run specific tests:

```
uv run pytest tests/test_dag_simple.py::TestBasicNode::test_node_creation -v
```

### Type Checking

Run pyright for type checking:

```
uv run pyright
```

### Code Formatting

Format code with ruff:

```
uv run ruff format .
```

### Linting

Check code with ruff linter:

```text
uv run ruff check .
```

Fix auto-fixable issues:

```
uv run ruff check --fix .
```

### Running All Checks

Before submitting a PR, run all checks:

```
uv run pytest && uv run pyright && uv run ruff format . && uv run ruff check .
```

## Code Style Guidelines

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Write docstrings for public APIs
- Keep functions focused and small
- Add tests for new functionality

## Testing Guidelines

- Write tests for all new features
- Aim for high test coverage (>90%)
- Use descriptive test names
- Include both positive and negative test cases
- Test edge cases

### Test Organization

Tests are organized in `tests/test_dag_simple.py` with these categories:

- `TestBasicNode` - Basic node functionality
- `TestTypeValidation` - Type validation features
- `TestCaching` - Caching functionality
- `TestCycleDetection` - Cycle detection
- `TestTopologicalSort` - Topological sorting
- `TestInputNodes` - Input node functionality
- `TestDAGClass` - High-level DAG API
- `TestErrorHandling` - Error handling
- `TestIntrospection` - Introspection methods
- `TestComplexScenarios` - Real-world scenarios

## Submitting Changes

### Pull Request Process

1. **Create a feature branch:**

   ```
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**

   - Write your code
   - Add tests
   - Update documentation if needed

3. **Run all checks:**

   ```
   uv run pytest
   uv run mypy src
   uv run ruff format .
   uv run ruff check .
   ```

4. **Commit your changes:**

   ```
   git add .
   git commit -m "Add feature: brief description"
   ```

   Use clear, descriptive commit messages.

5. **Push to your fork:**

   ```
   git push origin feature/your-feature-name
   ```

6. **Open a Pull Request:**
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your feature branch
   - Fill out the PR template
   - Link any related issues

### Pull Request Guidelines

- Keep PRs focused on a single feature or fix
- Include tests for new functionality
- Update documentation as needed
- Ensure all CI checks pass
- Respond to review feedback promptly

### Commit Message Format

Use clear, descriptive commit messages:

```
Add feature: brief description

Longer description of what changed and why (if needed).

Fixes #123
```

## Adding New Features

When adding new features:

1. **Discuss first:** Open an issue to discuss the feature before implementing
2. **Write tests:** Add comprehensive tests for the feature
3. **Update docs:** Update README.md and docstrings
4. **Keep it simple:** Follow the library's philosophy of simplicity
5. **Maintain compatibility:** Ensure changes don't break existing APIs

## Reporting Bugs

When reporting bugs, include:

1. Python version
2. dag-simple version
3. Minimal code example that reproduces the bug
4. Expected behavior
5. Actual behavior
6. Error messages/stack traces

## Feature Requests

We welcome feature requests! When requesting a feature:

1. Explain the use case
2. Describe the desired behavior
3. Suggest an implementation approach (optional)
4. Consider if it fits the library's philosophy of simplicity

## Documentation

Good documentation is crucial. When contributing:

- Update README.md for user-facing changes
- Add docstrings for new functions/classes
- Include code examples
- Keep examples simple and clear

## Questions?

- Open an issue for general questions
- Tag maintainers for urgent matters
- Check existing issues and PRs first

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Assume good intentions

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing to DAG Simple! 🎉
