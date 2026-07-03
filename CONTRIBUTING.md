# Contributing to Buraq

First off, thank you for considering contributing to Buraq! It's people like you that make Buraq such a great tool.

## Where do I go from here?

If you've noticed a bug or have a question, [search the issue tracker](#) to see if someone else in the community has already created a ticket. If not, go ahead and make one!

## How to contribute

### 1. Setting up your environment

1. Fork the repository and clone it locally.
2. Ensure you have Go 1.20+ and Docker installed.
3. Bring up the required backend services utilizing `docker-compose up -d`.

### 2. Making Changes

1. Create a new branch logically named for the feature or fix you’re working on.
2. Make your modifications.
3. If applicable, add or update Go unit tests and test everything utilizing `go test ./...`.
4. Ensure your code passes standard Go formatting `go fmt ./...`.

### 3. Submission Guidelines

1. Make sure all local tests pass.
2. Push the changes to your fork.
3. Open a Pull Request with a clear title and description against the `main` branch.
4. Document the 'Why' behind your changes, not just the 'What'.
5. We will review your code, and potentially request adjustments!

## Code of Conduct

Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms. Always be respectful and constructive in your feedback to others.
