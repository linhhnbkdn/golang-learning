# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is a Go learning repository. Code here is exploratory — exercises, experiments, and small programs to learn Go idioms.

## Common Commands

```bash
# Run a specific file or package
go run ./path/to/main.go
go run ./some/package/

# Build
go build ./...

# Run all tests
go test ./...

# Run tests for a specific package
go test ./some/package/

# Run a single test
go test -run TestFunctionName ./some/package/

# Run tests with verbose output
go test -v ./...

# Format code
gofmt -w .
# or
go fmt ./...

# Vet (static analysis)
go vet ./...

# Add a dependency
go get github.com/some/package

# Tidy dependencies
go mod tidy
```

## Project Structure

Go code is typically organized as:
- Each directory is a package
- Executable programs have `package main` with a `main()` function
- Tests live alongside source files as `*_test.go`
- A `go.mod` file at root defines the module name (create with `go mod init <module-name>`)

## Go Conventions to Follow

- Error handling: always check and handle errors explicitly; do not use `_` for errors unless intentional
- Use `gofmt` formatting (handled automatically by editors with Go support)
- Prefer table-driven tests using `t.Run` for subtests
- Use interfaces to decouple behavior; keep interfaces small (1–3 methods)
