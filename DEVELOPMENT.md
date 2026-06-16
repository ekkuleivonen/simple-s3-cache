# Development

Keep the project boring, small, and test-driven.

## Working Style

* Prefer TDD: write or update the failing test first, then make the smallest
  change that passes.
* Keep changes small and reviewable.
* Build focused packages with clear responsibilities. Add abstraction only when
  it removes real duplication or makes correctness easier to see.
* Prefer simple, explicit code over clever helpers.

## Go Practices

* Use `context.Context` for request-scoped work, cancellation, and timeouts.
* Return errors with useful context; do not panic in normal request paths.
* Keep interfaces small and define them near the code that consumes them.
* Avoid package-level mutable state unless it is part of process setup.
* Make concurrency ownership obvious: document goroutine lifetimes, close
  channels from the sender side, and avoid unbounded background work.
* Run `gofmt`, `go test ./...`, and relevant integration tests before calling a
  change done.

