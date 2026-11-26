## Summary
Describe the changes introduced by this pull request and why they are needed.

## Related ADR / Issue
- ADR: (e.g., `docs/adr/001-example.md` or N/A)
- Issue: (e.g., `Closes #123`)

## Type of Change
- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Refactor / chore / documentation

## Verification Checklist
- [ ] Code is formatted with `gofmt`
- [ ] `go vet ./...` passes with no warnings
- [ ] `go test -race -count=1 ./...` passes
- [ ] Added or updated deterministic unit tests covering the changes
- [ ] Non-trivial design choices are documented in `docs/adr/` (if applicable)
- [ ] No `.env` files, API keys, secrets, or PII/PHI fixtures are included
