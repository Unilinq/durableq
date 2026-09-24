## What does this change and why

## Checklist

- [ ] Tests added or updated for this change
- [ ] `go test ./... -race` passes locally
- [ ] `gofmt -l .` and `go vet ./...` are clean
- [ ] Commits are signed off (`git commit -s`) per the DCO
- [ ] If this changes storage semantics, `storage/storagetest` was extended
- [ ] If this is design-affecting (new public API, or a change to delivery,
      leasing, or retry semantics), an issue was opened first and is linked
      here
