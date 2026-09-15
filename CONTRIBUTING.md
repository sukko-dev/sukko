# Contributing

Thanks for reading the source. This document says plainly what we can accept, so
that nobody spends an evening on a change we then have to turn down.

## Code contributions are not accepted by default

Sukko is developed by Sukko Pty Ltd. We do not take unsolicited pull requests —
not because outside work isn't welcome in principle, but because this is a
multi-tenant system where a subtly wrong change is a cross-tenant data leak, and
reviewing to that standard takes more care than we can offer to an unplanned
patch.

**If you have a change you think belongs here, open an issue first.** Describe
the problem and the shape of the fix. If we agree it's right, we'll say so, and
we'll sort out the contribution terms then — there is an agreement to sign,
because Sukko Pty Ltd sells licensed editions and cannot ship your code in them
without your permission. That conversation happens once, for changes worth
having, rather than as a tollbooth on the way in.

Please don't open a pull request cold. We would have to close it, which is a
waste of your time and an unpleasant way for us to meet.

## What is genuinely useful

**Bug reports.** The most useful report has the version (`sukko version`, or the
image tag), the edition, what you expected, what happened, and the smallest
reproduction you can manage. Logs are structured JSON — please paste the relevant
lines rather than a screenshot.

**Security reports.** Do not open a public issue. See [SECURITY.md](SECURITY.md).

**Documentation problems.** Anything unclear, wrong, or missing — including in
this file. Documentation bugs are bugs.

**Questions about behaviour.** If the system does something surprising and you
cannot tell whether it's a bug or a deliberate design decision, ask. The answer
usually belongs in the documentation, which makes the question valuable.

## Attribution policy

We do not accept machine-generated contributions, or commits carrying
third-party tool attribution. Authorship names a person who takes responsibility
for the change and can answer questions about it. CI enforces this on commit
messages and added lines.

CI also rejects tracked editor and assistant configuration — the list is
`scripts/check-provenance/forbidden-paths.txt`. Those are developer-local
settings, not project source, and they drift silently because nothing builds or
tests them. Keep them in your own ignore file; git supports a machine-global one
via `core.excludesFile`.

## Building and testing a local checkout

The licence lets you read, use, modify and redistribute the source for any
purpose that doesn't compete with our business, and each version additionally
becomes Apache-2.0 two years after release. See [LICENSE](LICENSE) for the
authoritative terms, including the licence-key provisions.

```sh
cd ws
go build ./...
go test -race ./...        # the race detector is mandatory, not optional
go vet ./...
```

Helm charts and Terraform live under `deployments/`. `task --list-all` shows the
developer task surface.

## How this codebase is meant to be written

The rules the source follows are published in
[docs/engineering-principles.md](docs/engineering-principles.md). Code comments
cite it by section number — a comment mentioning `§VII` means section VII of that
document, and the numbering is stable.

Durable design decisions, including the alternatives that were rejected and why,
are recorded as ADRs in [docs/adr/](docs/adr/). If you want to understand why
something is shaped the way it is, look there before the code.

## Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
