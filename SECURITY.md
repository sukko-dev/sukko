# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately, either way:

- Email **security@sukko.dev**
- Or use GitHub's private vulnerability reporting on this repository
  (Security → Report a vulnerability)

A useful report includes the affected version, the impact you believe it has,
and the steps to reproduce it. If you have a proof of concept, please include
it — it shortens the time to a fix considerably.

You do not need to have a complete exploit, or to be certain. A credible
suspicion reported privately is more valuable than a confirmed finding reported
publicly.

## What to expect

We will acknowledge your report and tell you whether we can reproduce it. If we
can, we will tell you our assessment of severity and keep you informed while we
work on a fix. If we cannot reproduce it, we will say so and explain what we
tried, so you can correct us if we have misunderstood.

We will credit you in the release notes for the fix unless you would rather we
did not — just tell us which you prefer.

We ask that you give us a reasonable opportunity to ship a fix before disclosing
the issue publicly. We will not ask you to stay quiet indefinitely.

## Scope

In scope: the services in this repository — the WebSocket gateway, the WebSocket
server, the provisioning API, and the webhook worker — along with their
authentication, tenant isolation, and edition-enforcement paths. Cross-tenant
data leakage is treated as critical severity.

Also in scope: the deployment artefacts under `deployments/`, where a default
would leave an operator exposed.

Out of scope: findings that depend on a deliberately misconfigured deployment
(for example, disabling TLS or publishing debug endpoints, both of which are off
by default), denial of service by sheer volume against a deployment you control,
and reports produced by a scanner without a demonstrated impact.

## Supported versions

Security fixes land on the latest released minor version. Older versions are not
backported unless the issue is critical and the upgrade path is genuinely
blocked for affected operators.
