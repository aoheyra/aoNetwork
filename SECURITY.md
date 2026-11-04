# Security policy

## Reporting a vulnerability

**Do not open a public issue for a vulnerability.** Report it privately through
[GitHub Security Advisories](https://github.com/aoheyra/aoNetwork/security/advisories/new)
("Report a vulnerability") on this repository. The maintainers will acknowledge
the report, investigate and coordinate a fix and disclosure with you.

Please include:

- the affected component (frame codec, reassembler, link state machine, …)
  and the `NET-xx` issue you believe is involved, if known;
- a description of the impact and, if possible, a minimal reproducer;
- whether the issue is already public or under embargo.

## Scope

This crate is the BLE link layer: it treats every neighbouring radio as
hostile. In scope are anything that lets a remote peer panic the process,
over-allocate memory, bypass rate limits or the degree cap, corrupt link
state, or leak data across links.

Not in scope: issues that require a compromised local device or a malicious
build of the app itself; vulnerabilities in sibling crates (`aoMesh`,
`aoIdents`, the Android app), which are reported against their own repos.

## Supported versions

Pre-`v0.1.0` there are no supported releases: the repository `master` branch
is the only supported target and fixes land there. Once `v0.1.0` is tagged
(NET-27), security fixes are backported to the latest minor release line.
