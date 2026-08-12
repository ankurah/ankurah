# Contributing to Ankurah

Thank you for your interest in contributing to the Ankurah project!
We aim to create an inclusive and supportive environment, and to
build Ankurah into a robust next generation framework for reactive state.

## Our AI Policy

If you plan to use AI in any way for any part of your contribution, please read
[AI_POLICY.md](./AI_POLICY.md) now.

## Talk first for anything non-trivial

It's fine to file PRs for small or obvious fixes. No coordination necessary.

For anything larger like a new feature, refactor, or a change to the
architecture or a public interface, it's imperative that you socialize the idea first.

Open an issue describing the problem and your proposed approach. You can also reach out to us at [community.ankurah.org](https://community.ankurah.org) (which itself uses Ankurah btw!)

Any large PRs, no matter how amazingly good, which do not have some kind of in-advance socialization of the idea first will be summarily closed. It's nothing personal, but the AI tsunami is here, and alignment is key. Following these guidelines protects your time and ours.

## Pull requests are for finished work

Open a pull request when the change is done and you have reviewed it yourself,
line by line. For feedback on direction before then, use an issue or
[community.ankurah.org](https://community.ankurah.org).

## Sign off your commits

Every commit needs a Developer Certificate of Origin sign-off:

    git commit -s

See [AI_POLICY.md](./AI_POLICY.md) for what that sign-off means and why we
require it.

Any PR with commits lacking a Developer Certificate of Origin sign-off will
auto-close.

## Build, test, and format

Before you open a pull request, make sure the change passes all the checks that
CI runs:

- Tests: `cargo test`
- WASM tests: `./scripts/run-wasm-tests.sh`
- Rust formatting: `cargo +nightly fmt --all` (CI checks this with `--check`)
- TOML formatting: `taplo fmt`
- TypeScript formatting (examples): `prettier --write "examples/react-app/src/**/*.{ts,tsx}"`

If you change any code that the README uses as an example, keep the README in
sync. CI verifies those embedded examples with
[liaison](https://github.com/dnorman/liaison) (`liaison --check README.md`).

## Licensing

Ankurah is dual-licensed under MIT or Apache-2.0. By contributing, you agree
that your contribution is licensed under those same terms.
