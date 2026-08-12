# Contributing to Ankurah with AI

Use AI as much as you like in private. Only finished work that you can
personally vouch for belongs in this repository.

We do not care how you arrive at a change. We care that the result is small,
correct, vouched for by someone who understands every line, and aligned with
the broader Ankurah team.

## You are the author

Whether you used an AI assistant, an IDE, a search engine, or your own two
hands, you are the author of what you submit. You are accountable for every
line as if you had typed it yourself. Using AI does not lower that standard.
"The model wrote it" is not a defense for a change you cannot explain.

Before you open a pull request, you must read every line of the diff and be
ready to explain why each one is there. If you would not stake your name on a
line, it does not belong in the pull request.

## Explore freely in private, land only the polished result

Use AI to explore ideas, build proofs of concept, and prototype features. That
exploration should not be visible here. Submit the minimal, finished change
that survived your review, not the experiments that produced it.

For contributions:

- Do not open draft or work-in-progress pull requests so others can watch you
  think. See "Pull requests are for finished work" in CONTRIBUTING.
- Do not carry AI-generated commit noise, dead code, speculative abstractions,
  or half-built features into a pull request.
- If a change is large or shifts the architecture, start the conversation in
  an issue before writing any code. See CONTRIBUTING.

## Sign off on every commit

Ankurah uses the Developer Certificate of Origin. Every commit carries a
`Signed-off-by:` line, which you add with:

    git commit -s

The DCO, available in full at https://developercertificate.org, is your
statement that you have the right to submit the work under the project's
license. At Ankurah, it carries a second meaning that we take just as seriously.
By signing off, you attest that you have personally reviewed the change and
vouch for it, whatever tools helped you write it.

If an AI tool produced any part of your contribution, your sign-off also means
you have confirmed that it carries no license terms or third-party code
incompatible with Ankurah's MIT or Apache-2.0 licensing. If you are not certain,
do not submit it.

## AI may draft your pull request description under three conditions

You may use AI to draft a pull request description, provided all three of these
conditions hold:

1. You open the pull request as a draft first, so the description is not treated
   as final until a human has checked it.
2. You read the entire description yourself and confirm that it accurately describes
   the change.
3. The wording is grounded in Ankurah's own lexicon: the words this project
   already uses in its README, its book, its [lexicon](./LEXICON.md), and its
   existing code and comments. AI can readily invent new terminology, but that
   terminology does not belong here. Describe the change in the project's
   established terms, or do not describe it.

Mark the pull request ready for review only after the description passes all
three conditions.

## No AI review on public pull requests

Review is a human act. Every review comment posted on this repository must
express a person's own judgment in that person's own words. The reviewer must
stand behind it.

You may use AI privately to help you understand a diff before reviewing it. You
may not paste AI output into a review, and you may not point an automated review
bot at a pull request. AI reviewing AI produces confident noise that helps no
one.

## Why we are strict about this

A small maintainer team can receive plausible-looking, low-effort submissions
faster than it can review them. In early 2026, the curl project shut down its
entire security bug bounty because AI slop reports overwhelmed its maintainers.
Our expectation is simple: bring us less, make it finished, and stand behind it.

Contributions that ignore this policy will be closed without a detailed review.
If you repeatedly spend maintainer time this way, you will be blocked from the
project. This policy is not hostile to AI. It protects the small number of
people who keep Ankurah alive.
