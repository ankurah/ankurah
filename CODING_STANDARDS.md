# Coding Standards

## Source files state their purpose and vocabulary

Every source file touched by a change must begin with a module-level
documentation comment (`//!` in Rust, or the language's equivalent) that
states:

- the file's single overarching purpose;
- the important nouns it introduces or owns and what each noun means; and
- any easily confused responsibility that explicitly belongs elsewhere.

Write or revise this block before changing the implementation. It is an
intention-setting and alignment step, not a description invented after the
code is finished.

If the purpose and nouns cannot be stated clearly and concisely, stop and
reorganize the code until the file has a coherent responsibility. Treat a
missing, vague, or stale purpose block as a structural problem, and keep the
block current when the file's responsibility changes.
