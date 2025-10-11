# Default Backends Configuration

This directory contains configuration files for Ankurah's built-in property backends. These files are symlinked from the `core` crate rather than duplicated.

## Why Symlinks?

These configuration files define the canonical behavior of property backends in the `core` crate. The `derive` crate needs access to them at compile time (via `include_bytes!`) to generate specialized code for each backend type. Symlinking ensures a single source of truth while allowing the derive macros to access the definitions.

## Purpose

These RON files configure the derive macro's code generation for different property backends

Each configuration specifies:

- Type patterns and generic parameters
- Method signatures to generate for each backend
- Namespace resolution for both internal and external use
- Field type inference rules

This enables the `#[derive(Model)]` macro to automatically generate properly-typed accessor methods for property fields based on their backend type, including proper wasm-bindgen exports when targeting WebAssembly.
