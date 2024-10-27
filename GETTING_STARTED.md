# Getting started

Note that this is very early in development, but if you want to see what exists so far you can run the following:

1. Install rust:
   https://rustup.rs/

2. Install cargo watch (useful for dev workflow)

   ```
   cargo install cargo-watch
   ```

3. Start the server (leave this running):

   ```
   # from the root directory of the repo
   cargo watch -x 'run --bin ankurah-server'
   ```

4. Install wasm-pack
   https://rustwasm.github.io/wasm-pack/installer/

5. In a new tab, compile the wasm bindings for the example app (leave this running):
   If you use react, your application will need a crate like this to use your models

   ```
   cd examples/wasm-bindings
   cargo watch -s 'wasm-pack build --target web --debug'
   ```

6. install `bun` (npm/node might work, but I haven't tested it, and bun is way faster)

   https://bun.sh/docs/installation

7. In a new tab, run the example react app (leave this running):
   ```
   cd examples/react-app
   bun install
   bun dev
   ```
