extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::*;

pub fn derive_wasm_signal_impl(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input: DeriveInput = parse(input).unwrap();

    // Get the name of the type
    let name = input.ident;

    // Generate the wrapper struct name
    let wrapper_name = Ident::new(&format!("{}Signal", name), name.span());
    println!("wrapper_name: {}", wrapper_name);

    // Generate the code
    let expanded = quote! {
        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub struct #wrapper_name( ::reactive_graph::signal::ReadSignal<#name> );

        #[::wasm_bindgen::prelude::wasm_bindgen]
        impl #wrapper_name {
            fn new(signal: ::reactive_graph::signal::ReadSignal<#name>) -> Self {
                Self(signal)
            }

            #[::wasm_bindgen::prelude::wasm_bindgen]
            #[wasm_bindgen(js_name = "subscribe")]
            pub fn js_subscribe(&self, callback: js_sys::Function) -> ankurah_react_signals::Subscription {
                let signal = self.0;
                let effect = Effect::new(move |_| {
                    let value = signal.get();
                    // let js_value = wasm_bindgen::JsValue::from_str(value);
                    callback
                        .call1(&wasm_bindgen::JsValue::NULL, &value.into())
                        .unwrap();
                });

                ankurah_react_signals::Subscription::new(effect)
            }

            #[wasm_bindgen(getter)]
            pub fn value(&self) -> #name {
                self.0.get()
            }
        }
    };

    // Convert the expanded code into a TokenStream and return it
    TokenStream::from(expanded)
}
