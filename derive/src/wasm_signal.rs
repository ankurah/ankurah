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


        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        pub struct #wrapper_name(Box<dyn ::ankurah::derive_deps::GetSignalValue<Value = #name>>);

        impl <T> From<T> for #wrapper_name  where T: ::ankurah::GetSignalValue<Value = #name> + 'static{
            fn from(value: T) -> Self {
                #wrapper_name(Box::new(value))
            }
        }
        // use ::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen;
        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        impl #wrapper_name {

            #[wasm_bindgen(js_name = "subscribe")]
            pub fn subscribe(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::derive_deps::ankurah_react_signals::Subscription {
                use ::ankurah::GetSignalValue;
                let signal : Box<dyn GetSignalValue<Value = #name>> = self.0.cloned(); // Now using the cloned() method from GetSignalValue
                // leave this commented out for now
                let effect = ::ankurah::derive_deps::reactive_graph::effect::Effect::new(move |_| {
                    use ::ankurah::derive_deps::reactive_graph::traits::Get;
                    let value = signal.get();
                    callback
                        .call1(&::ankurah::derive_deps::wasm_bindgen::JsValue::NULL, &value.into())
                        .unwrap();
                });

                ::ankurah::derive_deps::ankurah_react_signals::Subscription::new(effect)
            }

            #[wasm_bindgen(getter)]
            pub fn value(&self) -> #name {
                use ::ankurah::derive_deps::reactive_graph::traits::Get;
                self.0.get()
            }
        }
    };

    // Convert the expanded code into a TokenStream and return it
    TokenStream::from(expanded)
}
