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

    // Generate TypeScript interface to augment the class with properly typed method signature
    let ts_wrapper_interface = format!(
        "// Augments the {} class\ninterface {} {{\n  subscribe(callback: (value: {}) => void): SubscriptionGuard;\n}}",
        wrapper_name, wrapper_name, name
    );

    // Generate the code
    let expanded = quote! {

        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        pub struct #wrapper_name{
            pub (crate) sig: Box<dyn ::ankurah::signals::GetAndDynSubscribe<#name>>,
            pub (crate) handle: ::std::boxed::Box<dyn ::std::any::Any>
        }

        // Add TypeScript interface override
        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen(typescript_custom_section)]
        const TS_APPEND_CONTENT: &'static str = #ts_wrapper_interface;

        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        impl #wrapper_name {

            #[wasm_bindgen(js_name = "subscribe", skip_typescript)]
            pub fn subscribe(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::signals::SubscriptionGuard {
                use ::ankurah::signals::DynSubscribe;
                let callback = ::ankurah::derive_deps::send_wrapper::SendWrapper::new(callback);

                self.sig.dyn_subscribe(Box::new(move |value: #name| {
                    // Get the current value and call the JavaScript callback
                    let _ = callback.call1(
                        &::ankurah::derive_deps::wasm_bindgen::JsValue::NULL,
                        &value.into()
                    );
                }))
            }

            #[wasm_bindgen(getter)]
            pub fn value(&self) -> #name {
                use ::ankurah::signals::Get;
                self.sig.get()
            }

            #[wasm_bindgen(getter)]
            pub fn peek(&self) -> #name {
                use ::ankurah::signals::Peek;
                self.sig.peek()
            }
        }
    };

    // Convert the expanded code into a TokenStream and return it
    TokenStream::from(expanded)
}
