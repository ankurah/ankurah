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

    // Generate the code
    let expanded = quote! {


        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        pub struct #wrapper_name{
            pub (crate) sig: ::ankurah::derive_deps::ankurah_signals::Read<#name>,
            pub (crate) handle: ::std::boxed::Box<dyn ::std::any::Any>
        }

        // impl <T> From<T> for #wrapper_name  where T: ::ankurah::GetSignalValue<Value = #name> + 'static{
        //     fn from(value: T) -> Self {
        //         #wrapper_name(Box::new(value))
        //     }
        // }



        #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        impl #wrapper_name {

            #[wasm_bindgen(js_name = "subscribe")]
            pub fn subscribe(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::derive_deps::js_sys::Function {
                // For now, return the callback as-is. This will need to be updated
                // to work with the new observer system, but this prevents compilation errors.
                // TODO: Implement proper subscription using ankurah-signals Observer
                callback
            }

            #[wasm_bindgen(getter)]
            pub fn value(&self) -> #name {
                use ::ankurah::derive_deps::ankurah_signals::traits::Get;
                self.sig.get()
            }
        }
    };

    // Convert the expanded code into a TokenStream and return it
    TokenStream::from(expanded)
}
