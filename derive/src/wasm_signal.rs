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
        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub struct #wrapper_name {
            inner: ::std::rc::Rc<::std::cell::RefCell<::std::boxed::Box<dyn ::futures_signals::signal::Signal<Item = #name>>>>,
        }

        // #[::wasm_bindgen::prelude::wasm_bindgen]
        impl #wrapper_name {
            fn new<S>(signal: S) -> Self
            where
                S: ::futures_signals::signal::Signal<Item = #name> + 'static,
            {
                Self {
                    inner: ::std::rc::Rc::new(::std::cell::RefCell::new(::std::boxed::Box::new(signal))),
                }
            }

            // #[::wasm_bindgen::prelude::wasm_bindgen]
            pub fn for_each(&self, callback: ::js_sys::Function) -> ::ankurah_react_signals::Subscription {
                let signal = ::std::rc::Rc::clone(&self.inner);
                let callback = ::std::rc::Rc::new(callback);

                let (abort_handle, abort_registration) = ::futures::future::AbortHandle::new_pair();

                let future = {
                    let callback = callback.clone();
                    let signal = (*signal.borrow()).as_ref();

                    let stream_future = signal.for_each(move |value| {
                        let this = ::wasm_bindgen::JsValue::NULL;
                        let js_value = ::wasm_bindgen::JsValue::from(value);
                        callback.call1(&this, &js_value).unwrap();
                        ::futures::future::ready(())
                    });

                    ::futures::future::Abortable::new(stream_future, abort_registration)
                };

                ::wasm_bindgen_futures::spawn_local(async move {
                    let _ = future.await;
                });

                ::ankurah_react_signals::Subscription::new(callback, abort_handle)
            }
        }
    };

    // Convert the expanded code into a TokenStream and return it
    TokenStream::from(expanded)
}
