use ankurah_web_client::Client;
use example_model::*;
use leptos::logging::*;
use leptos::prelude::*;

fn main() {
    console_error_panic_hook::set_once();

    // TODO: use the model in the app
    let _album = Album {
        name: "test".to_string(),
    };

    mount_to_body(|| view! { <App/> })
}

#[component]
fn App() -> impl IntoView {
    log!("App");

    let client = Client::new("localhost:9797").unwrap();
    let (count, set_count) = signal(0);

    let connection_state = client.connection_state();
    view! {
        <div>
            <p>{count}</p>
            <p>{move || connection_state.get().to_string()}</p>
            // <button
            //     on:click=move |_| {
            //         // on stable, this is set_count.set(3);
            //         set_count(3);
            //         client.send_message(&format!("the counter state is {}", count()));
            //     }
            // >
            //     "Click me: "
            //     // on stable, this is move || count.get();
            //     {move || count()}
            // </button>
        </div>
    }
}
