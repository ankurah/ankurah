use ankurah_web::client::Client;
use leptos::logging::*;
use leptos::*;

fn main() {
    console_error_panic_hook::set_once();

    let client = Client::new().unwrap();
    leptos::mount_to_body(|| view! { <App client=client/> })
}

#[component]
fn App(client: Client) -> impl IntoView {
    log!("App");
    let (count, set_count) = create_signal(0);

    view! {
        <button
            on:click=move |_| {
                // on stable, this is set_count.set(3);
                set_count(3);
                client.send_message(&format!("the counter state is {}", count()));
            }
        >
            "Click me: "
            // on stable, this is move || count.get();
            {move || count()}
        </button>
    }
}
