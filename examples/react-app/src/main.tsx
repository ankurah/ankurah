import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.tsx";
import init_bindings, * as bindings from "example-wasm-bindings";

interface AppState {
  client: bindings.WebsocketClient;
  context: bindings.Context;
}
let appState: AppState | null = null;
export const useAppState = () => appState!;
export const useContext = () => appState!.context;

(async () => {
  console.log("Initializing application");
  await init_bindings();
  const client = await bindings.create_client();
  const context = await bindings.get_context();
  appState = { client, context };

  await client.ready();
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
})();
