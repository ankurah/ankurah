import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.tsx";
import { AppStateProvider } from "./AppState.tsx";
import init_bindings from "example-wasm-bindings";

init_bindings().then(async () => {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <AppStateProvider>
        <App />
      </AppStateProvider>
    </StrictMode>,
  );
});
