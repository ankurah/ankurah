import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.tsx";
import init_bindings, { ready } from "example-wasm-bindings";

(async () => {
  console.log("Initializing application");
  await init_bindings();

  await ready();
  createRoot(document.getElementById("root")!).render(<App />);
})();
