import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import init_bindings, * as bindings from "example-wasm-bindings";

interface AppState {
  client: bindings.WebsocketClient | null;
}

const AppState = createContext<AppState | null>(null);

export const useAppState = () => useContext(AppState);

export const AppStateProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const isFirstMount = useRef(true);
  const [appState, setAppState] = useState<AppState | null>(null);

  useEffect(() => {
    if (!isFirstMount.current) return;
    isFirstMount.current = false;
    init_bindings().then(async () => {
      const newClient = await bindings.create_client();
      await newClient.ready();
      setAppState({ client: newClient });
    });
  });

  return <AppState.Provider value={appState}>{children}</AppState.Provider>;
};
