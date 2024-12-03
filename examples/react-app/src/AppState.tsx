import React, { createContext, useContext, useEffect, useState } from 'react';
import init_bindings, * as bindings from 'example-wasm-bindings';

interface AppState {
  client: bindings.Client | null;
}

const AppState = createContext<AppState | null>(null);

export const useAppState = () => useContext(AppState);

export const AppStateProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [appState, setAppState] = useState<AppState | null>(null);

  useEffect(() => { // use useMemo to ensure that the client is only initialized once
    // React functional components are just classes with more steps. Change my mind
    // if (initializing) {
    //   return;
    // }
    // initializing = true;
    init_bindings().then(async () => {
      const newClient = bindings.Client.new();

      // await newClient.ready();
      setAppState({ client: newClient });
    });
  }, []);

  return (
    <AppState.Provider value={appState}>
      {children}
    </AppState.Provider>
  );
};