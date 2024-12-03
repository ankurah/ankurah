import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'
import { AppStateProvider } from './AppState.tsx'
import init_bindings from 'example-wasm-bindings';


init_bindings().then(async () => {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    // <React.StrictMode>
    <AppStateProvider>
      <App />
    </AppStateProvider>
    /* </React.StrictMode>, */
  )
})
