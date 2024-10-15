import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'
import { AppStateProvider } from './AppState.tsx'

ReactDOM.createRoot(document.getElementById('root')!).render(
  // <React.StrictMode>
    <AppStateProvider>
      <App />
    </AppStateProvider>
  /* </React.StrictMode>, */
)
