import { useEffect, useState } from 'react'
import reactLogo from './assets/react.svg'
import viteLogo from '/vite.svg'
import './App.css'
import { useAppState } from './AppState'
import * as bindings from 'example-wasm-bindings';

function App() {
  const appState = useAppState()
  console.log('appState', appState);

  const [connectionState, setConnectionState] = useState<bindings.ConnectionState | null>(null);

  useEffect(() => {
    if (appState?.client) {
      appState.client.connection_state.for_each((state: bindings.ConnectionState) => {
        setConnectionState(state);
      });
    }
  }, [appState?.client]);

  const handleSendMessage = () => {
    if (appState?.client) {
      appState.client.send_message('Hello from App!');
      console.log('Message sent');
    } else {
      console.log('Client not ready');
    }
  }

  return (
    <>
      <div>
        <a href="https://vitejs.dev" target="_blank">
          <img src={viteLogo} className="logo" alt="Vite logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
      </div>
      <h1>Vite + React</h1>
      <div className="card">
        <div className="connection-status">
          Connection State: {connectionState ? bindings.ConnectionState[connectionState] : 'Not Connected'}
        </div>
        <button onClick={handleSendMessage} disabled={!appState?.client}>
          Send Message
        </button>
      </div>
    </>
  )
}

export default App