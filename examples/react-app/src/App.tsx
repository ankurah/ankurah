import reactLogo from './assets/react.svg'
import viteLogo from '/vite.svg'
import './App.css'
import { useAppState } from './AppState'
import { useSignals } from 'example-wasm-bindings';


function App() {
  const c = useSignals();

  try {
    const appState = useAppState()
    console.log('render 1', { appState });

    // const [connectionState, setConnectionState] = useState<string | null>(null);
    const connectionState = appState?.client?.connection_state.value;

    console.log('render 2', { connectionState });

    // useEffect(() => {
    //   console.log('useEffect', appState?.client);
    //   if (appState?.client) {
    //     // console.log('subscribing to connection state');
    //     // appState.client.connection_state.subscribe((state: string) => {
    //     //   console.log('connection state changed', state);
    //     //   setConnectionState(state);
    //     // });
    //   }
    // }, [appState?.client]);

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
            Connection State: {connectionState}
          </div>
          <button onClick={handleSendMessage} disabled={!appState?.client}>
            Send Message
          </button>
        </div>
      </>
    )
  } finally {
    c.finish();
  }
}

export default App