import { useEffect, useMemo, useState } from "react";
import {
  initialize_client,
  Album,
  useObserve,
  ctx,
  AlbumLiveQuery,
} from "ankurah-doc-example-wasm-bindings";
import "./App.css";

export function signalObserver<T>(fc: React.FC<T>): React.FC<T> {
  return (props: T) => {
    const observer = useObserve();
    try {
      return fc(props);
    } finally {
      observer.finish();
    }
  };
}

// React Component example from the landing page

interface Props {
  albums: AlbumLiveQuery;
}
// liaison id=react-component
/* creates and Binds a ReactObserver to the component */
const AlbumList = signalObserver(({ albums }: Props) => {
  return (
    <ul>
      /* React Observer automatically tracks albums */
      {albums.items.map((album) => (
        <li>{album.name}</li>
      ))}
    </ul>
  );
});
// liaison end

function App() {
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    initialize_client("ws://localhost:9797")
      .then(() => {
        console.log("✓ Client initialized");
        setReady(true);
      })
      .catch((err) => {
        console.error("Failed to initialize client:", err);
        setError(String(err));
      });
  }, []);

  const albums = useMemo(() => {
    // liaison id=react-livequery
    const q: AlbumLiveQuery = Album.query(ctx(), "year > 1985");
    // liaison end
    return q;
  }, []);

  return (
    <div className="app">
      <h1>Ankurah Doc Example</h1>
      <p>This example validates the code snippets from the README.</p>

      {error && (
        <div className="error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {ready ? (
        <div>
          <h2>Albums (year &gt; 1985)</h2>
          <AlbumList albums={albums} />
        </div>
      ) : (
        <div className="status">
          Connecting to server at ws://localhost:9797...
        </div>
      )}
    </div>
  );
}

export default App;
