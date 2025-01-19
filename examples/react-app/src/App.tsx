import styled from "styled-components";
import { useAppState } from "./AppState";
import {
  withSignals,
  fetch_test_items,
  create_test_entity,
  SessionView,
  subscribe_test_items,
} from "example-wasm-bindings";
import { useEffect, useMemo, useState } from "react";

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 1rem;
`;

const Th = styled.th`
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
  background-color: #f4f4f4;
  font-weight: bold;
`;

const Td = styled.td`
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
`;

const Tr = styled.tr`
  &:nth-child(even) {
    background-color: #f8f8f8;
  }
  &:hover {
    background-color: #f0f0f0;
  }
`;

const Container = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  width: 100vw;
  min-height: 100vh;
  padding: 20px;
  position: absolute;
  left: 0;
  top: 0;
`;

const Card = styled.div`
  width: 100%;
  max-width: 1200px;
  background-color: #f0f0f0;
  padding: 2rem;
  border-radius: 8px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
`;

function App() {
  // Temporary fix - this should be done differently
  return withSignals(() => {
    // try {
    const appState = useAppState();
    console.log("render 1", { appState });

    // const [connectionState, setConnectionState] = useState<string | null>(null);
    const connectionState = appState?.client?.connection_state.value?.value();
    const test_items_signal = useMemo(
      () => (appState?.client ? subscribe_test_items(appState?.client) : null),
      [appState?.client],
    );

    // const [sessions, setSessions] = useState<SessionView[]>([]);
    console.log("render 2", { connectionState });

    // useEffect(() => {
    // if (appState?.client) {
    //   fetch_test_items(appState?.client).then((sessions) => {
    //     console.log('sessions fetched', sessions);
    //     setSessions(sessions);
    //   });
    // }
    // const intervalId = setInterval(() => {
    //   const client = appState?.client;
    //   if (client) {
    //     console.log('fetching sessions', client);
    //     fetch_test_items(client).then((sessions) => {
    //       console.log('sessions fetched', sessions);
    //       setSessions(sessions);
    //     });
    //   }
    // }, 5000);

    // return () => clearInterval(intervalId);
    // }, [appState?.client]);

    const handleButtonPress = () => {
      if (appState?.client) {
        create_test_entity(appState.client).then(() => {
          console.log("Session created");
        });
      } else {
        console.log("Client not ready");
      }
    };

    return (
      <Container>
        <h1>Ankurah Example App</h1>
        <Card>
          <div className="connection-status" style={{ textAlign: "center" }}>
            Connection State: {connectionState}
          </div>
          <div style={{ textAlign: "center", margin: "20px 0" }}>
            <button onClick={handleButtonPress} disabled={!appState?.client}>
              Create
            </button>
          </div>
          <div style={{ textAlign: "center", marginBottom: "10px" }}>
            Sessions:
          </div>
          <Table>
            <thead>
              <tr>
                <Th>ID</Th>
                <Th>Date Connected</Th>
                <Th>IP Address</Th>
                <Th>Node ID</Th>
              </tr>
            </thead>
            <tbody>
              {test_items_signal?.value.resultset()?.map((session) => (
                <Tr key={session.id().as_string()}>
                  <Td>{session.id().as_string()}</Td>
                  <Td>{session.date_connected()}</Td>
                  <Td>{session.ip_address()}</Td>
                  <Td>{session.node_id()}</Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </Card>
      </Container>
    );
    // } catch ;
  })
}

export default App;
