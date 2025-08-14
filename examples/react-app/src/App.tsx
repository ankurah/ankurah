import styled from "styled-components";
import {
  useObserve,
  Entry,
  ctx,
  ws_client,
  EntryView,
  edit_entry,
} from "example-wasm-bindings";
import { useMemo } from "react";

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

function signalObserver<T>(fc: React.FC<T>): React.FC<T> {
  return (props: T) => {
    const observer = useObserve();
    try {
      return fc(props);
    } finally {
      observer.finish();
    }
  };
}
const App: React.FC = signalObserver(() => {
  console.log("RENDER App");
  const connectionState = ws_client().connection_state.value?.value();

  // ignore unused _h warning
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [test_items_signal, _h] = useMemo(() => {
    // subscribe to the ankurah query - returns a signal
    const signal = Entry.subscribe(ctx(), "added = '2024-01-01'");

    // subscribe to the signal - not necessary for this page - just illustrating how to do it
    // the call to test_items_signal.value below is what is doing the real work here,
    // because it automatically subscribes the current observer to the signal
    const h = signal.subscribe((value) => {
      console.log(
        "Subcription to EntryResultSetSignal called the callback with value: ",
        value,
      );
    });
    // h is a SubscriptionGuard. We just keep it resident so the finalization registry doesn't free it and cancel the subscription
    return [signal, h];
  }, []);

  const handleButtonPress = () => {
    (async () => {
      const transaction = ctx().begin();
      await Entry.create(transaction, {
        added: "2024-01-01",
        ip_address: "127.0.0.1",
        node_id: ctx().node_id().to_base64(),
        complex: {
          name: "wesh",
          value: 123,
          thing: {
            Bravo: {
              b: "ça dit quoi",
              c: 123,
            },
          },
        },
      });
      transaction.commit();
    })();
  };

  return (
    <Container>
      <h1>Ankurah Example App</h1>
      <Card>
        <div className="connection-status" style={{ textAlign: "center" }}>
          Connection State: {connectionState}
        </div>
        <div style={{ textAlign: "center", margin: "20px 0" }}>
          <button onClick={handleButtonPress}>Create</button>
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
              <Th>Complex</Th>
            </tr>
          </thead>
          <tbody>
            {(() => {
              // calling .value automatically subscribes the current observer to the signal
              return test_items_signal.value.items.map((item: EntryView) => (
                <EntryRow key={item.id().as_string()} entry={item} />
              ));
            })()}
          </tbody>
        </Table>
      </Card>
    </Container>
  );
});

interface EntryRowProps {
  entry: EntryView;
}

const EntryRow: React.FC<EntryRowProps> = signalObserver(({ entry }) => {
  // manually track the entry in the current observer until the field accessors track
  entry.track();
  console.log("RENDER EntryRow");
  return (
    <Tr onClick={() => edit_entry(entry)} style={{ cursor: "pointer" }}>
      {/* These are not currently calling signal::Get, because they are using FromActiveType */}
      <Td>{entry.id().as_string()}</Td>
      <Td>{entry.added()}</Td>
      <Td>{entry.ip_address()}</Td>
      <Td>{entry.node_id()}</Td>
      <Td>{JSON.stringify(entry.complex())}</Td>
    </Tr>
  );
});

export default App;
