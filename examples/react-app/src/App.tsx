import styled from "styled-components";
import { useObserve, Entry, ctx, ws_client } from "example-wasm-bindings";
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

function App() {
  const observer = useObserve();
  console.log("MARK 🔵 App component rendering, observer:", observer);

  try {
    const connectionState = ws_client().connection_state.value?.value();
    console.log("MARK 🌐 Connection state:", connectionState);

    const test_items_signal = useMemo(() => {
      console.log("MARK 🔄 Initializing test_items_signal useMemo");
      const signal = Entry.subscribe(ctx(), "added = '2024-01-01'");
      console.log("📡 Created signal:", signal);
      return signal;
    }, []);

    console.log("MARK 📊 Current signal value:", test_items_signal.value);
    console.log("MARK 📝 Items in signal:", test_items_signal.value.items);
    console.log("MARK 📏 Items count:", test_items_signal.value.items.length);

    const handleButtonPress = () => {
      (async () => {
        console.log("🚀 Creating new entry...");
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
        console.log("MARK ✅ Entry created", ctx().node_id());
        transaction.commit();
        console.log("MARK 💾 Transaction committed");

        // Check signal after creation
        setTimeout(() => {
          console.log("MARK 🔍 Post-creation signal check:");
          console.log("MARK 📊 Signal value after create:", test_items_signal.value);
          console.log("MARK 📝 Items after create:", test_items_signal.value.items);
          console.log("MARK 📏 Items count after create:", test_items_signal.value.items.length);
        }, 100);
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
                const items = test_items_signal.value.items;
                console.log("🎨 Rendering items:", items, "count:", items.length);
                return items.map((item: any) => (
                  <Tr key={item.id().as_string()}>
                    <Td>{item.id().as_string()}</Td>
                    <Td>{item.added()}</Td>
                    <Td>{item.ip_address()}</Td>
                    <Td>{item.node_id()}</Td>
                    <Td>{JSON.stringify(item.complex())}</Td>
                  </Tr>
                ));
              })()}
            </tbody>
          </Table>
        </Card>
      </Container>
    );
  } finally {
    console.log("🏁 Finishing observer");
    observer.finish();
  }
}

export default App;
