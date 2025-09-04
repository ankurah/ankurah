import styled from "styled-components";
import {
  useObserve,
  LogEntry,
  Flags,
  ctx,
  ws_client,
  LogEntryView,
  LogEntryMut,
} from "example-wasm-bindings";
import { useMemo, useState, useEffect } from "react";

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed; /* Fixed layout for consistent column widths */
`;

const Th = styled.th`
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
  background-color: #f4f4f4;
  font-weight: bold;
  height: 40px;
  box-sizing: border-box;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  /* Fixed column widths */
  &:nth-child(1) {
    width: 120px;
  } /* Timestamp */
  &:nth-child(2) {
    width: 80px;
  } /* Level */
  &:nth-child(3) {
    width: 100px;
  } /* Source */
  &:nth-child(4) {
    width: 300px;
  } /* Message */
  &:nth-child(5) {
    width: 120px;
  } /* Node ID */
  &:nth-child(6) {
    width: 200px;
  } /* Payload */
`;

const Td = styled.td`
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
  height: 40px;
  box-sizing: border-box;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  /* Fixed column widths - must match header */
  &:nth-child(1) {
    width: 120px;
  } /* Timestamp */
  &:nth-child(2) {
    width: 80px;
  } /* Level */
  &:nth-child(3) {
    width: 100px;
  } /* Source */
  &:nth-child(4) {
    width: 300px;
  } /* Message */
  &:nth-child(5) {
    width: 120px;
  } /* Node ID */
  &:nth-child(6) {
    width: 200px;
  } /* Payload */
`;

const Tr = styled.tr`
  height: 40px;
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
  width: 100vw;
  height: 100vh;
  padding: 0;
  margin: 0;
`;

const Header = styled.div`
  flex-shrink: 0;
  padding: 20px;
  background-color: #f8f9fa;
  border-bottom: 1px solid #ddd;
`;

const Controls = styled.div`
  flex-shrink: 0;
  padding: 10px 20px;
  background-color: #fff;
  border-bottom: 1px solid #eee;
  text-align: center;
`;

const LogContainer = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  padding: 0 20px;
`;

// Simple live view - no scrolling complexity

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
  const flags = useMemo(() => Flags.query(ctx(), ``), []); // get all flags

  // Simple live view state - just track the window start
  const [windowStart, setWindowStart] = useState(() =>
    new Date(Date.now() - 60000).toISOString(),
  ); // Start with last 1 minute

  // Create the logs query with initial window
  const logs = useMemo(
    () =>
      LogEntry.query(
        ctx(),
        "timestamp >= '" + new Date(Date.now() - 60000).toISOString() + "'",
      ),
    [],
  );
  // Use update_selection to change the window - don't re-create the query

  const generateLogsFlag = flags.resultset.items.find(
    (flag) => flag.name === "generate_logs",
  );

  // Sort logs in descending order (newest first)
  const sortedLogs = useMemo(() => {
    return [...(logs.resultset.items || [])].sort(
      (a, b) =>
        new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime(),
    );
  }, [logs.resultset.items]);

  // Update window every 10 seconds to keep showing recent logs
  useEffect(() => {
    const interval = setInterval(async () => {
      const newWindowStart = new Date(Date.now() - 60000).toISOString(); // Always show last 1 minute
      setWindowStart(newWindowStart);

      // Update the query predicate (no upper bound for live streaming)
      const newSelection = `timestamp >= '${newWindowStart}'`;
      console.log(
        `Updating live window: ${new Date(newWindowStart).toLocaleTimeString()}`,
      );

      try {
        await logs.update_selection(newSelection);
      } catch (error) {
        console.error("Failed to update selection:", error);
      }
    }, 10000); // Every 10 seconds

    return () => clearInterval(interval);
  }, [logs]);

  const handleToggleLogGeneration = async () => {
    try {
      const transaction = ctx().begin();

      if (generateLogsFlag) {
        generateLogsFlag.edit(transaction).value.set(!generateLogsFlag?.value);
      } else {
        await Flags.create(transaction, { name: "generate_logs", value: true });
      }
      await transaction.commit();

      console.log("Log generation toggled to:", !generateLogsFlag?.value);
    } catch (error) {
      console.error("Failed to toggle log generation:", error);
    }
  };

  const handleCreateTestLog = async () => {
    try {
      const transaction = ctx().begin();
      await LogEntry.create(transaction, {
        timestamp: new Date().toISOString(),
        level: "Info",
        message: "Test log entry created from React",
        source: "react-app",
        node_id: ctx().node_id().to_base64(),
        payload: { Text: "Manual test entry" },
      });
      await transaction.commit();
      console.log("Test log entry created");
    } catch (error) {
      console.error("Failed to create test log:", error);
    }
  };

  return (
    <Container>
      <Header>
        <h1 style={{ margin: "0 0 10px 0" }}>
          Ankurah Example App - Fake Log Viewer
        </h1>
        <div style={{ fontSize: "14px", color: "#666" }}>
          <strong>Connection:</strong>{" "}
          <span
            style={{ color: connectionState === "Connected" ? "green" : "red" }}
          >
            {connectionState || "Unknown"}
          </span>
          {generateLogsFlag?.value && (
            <span style={{ marginLeft: "20px", color: "orange" }}>
              ⚡ Server generating logs
            </span>
          )}
        </div>
      </Header>

      <Controls>
        <button
          onClick={handleToggleLogGeneration}
          style={{
            backgroundColor: generateLogsFlag?.value ? "#ff4444" : "#44ff44",
            color: "white",
            padding: "10px 20px",
            border: "none",
            borderRadius: "5px",
            marginRight: "10px",
            cursor: "pointer",
          }}
        >
          {generateLogsFlag?.value
            ? "Stop Log Generation"
            : "Start Log Generation"}
        </button>
        <button
          onClick={handleCreateTestLog}
          style={{
            backgroundColor: "#4444ff",
            color: "white",
            padding: "10px 20px",
            border: "none",
            borderRadius: "5px",
            cursor: "pointer",
          }}
        >
          Create Test Log
        </button>
      </Controls>

      <LogContainer>
        <div
          style={{ textAlign: "center", marginBottom: "10px", flexShrink: 0 }}
        >
          <h3 style={{ margin: "10px 0 5px 0" }}>
            Live Log Entries ({sortedLogs.length} logs)
          </h3>
          <small>
            Showing logs from {new Date(windowStart).toLocaleTimeString()}{" "}
            onwards (live streaming)
          </small>
          <br />
          <small style={{ color: "#666" }}>
            Window updates every 10 seconds • Always showing last 1 minute
          </small>
        </div>

        <div
          style={{
            border: "1px solid #ddd",
            borderRadius: "4px",
            overflow: "hidden",
          }}
        >
          <Table>
            <thead style={{ backgroundColor: "#f8f9fa" }}>
              <tr>
                <Th>Timestamp</Th>
                <Th>Level</Th>
                <Th>Source</Th>
                <Th>Message</Th>
                <Th>Node ID</Th>
                <Th>Payload</Th>
              </tr>
            </thead>
            <tbody>
              {sortedLogs.length > 0 ? (
                sortedLogs.map((item: LogEntryView) => (
                  <LogEntryRow key={item.id.toString()} entry={item} />
                ))
              ) : (
                <tr>
                  <td
                    colSpan={6}
                    style={{
                      textAlign: "center",
                      padding: "40px 20px",
                      color: "#666",
                      fontStyle: "italic",
                    }}
                  >
                    No logs in the last minute
                  </td>
                </tr>
              )}
            </tbody>
          </Table>
        </div>
      </LogContainer>
    </Container>
  );
});

interface LogEntryRowProps {
  entry: LogEntryView;
}

const LogEntryRow: React.FC<LogEntryRowProps> = signalObserver(({ entry }) => {
  console.log("RENDER LogEntryRow");

  const handleEntryClick = async () => {
    try {
      // Create a new transaction
      const transaction = ctx().begin();

      // Edit the entry to get a mutable version
      const mutableEntry: LogEntryMut = entry.edit(transaction);

      // Mutate the entry using the config-driven WASM wrappers
      const messageWrapper = mutableEntry.message; // Returns YrsStringString wrapper (getter)
      messageWrapper.insert(0, "[EDITED] ");

      const sourceWrapper = mutableEntry.source; // Returns LWWString wrapper (getter)
      await sourceWrapper.set(entry.source + "-modified");

      // Commit the transaction
      await transaction.commit();

      console.log("Log entry updated successfully!");
    } catch (error) {
      console.error("Failed to update log entry:", error);
    }
  };

  const formatTimestamp = (timestamp: string) => {
    try {
      return new Date(timestamp).toLocaleString();
    } catch {
      return timestamp;
    }
  };

  const formatPayload = (payload: any) => {
    if (typeof payload === "object" && payload.Text) {
      return payload.Text;
    } else if (typeof payload === "object" && payload.Json) {
      return JSON.stringify(payload.Json);
    }
    return JSON.stringify(payload);
  };

  return (
    <Tr onClick={handleEntryClick} style={{ cursor: "pointer" }}>
      <Td>{formatTimestamp(entry.timestamp)}</Td>
      <Td>{JSON.stringify(entry.level)}</Td>
      <Td>{entry.source}</Td>
      <Td>{entry.message}</Td>
      <Td>{entry.node_id.substring(0, 8)}...</Td>
      <Td>{formatPayload(entry.payload)}</Td>
    </Tr>
  );
});

export default App;
