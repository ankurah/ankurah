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
import { useMemo, useState, useEffect, useRef } from "react";

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
  height: 30px;
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
  height: 30px;
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
  height: 30px;
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
  position: relative;
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

// Scroll controller class to manage pagination logic
class ScrollController {
  private logs: any;
  private setIsLoadingMore: (loading: boolean) => void;
  private setIsLiveMode: (live: boolean) => void;
  private setLastPredicate: (predicate: string) => void;
  private setScrollDebug: (debug: any) => void;
  private scrollContainerRef: React.RefObject<HTMLDivElement>;
  private selectionBaseRef: React.MutableRefObject<string>;
  private isOperationInProgress = false; // Class-level guard
  private lastLimit: number = 0; // Track last limit to avoid unnecessary updates
  private lastScrollDebug: any = null; // Track last scroll debug to avoid unnecessary updates

  public readonly ROW_HEIGHT = 30;
  private currentTopPadPx = 0; // Track current padding internally

  // Get the spacer element (table row used for padding)
  private getSpacerElement = (): HTMLElement | null => {
    const scrollContainer = this.scrollContainerRef.current;
    if (!scrollContainer) return null;

    // Find the spacer row (first tbody > tr with only one td)
    const spacerRow = scrollContainer.querySelector("tbody tr:first-child");
    return spacerRow as HTMLElement;
  };

  // Set top padding directly via DOM
  setTopPadding = (paddingPx: number): void => {
    const spacerElement = this.getSpacerElement();
    if (spacerElement) {
      const cell = spacerElement.querySelector("td");
      if (cell) {
        (cell as HTMLElement).style.height = `${paddingPx}px`;
        this.currentTopPadPx = paddingPx;
        console.log(`Set top padding to: ${paddingPx}px`);
      }
    }
  };

  // Get current top padding
  getTopPadding = (): number => {
    return this.currentTopPadPx;
  };

  // Apply scroll delta directly to DOM
  applyScrollDelta = (delta: number): void => {
    const scrollContainer = this.scrollContainerRef.current;
    if (scrollContainer) {
      scrollContainer.scrollTop += delta;
      console.log(
        `Applied scroll delta: ${delta}px, new scrollTop: ${scrollContainer.scrollTop}`,
      );
    }
  };

  // Test method to add 100px to both padding and scroll
  testPaddingScrollAlignment = (): void => {
    const scrollContainer = this.scrollContainerRef.current;
    if (!scrollContainer) return;

    const beforeScroll = scrollContainer.scrollTop;
    const beforeHeight = scrollContainer.scrollHeight;
    const beforeClientHeight = scrollContainer.clientHeight;
    const beforePadding = this.getTopPadding();

    console.log("=== BUTTON PRESS TEST ===");
    console.log(
      "BEFORE: scrollTop =",
      beforeScroll,
      "scrollHeight =",
      beforeHeight,
      "clientHeight =",
      beforeClientHeight,
    );
    console.log("BEFORE: padding =", beforePadding);
    console.log(
      "BEFORE: scroll position as %:",
      ((beforeScroll / (beforeHeight - beforeClientHeight)) * 100).toFixed(2) +
        "%",
    );

    // Set new padding directly via DOM
    const newPadding = beforePadding + 100;
    this.setTopPadding(newPadding);

    // Measure actual height change immediately (no async timing issues)
    const afterHeight = scrollContainer.scrollHeight;
    const actualDelta = afterHeight - beforeHeight;
    console.log(
      "AFTER PADDING: scrollHeight =",
      afterHeight,
      "actual delta =",
      actualDelta,
    );

    // Apply scroll adjustment based on actual measured change
    const beforeScrollAdjust = scrollContainer.scrollTop;
    this.applyScrollDelta(actualDelta);
    const afterScrollAdjust = scrollContainer.scrollTop;

    const finalScroll = scrollContainer.scrollTop;
    const finalHeight = scrollContainer.scrollHeight;
    const finalClientHeight = scrollContainer.clientHeight;

    console.log(
      "SCROLL ADJUSTMENT: from",
      beforeScrollAdjust,
      "to",
      afterScrollAdjust,
      "delta applied:",
      actualDelta,
    );
    console.log(
      "FINAL: scrollTop =",
      finalScroll,
      "scrollHeight =",
      finalHeight,
      "clientHeight =",
      finalClientHeight,
    );
    console.log(
      "FINAL: scroll position as %:",
      ((finalScroll / (finalHeight - finalClientHeight)) * 100).toFixed(2) +
        "%",
    );
    console.log(
      "Net scroll change:",
      finalScroll - beforeScroll,
      "(should be 0)",
    );
    console.log(
      "Visual position change:",
      finalScroll - beforeScroll - actualDelta,
      "(should be 0)",
    );
    console.log("========================");
  };

  constructor(
    logs: any,
    setters: {
      setIsLoadingMore: (loading: boolean) => void;
      setIsLiveMode: (live: boolean) => void;
      setLastPredicate: (predicate: string) => void;
      setScrollDebug: (debug: any) => void;
    },
    refs: {
      scrollContainerRef: React.RefObject<HTMLDivElement>;
      selectionBaseRef: React.MutableRefObject<string>;
    },
  ) {
    this.logs = logs;
    this.setIsLoadingMore = setters.setIsLoadingMore;
    this.setIsLiveMode = setters.setIsLiveMode;
    this.setLastPredicate = setters.setLastPredicate;
    this.setScrollDebug = setters.setScrollDebug;
    this.scrollContainerRef = refs.scrollContainerRef;
    this.selectionBaseRef = refs.selectionBaseRef;
  }

  computeLimit = (): number => {
    const el = this.scrollContainerRef.current;
    const height = el?.clientHeight ?? 600;
    const rowsPerScreen = Math.max(1, Math.floor(height / this.ROW_HEIGHT));
    const dynamicLimit = rowsPerScreen * 2;
    return Math.max(20, dynamicLimit); // Minimum of 20 rows
  };

  loadOlderLogs = async (
    logEntries: any[],
    isLoadingMore: boolean,
  ): Promise<void> => {
    if (isLoadingMore || logEntries.length === 0) {
      console.log("loadOlderLogs: skipping due to loading or no entries");
      return;
    }
    console.log("loadOlderLogs: starting...");
    this.setIsLoadingMore(true);
    // Don't set isLiveMode(false) until after update_selection succeeds
    try {
      const k = Math.floor(logEntries.length * 0.5);
      const cont = logEntries[k];
      const rowsAbove = k - 2; // no idea why, but this makes it work. there's still a little blink, but it's very subtle now, instead of jumping like 3 extra rows upward
      const padDelta = rowsAbove * this.ROW_HEIGHT;

      // Apply padding and scroll atomically via DOM
      const scrollContainer = this.scrollContainerRef.current;
      if (scrollContainer) {
        const beforeHeight = scrollContainer.scrollHeight;
        const newPadding = this.getTopPadding() + padDelta;
        this.setTopPadding(newPadding);
        const afterHeight = scrollContainer.scrollHeight;
        const actualDelta = afterHeight - beforeHeight;
        this.applyScrollDelta(actualDelta);
      }
      const selection = `timestamp < '${cont.timestamp}' ORDER BY timestamp DESC`;
      const limit = this.computeLimit();
      const fullPredicate = `${selection} LIMIT ${limit}`;
      this.selectionBaseRef.current = selection;
      this.setLastPredicate(fullPredicate);
      await this.logs.update_selection(fullPredicate);
      // Only set historical mode after successful update
      this.setIsLiveMode(false);
    } catch (error) {
      console.error("Failed to load older logs:", error);
    } finally {
      this.setIsLoadingMore(false);
    }
  };

  loadNewerLogs = async (
    logEntries: any[],
    isLoadingMore: boolean,
  ): Promise<void> => {
    if (isLoadingMore || logEntries.length === 0) {
      console.log("loadNewerLogs: skipping due to loading or no entries");
      return;
    }
    console.log("loadNewerLogs: starting...");
    this.setIsLoadingMore(true);
    try {
      const k = Math.floor(logEntries.length * 0.5);
      const cont = logEntries[k];
      const rowsAbove = k + 1;
      const padDelta = rowsAbove * this.ROW_HEIGHT;

      // Apply padding and scroll atomically via DOM
      const scrollContainer = this.scrollContainerRef.current;
      if (scrollContainer) {
        const beforeHeight = scrollContainer.scrollHeight;
        const newPadding = Math.max(0, this.getTopPadding() - padDelta);
        this.setTopPadding(newPadding);
        const afterHeight = scrollContainer.scrollHeight;
        const actualDelta = afterHeight - beforeHeight;
        this.applyScrollDelta(actualDelta);
      }
      const selection = `timestamp > '${cont.timestamp}' ORDER BY timestamp ASC`;
      const limit = this.computeLimit();
      const fullPredicate = `${selection} LIMIT ${limit}`;
      this.selectionBaseRef.current = selection;
      this.setLastPredicate(fullPredicate);
      await this.logs.update_selection(fullPredicate);
    } catch (error) {
      console.error("Failed to load newer logs:", error);
    } finally {
      this.setIsLoadingMore(false);
    }
  };

  returnToLiveMode = async (): Promise<void> => {
    const limit = this.computeLimit();
    const fullPredicate = `true ORDER BY timestamp DESC LIMIT ${limit}`;
    this.selectionBaseRef.current = `true ORDER BY timestamp DESC`;
    this.setLastPredicate(fullPredicate);
    await this.logs.update_selection(fullPredicate);
    this.setIsLiveMode(true);
    this.setTopPadding(0);
    const el = this.scrollContainerRef.current;
    if (el) el.scrollTop = 0;
  };

  private scrollTimeout: number | null = null;

  handleScroll = (
    logEntries: any[],
    isLoadingMore: boolean,
    isLiveMode: boolean,
  ): void => {
    // Debounce scroll events to prevent busy loop
    if (this.scrollTimeout) {
      clearTimeout(this.scrollTimeout);
    }

    this.scrollTimeout = setTimeout(() => {
      const scrollContainer = this.scrollContainerRef.current;
      if (!scrollContainer || isLoadingMore || this.isOperationInProgress) {
        console.log("Scroll: skipping due to loading or operation in progress");
        return;
      }

      // Update scroll debug info
      this.updateScrollDebug();

      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      const nearBottom = scrollTop + clientHeight >= scrollHeight - 100;
      const nearTop = scrollTop <= 50;

      console.log("Scroll check:", {
        scrollTop,
        scrollHeight,
        clientHeight,
        nearTop,
        nearBottom,
        isLiveMode,
        logCount: logEntries.length,
      });

      if (nearBottom && logEntries.length > 0) {
        console.log("Loading older logs...");
        this.isOperationInProgress = true;
        this.loadOlderLogs(logEntries, isLoadingMore).finally(() => {
          this.isOperationInProgress = false;
        });
      } else if (nearTop && !isLiveMode) {
        if (logEntries.length < (this.logs.resultset.limit || 20)) {
          console.log("Returning to live mode (short of limit)...");
          this.isOperationInProgress = true;
          this.returnToLiveMode().finally(() => {
            this.isOperationInProgress = false;
          });
        } else {
          console.log("Loading newer logs...");
          this.isOperationInProgress = true;
          this.loadNewerLogs(logEntries, isLoadingMore).finally(() => {
            this.isOperationInProgress = false;
          });
        }
      }
    }, 200); // Increased debounce to 200ms
  };

  updateScrollDebug = (): void => {
    const scrollContainer = this.scrollContainerRef.current;
    if (!scrollContainer) return;

    const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
    const nearBottom = scrollTop + clientHeight >= scrollHeight - 100;
    const nearTop = scrollTop <= 50;

    // Calculate pixels until next pagination trigger
    const pixelsUntilLoadOlder = Math.max(
      0,
      scrollHeight - 100 - (scrollTop + clientHeight),
    );
    const pixelsUntilLoadNewer = Math.max(0, 50 - scrollTop);

    const newScrollDebug = {
      nearTop,
      nearBottom,
      scrollTop,
      scrollHeight,
      clientHeight,
      pixelsUntilLoadOlder,
      pixelsUntilLoadNewer,
      isLiveMode:
        this.selectionBaseRef.current === "true ORDER BY timestamp DESC",
      logCount: 0, // Will be updated by caller
    };

    // Only update if values actually changed
    if (
      this.lastScrollDebug &&
      this.lastScrollDebug.scrollTop === newScrollDebug.scrollTop &&
      this.lastScrollDebug.scrollHeight === newScrollDebug.scrollHeight &&
      this.lastScrollDebug.pixelsUntilLoadOlder ===
        newScrollDebug.pixelsUntilLoadOlder &&
      this.lastScrollDebug.pixelsUntilLoadNewer ===
        newScrollDebug.pixelsUntilLoadNewer
    ) {
      return; // No change, skip update
    }

    this.lastScrollDebug = newScrollDebug;
    this.setScrollDebug(newScrollDebug);
  };

  updateLimit = (): void => {
    if (this.isOperationInProgress) {
      console.log("updateLimit: skipping due to operation in progress");
      return;
    }
    const limit = this.computeLimit();

    // Only update if limit actually changed
    if (limit === this.lastLimit) {
      console.log("updateLimit: skipping, limit unchanged:", limit);
      // Still update scroll debug even if limit unchanged
      this.updateScrollDebug();
      return;
    }

    this.lastLimit = limit;
    const base = this.selectionBaseRef.current;
    const fullPredicate = `${base} LIMIT ${limit}`;
    console.log("updateLimit: updating with new limit:", { base, limit });
    this.setLastPredicate(fullPredicate);
    this.logs.update_selection(fullPredicate).catch(() => {});
    // Update scroll debug after selection update
    setTimeout(() => this.updateScrollDebug(), 50);
  };
}

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
  const connectionState = ws_client().connection_state.value?.value();
  const flags = useMemo(() => Flags.query(ctx(), ``), []); // get all flags

  // Pagination state
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [isLiveMode, setIsLiveMode] = useState(true);
  const [lastPredicate, setLastPredicate] = useState(
    "true ORDER BY timestamp DESC LIMIT 20",
  );
  const [scrollDebug, setScrollDebug] = useState({
    nearTop: false,
    nearBottom: false,
    scrollTop: 0,
    scrollHeight: 0,
    pixelsUntilLoadOlder: 0,
    pixelsUntilLoadNewer: 0,
  });
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const selectionBaseRef = useRef<string>("true ORDER BY timestamp DESC");

  // Create the logs query
  const logs = useMemo(
    () => LogEntry.query(ctx(), "true ORDER BY timestamp DESC LIMIT 100"),
    [],
  );

  const generateLogsFlag = flags.resultset.items.find(
    (flag: { name: string; value: boolean }) => flag.name === "generate_logs",
  );
  const logEntries = useMemo(
    () => logs.resultset.items || [],
    [logs.resultset.items],
  );

  // Determine live mode based on actual query predicate rather than state
  const actualIsLiveMode = useMemo(() => {
    return selectionBaseRef.current === "true ORDER BY timestamp DESC";
  }, []); // Run once on mount

  // Create scroll controller
  const controller = useMemo(
    () =>
      new ScrollController(
        logs,
        { setIsLoadingMore, setIsLiveMode, setLastPredicate, setScrollDebug },
        { scrollContainerRef, selectionBaseRef },
      ),
    [logs],
  );

  // Initial scroll debug update
  useEffect(() => {
    controller.updateScrollDebug();
  }, [controller]);

  // Initial setup - debounced to avoid double calls during mount
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;

    // Debounce initial updateLimit to avoid firing during rapid size changes
    const timeout = setTimeout(() => {
      controller.updateLimit();
      // Also update scroll debug initially
      controller.updateScrollDebug();
    }, 100);

    return () => clearTimeout(timeout);
  }, [controller]); // Only depend on controller

  // Handle resize and scroll events
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;

    // Throttled resize observer (no initial updateLimit call)
    let resizeTimeout: number | null = null;
    const ro = new ResizeObserver(() => {
      if (resizeTimeout) clearTimeout(resizeTimeout);
      resizeTimeout = setTimeout(() => controller.updateLimit(), 300);
    });
    ro.observe(el);

    // Throttled scroll handler
    let scrollTimeout: number | null = null;
    const handleScroll = () => {
      if (scrollTimeout) clearTimeout(scrollTimeout);
      scrollTimeout = setTimeout(() => {
        controller.handleScroll(logEntries, isLoadingMore, isLiveMode);
      }, 100);
    };
    el.addEventListener("scroll", handleScroll, { passive: true });

    return () => {
      ro.disconnect();
      el.removeEventListener("scroll", handleScroll);
      if (resizeTimeout) clearTimeout(resizeTimeout);
      if (scrollTimeout) clearTimeout(scrollTimeout);
    };
  }, [controller, logEntries, isLoadingMore, isLiveMode]);

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

        {/* Debug info in top right corner */}
        <div
          style={{
            position: "absolute",
            top: "10px",
            right: "10px",
            fontSize: "11px",
            color: "#666",
            backgroundColor: "rgba(255,255,255,0.9)",
            padding: "8px",
            borderRadius: "4px",
            border: "1px solid #ddd",
            textAlign: "left",
            minWidth: "600px",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              marginBottom: "4px",
            }}
          >
            <strong>Query:</strong>
            <div
              style={{
                width: "8px",
                height: "8px",
                borderRadius: "50%",
                backgroundColor: isLoadingMore ? "#22c55e" : "#e5e7eb",
                marginLeft: "8px",
                transition: "background-color 0.2s",
              }}
            />
          </div>
          <div style={{ fontSize: "10px", marginBottom: "4px" }}>
            {lastPredicate}
          </div>
          <div>
            <strong>Next Page Triggers:</strong>
          </div>
          <div style={{ marginLeft: "10px" }}>
            • Load Older: {scrollDebug.pixelsUntilLoadOlder || 0}px remaining
          </div>
          <div style={{ marginLeft: "10px" }}>
            • Load Newer:{" "}
            {actualIsLiveMode
              ? "live"
              : `${scrollDebug.pixelsUntilLoadNewer || 0}px remaining`}
          </div>
          <div>
            <strong>Current Set:</strong> {logEntries.length} rows
          </div>
          <div>
            <strong>Virtual Padding:</strong> {controller.getTopPadding()}px (
            {Math.round(controller.getTopPadding() / 30)} rows above)
          </div>
          <button
            onClick={() => controller.testPaddingScrollAlignment()}
            style={{
              marginTop: "8px",
              padding: "4px 8px",
              fontSize: "10px",
              backgroundColor: "#f0f0f0",
              border: "1px solid #ccc",
              borderRadius: "3px",
              cursor: "pointer",
            }}
          >
            Test +100px Padding/Scroll
          </button>
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
            {actualIsLiveMode ? "Showing latest logs" : "Historical view"}{" "}
            (ordered by timestamp)
          </h3>
        </div>

        <div
          ref={scrollContainerRef}
          style={{
            border: "1px solid #ddd",
            borderRadius: "4px",
            overflow: "auto",
            maxHeight: "calc(100vh - 300px)",
          }}
        >
          <Table>
            <thead
              style={{ backgroundColor: "#f8f9fa", position: "sticky", top: 0 }}
            >
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
              <tr aria-hidden="true">
                <td colSpan={6} style={{ height: 0, padding: 0, border: 0 }} />
              </tr>
              {logEntries.length > 0 ? (
                logEntries.map((item: LogEntryView) => (
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
                    No logs available
                  </td>
                </tr>
              )}
              {isLoadingMore && (
                <tr>
                  <td
                    colSpan={6}
                    style={{
                      textAlign: "center",
                      padding: "20px",
                      color: "#666",
                      fontStyle: "italic",
                    }}
                  >
                    Loading more logs...
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

  const formatPayload = (payload: unknown) => {
    if (typeof payload === "object" && payload !== null && "Text" in payload) {
      return (payload as { Text: string }).Text;
    } else if (
      typeof payload === "object" &&
      payload !== null &&
      "Json" in payload
    ) {
      return JSON.stringify((payload as { Json: unknown }).Json);
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
