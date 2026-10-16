import { expect, it } from "vitest";
import init, { digest_state_browser_roundtrip, digest_state_browser_interleaved } from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

it("compares Fetch and live-query state between two browser nodes", async () => {
    await init();
    await expect(digest_state_browser_roundtrip(`digest_state_${Date.now()}`)).resolves.toBeUndefined();
});

it("applies live changes while comparison feedback is held and preserves later removals", async () => {
    await init();
    await expect(digest_state_browser_interleaved(`digest_interleaved_${Date.now()}`)).resolves.toBeUndefined();
}, 60000);
