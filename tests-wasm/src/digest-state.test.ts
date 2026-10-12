import { expect, it } from "vitest";
import init, { digest_state_browser_roundtrip } from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

it("compares Fetch and live-query state between two browser nodes", async () => {
    await init();
    await expect(digest_state_browser_roundtrip(`digest_state_${Date.now()}`)).resolves.toBeUndefined();
});
