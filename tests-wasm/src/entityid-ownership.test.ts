/**
 * Tests for EntityId ownership semantics in WASM bindings.
 *
 * Verifies that EntityId is not consumed when passed to functions
 * that only need to read it (like by_id).
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import init, {
    create_test_context,
    cleanup_test_db,
    TestUser,
    Context,
} from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

describe("EntityId ownership", () => {
    let ctx: Context;
    const dbName = `test_entityid_ownership_${Date.now()}`;

    beforeAll(async () => {
        await init();
        ctx = await create_test_context(dbName);
    });

    afterAll(async () => {
        await cleanup_test_db(dbName);
    });

    it("by_id does not consume EntityId", async () => {
        // Create a user
        const user = await TestUser.create_one(ctx, { name: "Alice" });
        const userId = user.id;

        // Get the query/resultset (empty string = no filter)
        const query = TestUser.query(ctx, "");

        // Wait for it to load
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Call by_id with the EntityId
        const found = query.resultset.by_id(userId);
        expect(found).toBeDefined();
        expect(found!.name).toBe("Alice");

        // The EntityId should still be usable after by_id call
        // This would fail with "Attempt to use a moved value" if by_id consumed it
        const base64 = userId.to_base64();
        expect(base64).toBeTruthy();
        expect(typeof base64).toBe("string");

        // Can call by_id again with the same EntityId
        const foundAgain = query.resultset.by_id(userId);
        expect(foundAgain).toBeDefined();
        expect(foundAgain!.name).toBe("Alice");

        // EntityId still works
        expect(userId.to_base64()).toBe(base64);
    });

    it("EntityId can be used multiple times after creation", async () => {
        const user = await TestUser.create_one(ctx, { name: "Bob" });
        const userId = user.id;

        // Use the EntityId multiple times
        const b64_1 = userId.to_base64();
        const b64_2 = userId.to_base64();
        const b64_3 = userId.to_base64();

        expect(b64_1).toBe(b64_2);
        expect(b64_2).toBe(b64_3);

        // Can also compare with equals
        expect(userId.equals(userId)).toBe(true);
    });
});
