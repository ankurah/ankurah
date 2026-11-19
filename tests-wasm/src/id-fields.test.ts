/**
 * Tests for plain EntityId and NodeId fields at the JS→Rust boundary.
 *
 * create() takes an id field as its base64 text, which is what serde reads. A
 * query argument takes an id object and keeps its id type, so a NodeId object
 * is never read as an entity id.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import init, { create_test_context, cleanup_test_db, TestUser, TestEndorsement, Context } from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

describe("EntityId and NodeId fields in create() and query arguments", () => {
    let ctx: Context;
    const dbName = `test_id_fields_${Date.now()}`;

    beforeAll(async () => {
        await init();
        ctx = await create_test_context(dbName);
    });

    afterAll(async () => {
        await cleanup_test_db(dbName);
    });

    it("create() stores base64 text for EntityId and NodeId fields", async () => {
        const user = await TestUser.create_one(ctx, { name: "Alice" });
        const endorsement = await TestEndorsement.create_one(ctx, { endorsed: ctx.node_id().to_base64(), subject: user.id.to_base64() });

        expect(endorsement.endorsed.to_base64()).toBe(ctx.node_id().to_base64());
        expect(endorsement.subject.equals(user.id)).toBe(true);
    });

    it("a NodeId object query argument matches a NodeId field", async () => {
        const user = await TestUser.create_one(ctx, { name: "Bob" });
        const endorsement = await TestEndorsement.create_one(ctx, { endorsed: ctx.node_id().to_base64(), subject: user.id.to_base64() });
        const found = await TestEndorsement.fetch(ctx, "endorsed = ?", ctx.node_id());

        expect(found.map((view: { id: { to_base64(): string } }) => view.id.to_base64())).toContain(endorsement.id.to_base64());
        // A node id literal never converts to an entity id, so the same object is refused against an EntityId field.
        await expect(TestEndorsement.fetch(ctx, "subject = ?", ctx.node_id())).rejects.toThrow();
    });
});
