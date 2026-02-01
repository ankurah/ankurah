/**
 * Tests for JsValueCalculated signal.
 *
 * Verifies that calculated signals properly track dependencies
 * and recompute when upstream signals change.
 */

import { describe, it, expect, beforeAll } from "vitest";
import init, {
    JsValueMut,
    JsValueCalculated,
} from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

describe("JsValueCalculated", () => {
    beforeAll(async () => {
        await init();
    });

    it("computes initial value from upstream signals", () => {
        const a = new JsValueMut(5);
        const b = new JsValueMut(3);

        const sum = new JsValueCalculated(() => {
            return a.get() + b.get();
        });

        expect(sum.get()).toBe(8);
    });

    it("recomputes when upstream signal changes", () => {
        const count = new JsValueMut(1);

        const doubled = new JsValueCalculated(() => {
            return count.get() * 2;
        });

        expect(doubled.get()).toBe(2);

        count.set(5);
        expect(doubled.get()).toBe(10);

        count.set(100);
        expect(doubled.get()).toBe(200);
    });

    it("tracks multiple independent inputs", () => {
        const firstName = new JsValueMut("Alice");
        const lastName = new JsValueMut("Smith");

        const fullName = new JsValueCalculated(() => {
            return `${firstName.get()} ${lastName.get()}`;
        });

        expect(fullName.get()).toBe("Alice Smith");

        // Change first name only
        firstName.set("Bob");
        expect(fullName.get()).toBe("Bob Smith");

        // Change last name only
        lastName.set("Jones");
        expect(fullName.get()).toBe("Bob Jones");
    });

    it("supports closed-over mutable state", () => {
        const trigger = new JsValueMut(0);
        let callCount = 0;

        const counter = new JsValueCalculated(() => {
            trigger.get(); // track the trigger
            callCount += 1;
            return callCount;
        });

        expect(counter.get()).toBe(1);

        trigger.set(1);
        expect(counter.get()).toBe(2);

        trigger.set(2);
        expect(counter.get()).toBe(3);
    });

    it("notifies subscribers when value changes", () => {
        const source = new JsValueMut(10);
        const doubled = new JsValueCalculated(() => source.get() * 2);

        const receivedValues: number[] = [];
        const guard = doubled.subscribe((value: number) => {
            receivedValues.push(value);
        });

        source.set(20);
        source.set(30);

        expect(receivedValues).toEqual([40, 60]);
    });

    it("supports chained calculated signals", () => {
        const base = new JsValueMut(2);

        const doubled = new JsValueCalculated(() => base.get() * 2);
        const quadrupled = new JsValueCalculated(() => doubled.get() * 2);

        expect(quadrupled.get()).toBe(8);

        base.set(5);
        expect(quadrupled.get()).toBe(20);
    });

    it("peek() does not track dependencies", () => {
        const tracked = new JsValueMut(1);
        const untracked = new JsValueMut(100);
        let computeCount = 0;

        const result = new JsValueCalculated(() => {
            computeCount++;
            return tracked.get() + untracked.peek();
        });

        expect(result.get()).toBe(101);
        expect(computeCount).toBe(1);

        // Changing tracked signal should recompute
        tracked.set(2);
        expect(result.get()).toBe(102);
        expect(computeCount).toBe(2);

        // Changing untracked signal should NOT recompute
        untracked.set(200);
        expect(result.get()).toBe(102); // Still uses cached value
        expect(computeCount).toBe(2); // No recompute
    });
});
