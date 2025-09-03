/**
 * TypeScript equivalent of the Rust TestWatcher utility
 * Accumulates notifications and provides async waiting methods
 */
export class TestWatcher<T, U = T> {
    private changes: T[] = [];
    private resolvers: Array<(value: U[]) => void> = [];
    private transform: (item: T) => U;

    constructor(transform?: (item: T) => U) {
        this.transform = transform || ((item: T) => item as unknown as U);
    }

    /**
     * Creates a new TestWatcher with identity transform
     */
    static new<T>(): TestWatcher<T, T> {
        return new TestWatcher<T, T>();
    }

    /**
     * Creates a new TestWatcher with a custom transform function
     */
    static transform<T, U>(transform: (item: T) => U): TestWatcher<T, U> {
        return new TestWatcher<T, U>(transform);
    }

    /**
     * Add a new item to the watcher
     */
    notify(item: T): void {
        this.changes.push(item);
        this.notifyWaiters();
    }

    /**
     * Takes (empties and returns) all accumulated items, applying the transform
     */
    drain(): U[] {
        const items = this.changes.splice(0);
        return items.map(item => this.transform(item));
    }

    /**
     * Waits for exactly `count` items to accumulate, then drains and returns them
     */
    async take(count: number, timeoutMs: number = 10000): Promise<U[]> {
        await this.waitForCount(count, timeoutMs);
        const items = this.changes.splice(0, count);
        return items.map(item => this.transform(item));
    }

    /**
     * Returns the current number of accumulated items without draining them
     */
    count(): number {
        return this.changes.length;
    }

    /**
     * Waits for at least one item to be accumulated (with 10 second timeout)
     */
    async wait(timeoutMs: number = 10000): Promise<boolean> {
        return this.waitForCount(1, timeoutMs);
    }

    /**
     * Waits for at least 1 item, then takes and returns exactly the first one
     */
    async takeOne(timeoutMs: number = 10000): Promise<U> {
        const success = await this.waitForCount(1, timeoutMs);
        if (!success || this.changes.length === 0) {
            throw new Error(`takeOne() timed out waiting for items (waited ${timeoutMs}ms, got ${this.changes.length} items)`);
        }
        const item = this.changes.shift()!;
        return this.transform(item);
    }

    /**
     * Waits 100ms for any additional items, then returns the count (useful for asserting quiescence)
     */
    async quiesce(waitMs: number = 100): Promise<number> {
        await new Promise(resolve => setTimeout(resolve, waitMs));
        return this.count();
    }

    /**
     * Wait for at least `count` items to accumulate, then drain and return all items
     */
    async takeWhen(count: number, timeoutMs: number = 10000): Promise<U[]> {
        await this.waitForCount(count, timeoutMs);
        return this.drain();
    }

    /**
     * Waits for at least `count` items to be accumulated, with timeout
     */
    private async waitForCount(count: number, timeoutMs: number): Promise<boolean> {
        // Check if we already have enough changes
        if (this.changes.length >= count) {
            return true;
        }

        return new Promise<boolean>((resolve) => {
            const timeoutId = setTimeout(() => {
                // Remove this resolver from the list
                const index = this.resolvers.indexOf(checkAndResolve);
                if (index > -1) {
                    this.resolvers.splice(index, 1);
                }
                resolve(false);
            }, timeoutMs);

            const checkAndResolve = () => {
                if (this.changes.length >= count) {
                    clearTimeout(timeoutId);
                    // Remove this resolver from the list
                    const index = this.resolvers.indexOf(checkAndResolve);
                    if (index > -1) {
                        this.resolvers.splice(index, 1);
                    }
                    resolve(true);
                }
            };

            this.resolvers.push(checkAndResolve);

            // Check immediately in case items were added between the initial check and setting up the resolver
            checkAndResolve();
        });
    }

    /**
     * Notify all waiting resolvers
     */
    private notifyWaiters(): void {
        // Make a copy of resolvers to avoid modification during iteration
        const currentResolvers = [...this.resolvers];
        currentResolvers.forEach(resolver => resolver([]));
    }

    /**
     * Create a callback function that can be used with signal subscriptions
     */
    createCallback(): (item: T) => void {
        return (item: T) => this.notify(item);
    }
}
