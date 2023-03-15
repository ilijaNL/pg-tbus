import delay from 'delay';

type Item<T> = T & {
  /**
   * Delta time in millisonds between adding item and handling the batch
   */
  delta_ms: number;
};

export type BatcherConfig = { maxSize: number; maxTimeInMs: number };

function createBatcher<T>(flushBatch: (batch: Array<Item<T>>) => Promise<any>, config: BatcherConfig) {
  const dataArray: Array<T & { at: number }> = [];
  let delayPromise: delay.ClearablePromise<void> | null = null;
  let waitPromise: Promise<any> = Promise.resolve();

  /**
   * Flushes the current batch (if any items)
   * @returns
   */
  async function flush() {
    // already flushed somewhere else
    if (dataArray.length === 0) {
      return;
    }

    const now = Date.now();
    const currentDataArray = dataArray.map<Item<T>>((d) => ({ ...d, delta_ms: now - d.at }));
    dataArray.length = 0;

    await flushBatch(currentDataArray);
  }

  /**
   * Add an item to the current batch.
   * Resolves the promise when the batch is flushed
   * @param data
   */
  async function addAndWait(data: T): Promise<void> {
    dataArray.push({ ...data, at: Date.now() });

    if (dataArray.length >= config.maxSize) {
      delayPromise?.clear();
    }

    // first item, schedule a delay of maxTime and after flush
    if (dataArray.length === 1) {
      delayPromise = delay(config.maxTimeInMs);
      waitPromise = Promise.resolve()
        .then(() => delayPromise)
        .then(() => flush())
        .catch((err) => {});
    }

    await waitPromise;
  }

  return {
    add: addAndWait,
    flush,
  };
}

export default createBatcher;
