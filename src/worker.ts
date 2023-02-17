import delay from 'delay';

export type Worker = {
  notify: () => void;
  start: () => void;
  stop: () => Promise<void>;
};

export const resolveWithinSeconds = async (promise: Promise<any>, seconds: number) => {
  const timeout = Math.max(1, seconds) * 1000;
  const timeoutReject = delay.reject(timeout, { value: new Error(`handler execution exceeded ${timeout}ms`) });

  let result;

  try {
    result = await Promise.race([promise, timeoutReject]);
  } finally {
    try {
      timeoutReject.clear();
    } catch {}
  }

  return result;
};

type ShouldContinue = boolean;

export function createBaseWorker(run: () => Promise<ShouldContinue>, props: { loopInterval: number }): Worker {
  const { loopInterval } = props;
  let loopPromise: Promise<any>;
  let loopDelayPromise: delay.ClearablePromise<void> | null = null;
  let running = false;

  async function loop() {
    while (running) {
      const started = Date.now();
      const shouldContinue = await run();
      const duration = Date.now() - started;

      if (!shouldContinue && duration < loopInterval && running) {
        loopDelayPromise = delay(loopInterval - duration);
        await loopDelayPromise;
      }
    }
  }

  function notify() {
    if (loopDelayPromise && running) {
      loopDelayPromise.clear();
    }
  }

  function start() {
    if (running) {
      return;
    }
    running = true;
    loopPromise = loop();
  }

  async function stop() {
    running = false;
    await loopPromise;
  }

  return {
    start,
    notify,
    stop,
  };
}
