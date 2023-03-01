import delay from 'delay';

export type Worker = {
  notify: () => void;
  start: () => void;
  stop: () => Promise<void>;
};

type ShouldContinue = boolean | void | undefined;

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
    if (!running) {
      return;
    }

    running = false;
    // fix for clear bug
    setImmediate(() => loopDelayPromise?.clear());

    await loopPromise;
  }

  return {
    start,
    notify,
    stop,
  };
}
