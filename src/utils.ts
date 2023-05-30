import delay from 'delay';

export const debounce = <Args extends any[], F extends (...args: Args) => any>(
  fn: F,
  opts: { ms: number; maxMs: number }
) => {
  let timeoutId: ReturnType<typeof setTimeout>;
  let maxTimeoutId: ReturnType<typeof setTimeout>;
  let isFirstInvoke = true;
  let startedTime = Date.now();

  return function call(this: ThisParameterType<F>, ...args: Parameters<F>) {
    clearTimeout(timeoutId);
    clearTimeout(maxTimeoutId);

    const invoke = () => {
      clearTimeout(maxTimeoutId);
      clearTimeout(timeoutId);

      isFirstInvoke = true;

      fn.apply(this, args);
    };

    if (isFirstInvoke) {
      isFirstInvoke = false;
      startedTime = Date.now();
    }

    timeoutId = setTimeout(invoke, opts.ms);
    // need to reset every time otherwise it wont use the latest this & args in invoke
    maxTimeoutId = setTimeout(invoke, Math.max(0, opts.maxMs - (Date.now() - startedTime)));
  };
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
