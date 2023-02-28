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
    maxTimeoutId = setTimeout(invoke, Math.max(0, opts.maxMs - (Date.now() - startedTime)));
  };
};
