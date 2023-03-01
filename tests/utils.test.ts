import tap from 'tap';
import { debounce } from '../src/utils';

tap.test('debounce', async (t) => {
  t.test('debounces', async ({ equal }) => {
    let called = 0;
    const fn = () => ++called;

    const debouncedFn = debounce(fn, { ms: 50, maxMs: 3000 });

    debouncedFn();
    debouncedFn();
    debouncedFn();

    await new Promise((resolve) => setTimeout(resolve, 100));

    debouncedFn();
    debouncedFn();

    await new Promise((resolve) => setTimeout(resolve, 60));

    equal(called, 2);
  });

  t.test('maxWait', async ({ equal }) => {
    let called = 0;
    const fn = () => {
      ++called;
    };

    const debouncedFn = debounce(fn, { ms: 50, maxMs: 100 });

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 20));

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 20));

    equal(called, 0);

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 70));

    equal(called, 1);

    debouncedFn();
    debouncedFn();
    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 60));

    equal(called, 2);

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 20));

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 20));

    debouncedFn();
    await new Promise((resolve) => setTimeout(resolve, 70));
    equal(called, 3);
  });

  t.test('calls with latest', async ({ equal, plan }) => {
    plan(1);
    const fn = (input: string) => {
      equal(input, 'last');
    };

    const debouncedFn = debounce(fn, { ms: 50, maxMs: 100 });

    debouncedFn('firsit');
    await new Promise((resolve) => setTimeout(resolve, 20));

    debouncedFn('second');
    await new Promise((resolve) => setTimeout(resolve, 20));

    debouncedFn('last');
    await new Promise((resolve) => setTimeout(resolve, 70));
  });

  t.test('maxwait with latest', async ({ equal, plan }) => {
    plan(1);
    const fn = (input: string) => {
      equal(input, 'second');
    };

    const debouncedFn = debounce(fn, { ms: 50, maxMs: 60 });

    debouncedFn('first');
    await new Promise((resolve) => setTimeout(resolve, 40));

    debouncedFn('second');
    await new Promise((resolve) => setTimeout(resolve, 40));
  });
});
