import { once } from 'events';
import { EventEmitter } from 'stream';
import tap from 'tap';
import { createBaseWorker } from '../src/workers/base';

tap.test('baseworker', async (tap) => {
  tap.test('loops', async (t) => {
    t.plan(1);
    let called = 0;
    const worker = createBaseWorker(
      async () => {
        called += 1;
        return false;
      },
      { loopInterval: 100 }
    );

    worker.start();

    await new Promise((resole) => setTimeout(resole, 210));

    // 3 because start calls immedialtly
    t.equal(called, 3);
    await worker.stop();
  });

  tap.test('continues', async (t) => {
    t.plan(2);
    const ee = new EventEmitter();
    const worker = createBaseWorker(
      async () => {
        ee.emit('loop');
        t.pass('loop');
        return true;
      },
      { loopInterval: 10000000 }
    );

    worker.start();
    await once(ee, 'loop');
    await worker.stop();
  });

  tap.test('notifies', async (t) => {
    t.plan(3);
    const ee = new EventEmitter();
    const worker = createBaseWorker(
      async () => {
        ee.emit('loop');
        t.pass('loop');
        return true;
      },
      { loopInterval: 10000000 }
    );

    worker.start();
    await once(ee, 'loop');
    worker.notify();
    await once(ee, 'loop');
    await worker.stop();
  });

  tap.test('multiple starts', async (t) => {
    t.plan(1);
    let called = 0;
    const worker = createBaseWorker(
      async () => {
        called += 1;
        return false;
      },
      { loopInterval: 100 }
    );

    worker.start();
    worker.start();
    worker.start();

    await new Promise((resolve) => setTimeout(resolve, 210));

    t.equal(called, 3);
    await worker.stop();
  });
});
