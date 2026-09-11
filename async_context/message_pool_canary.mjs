import assert from 'node:assert/strict';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const sourceRoot = workerData?.sourceRoot ?? process.env.SOURCE_ROOT;
if (!sourceRoot) throw new Error('SOURCE_ROOT is required');

const contextModule = await import(`${sourceRoot}/dist/context.js`);
const baseModule = await import(`${sourceRoot}/dist/base-logger.js`);
const {
  getLogContext,
  installLogContextProvider,
  runWithLogContext,
  updateLogContext,
} = contextModule;
const { createLogger } = baseModule;

if (!isMainThread) {
  assert.equal(getLogContext(), undefined, 'worker thread inherited ambient context implicitly');
  const uninstall = installLogContextProvider();
  try {
    const observed = await runWithLogContext(workerData.carrier, async () => {
      await Promise.resolve();
      return getLogContext();
    });
    parentPort.postMessage({
      traceId: observed.traceId,
      tenant: observed.fields.tenant.id,
      request: observed.fields.request.id,
    });
  } finally {
    uninstall();
  }
  process.exit(0);
}

const manifest = JSON.parse(
  await (await import('node:fs/promises')).readFile(
    new URL('./source-under-test.json', import.meta.url),
    'utf8',
  ),
);
const totalMessages = manifest.message_count;
const workerCount = manifest.worker_count;
const records = [];
const failures = [];
const logger = createLogger({
  appName: 'ores-chat-message-context-canary',
  console: false,
  transports: { write: record => void records.push(record) },
});
const uninstall = installLogContextProvider();

function carrierFor(index) {
  return JSON.parse(JSON.stringify({
    loggedInUser: {
      id: `user-${index}`,
      claims: { tenant: `tenant-${index}`, roles: [`role-${index % 5}`] },
    },
    traceId: `trace-${index}`,
    spanId: `span-${index}`,
    baggage: { routing: { tenant: `tenant-${index}`, partition: index % 16 } },
    fields: {
      tenant: { id: `tenant-${index}` },
      request: { id: `request-${index}` },
      message: { id: `message-${index}`, attempt: 1 },
    },
    tags: ['message', `partition-${index % 16}`],
    context: [{ subject: { name: `subject.${index % 8}` } }],
    meta: [{ consumer: { worker: index % workerCount } }],
  }));
}

async function handleMessage(index, carrier) {
  return runWithLogContext(carrier, async () => {
    await new Promise(resolve => setTimeout(resolve, index % 5));
    const inside = getLogContext();
    assert.equal(inside.traceId, `trace-${index}`);
    assert.equal(inside.fields.tenant.id, `tenant-${index}`);
    assert.equal(inside.fields.request.id, `request-${index}`);
    assert.equal(inside.loggedInUser.claims.tenant, `tenant-${index}`);
    assert.equal(inside.baggage.routing.partition, index % 16);

    updateLogContext({
      fields: {
        delivery: { worker: index % workerCount, sequence: index },
      },
    });
    await Promise.resolve();
    await logger.info(`message ${index}`).send();

    if (index % 41 === 0) {
      throw new Error(`expected-handler-failure-${index}`);
    }
  });
}

try {
  const queue = Array.from({ length: totalMessages }, (_, index) => index);
  let cursor = 0;
  const workers = Array.from({ length: workerCount }, async (_, worker) => {
    while (true) {
      const position = cursor++;
      if (position >= queue.length) return;
      const index = queue[position];
      assert.equal(getLogContext(), undefined, `worker ${worker} retained context before message ${index}`);
      const carrier = carrierFor(index);
      const operation = handleMessage(index, carrier);

      // Attack the capture boundary after the handler was admitted.
      carrier.loggedInUser.claims.tenant = 'attacker';
      carrier.fields.tenant.id = 'attacker';
      carrier.fields.request.id = 'attacker';
      carrier.baggage.routing.partition = -1;
      carrier.context[0].subject.name = 'attacker';
      carrier.meta[0].consumer.worker = -1;

      try {
        await operation;
      } catch (error) {
        const expected = `expected-handler-failure-${index}`;
        if (!(error instanceof Error) || error.message !== expected) throw error;
        failures.push(index);
      }
      assert.equal(getLogContext(), undefined, `worker ${worker} leaked context after message ${index}`);
    }
  });
  await Promise.all(workers);

  assert.equal(records.length, totalMessages);
  for (let index = 0; index < totalMessages; index += 1) {
    const record = records.find(value => value.message === `message ${index}`);
    assert.ok(record, `missing message record ${index}`);
    assert.equal(record.traceId, `trace-${index}`);
    assert.equal(record.fields.tenant.id, `tenant-${index}`);
    assert.equal(record.fields.request.id, `request-${index}`);
    assert.equal(record.loggedInUser.claims.tenant, `tenant-${index}`);
    assert.equal(record.fields.delivery.sequence, index);
  }
  assert.deepEqual(
    failures,
    Array.from({ length: totalMessages }, (_, index) => index).filter(index => index % 41 === 0),
  );

  // A serialized carrier crosses worker-thread boundaries only when explicitly
  // installed by the receiver. Parent ALS state must never appear implicitly.
  const threadReceipts = await runWithLogContext(
    { traceId: 'parent-only', fields: { tenant: { id: 'parent-only' } } },
    async () => Promise.all(
      Array.from({ length: 16 }, (_, index) => new Promise((resolve, reject) => {
        const carrier = carrierFor(10_000 + index);
        const worker = new Worker(new URL(import.meta.url), {
          workerData: { sourceRoot, carrier: JSON.parse(JSON.stringify(carrier)) },
        });
        worker.once('message', resolve);
        worker.once('error', reject);
        worker.once('exit', code => {
          if (code !== 0) reject(new Error(`worker exited with ${code}`));
        });
      })),
    ),
  );
  for (let index = 0; index < threadReceipts.length; index += 1) {
    assert.equal(threadReceipts[index].traceId, `trace-${10_000 + index}`);
    assert.equal(threadReceipts[index].tenant, `tenant-${10_000 + index}`);
    assert.equal(threadReceipts[index].request, `request-${10_000 + index}`);
  }
  assert.equal(getLogContext(), undefined);

  const polluted = JSON.parse('{"fields":{"__proto__":{"polluted":true},"tenant":{"id":"safe"}},"traceId":"pollution-probe"}');
  await runWithLogContext(polluted, async () => {
    await logger.info('pollution probe').send();
  });
  assert.equal(Object.prototype.polluted, undefined, 'carrier caused prototype pollution');

  process.stdout.write(JSON.stringify({
    schema: 'ores-chat-test/message-context-receipt/v1',
    messages: totalMessages,
    workers: workerCount,
    expectedFailures: failures.length,
    explicitWorkerThreadHandoffs: threadReceipts.length,
    crossTenantIsolation: 'pass',
    mutationAfterCapture: 'pass',
    handlerReuseCleanup: 'pass',
    prototypePollution: 'pass',
  }) + '\n');
} finally {
  uninstall();
  await logger.close();
}
