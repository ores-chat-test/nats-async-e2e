import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { connect, headers, StringCodec } from 'nats';

const sourceRoot = process.env.SOURCE_ROOT;
if (!sourceRoot) throw new Error('SOURCE_ROOT is required');
const server = process.env.NATS_URL ?? 'nats://127.0.0.1:4222';
const manifest = JSON.parse(await readFile(new URL('./source-under-test.json', import.meta.url), 'utf8'));
const total = Math.min(manifest.message_count, 512);
const subject = `async.context.${Date.now()}.${process.pid}`;

const contextModule = await import(`${sourceRoot}/dist/context.js`);
const baseModule = await import(`${sourceRoot}/dist/base-logger.js`);
const {
  getLogContext,
  installLogContextProvider,
  runWithLogContext,
  updateLogContext,
} = contextModule;
const { createLogger } = baseModule;

const codec = StringCodec();
const records = [];
const logger = createLogger({
  appName: 'ores-chat-real-nats-context-canary',
  console: false,
  transports: { write: record => void records.push(record) },
});
const uninstall = installLogContextProvider();
const nc = await connect({ servers: server, timeout: 5_000, maxReconnectAttempts: 2 });

function makeCarrier(index) {
  return {
    loggedInUser: { id: `nats-user-${index}`, claims: { tenant: `nats-tenant-${index}` } },
    traceId: index.toString(16).padStart(32, '0'),
    spanId: index.toString(16).padStart(16, '0'),
    traceFlags: index % 2,
    baggage: { routing: { tenant: `nats-tenant-${index}`, partition: index % 32 } },
    fields: {
      tenant: { id: `nats-tenant-${index}` },
      request: { id: `nats-request-${index}` },
      message: { id: `nats-message-${index}`, subject },
    },
    tags: ['nats', `partition-${index % 32}`],
  };
}

function parseCarrier(raw) {
  if (typeof raw !== 'string' || raw.length === 0 || raw.length > 8_192) {
    throw new Error('invalid context carrier length');
  }
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('invalid context carrier shape');
  }
  if (typeof parsed.traceId !== 'string' || !/^[0-9a-f]{32}$/.test(parsed.traceId)) {
    throw new Error('invalid trace id');
  }
  if (typeof parsed.spanId !== 'string' || !/^[0-9a-f]{16}$/.test(parsed.spanId)) {
    throw new Error('invalid span id');
  }
  if (!parsed.fields?.tenant?.id || !parsed.fields?.request?.id) {
    throw new Error('missing tenant or request identity');
  }
  return parsed;
}

try {
  assert.throws(() => parseCarrier(''), /length/);
  assert.throws(() => parseCarrier('x'.repeat(8_193)), /length/);
  assert.throws(() => parseCarrier('{}'), /trace id/);
  assert.throws(
    () => parseCarrier(JSON.stringify({ ...makeCarrier(1), traceId: 'not-a-trace' })),
    /trace id/,
  );

  const sub = nc.subscribe(subject);
  const handlerResults = [];
  const consumption = (async () => {
    let seen = 0;
    for await (const message of sub) {
      const envelope = JSON.parse(codec.decode(message.data));
      const carrier = parseCarrier(message.headers?.get('x-ores-context'));
      const index = envelope.index;
      const operation = runWithLogContext(carrier, async () => {
        await new Promise(resolve => setTimeout(resolve, index % 11));
        const inside = getLogContext();
        assert.equal(inside.fields.tenant.id, `nats-tenant-${index}`);
        assert.equal(inside.fields.request.id, `nats-request-${index}`);
        assert.equal(inside.loggedInUser.claims.tenant, `nats-tenant-${index}`);
        assert.equal(inside.baggage.routing.partition, index % 32);
        updateLogContext({ fields: { delivery: { sequence: index, redelivered: false } } });
        await Promise.resolve();
        await logger.info(`nats ${index}`).send();
        if (index % 53 === 0) throw new Error(`expected-nats-handler-failure-${index}`);
      });

      carrier.loggedInUser.claims.tenant = 'attacker';
      carrier.fields.tenant.id = 'attacker';
      carrier.fields.request.id = 'attacker';
      carrier.baggage.routing.partition = -1;

      handlerResults.push(
        operation.then(
          () => ({ index, state: 'ok' }),
          error => {
            assert.equal(error.message, `expected-nats-handler-failure-${index}`);
            return { index, state: 'expected-failure' };
          },
        ),
      );
      seen += 1;
      assert.equal(getLogContext(), undefined, `subscriber loop retained message ${index} context`);
      if (seen === total) break;
    }
  })();

  for (let index = 0; index < total; index += 1) {
    const carrier = makeCarrier(index);
    const h = headers();
    h.set('x-ores-context', JSON.stringify(carrier));
    h.set('traceparent', `00-${carrier.traceId}-${carrier.spanId}-${carrier.traceFlags ? '01' : '00'}`);
    h.set('x-tenant-id', carrier.fields.tenant.id);
    nc.publish(subject, codec.encode(JSON.stringify({ index })), { headers: h });
  }
  await nc.flush();
  await consumption;
  const outcomes = await Promise.all(handlerResults);

  assert.equal(outcomes.length, total);
  assert.equal(records.length, total);
  for (let index = 0; index < total; index += 1) {
    const record = records.find(value => value.message === `nats ${index}`);
    assert.ok(record, `missing NATS record ${index}`);
    assert.equal(record.traceId, index.toString(16).padStart(32, '0'));
    assert.equal(record.fields.tenant.id, `nats-tenant-${index}`);
    assert.equal(record.fields.request.id, `nats-request-${index}`);
    assert.equal(record.loggedInUser.claims.tenant, `nats-tenant-${index}`);
    assert.equal(record.fields.delivery.sequence, index);
  }
  assert.equal(getLogContext(), undefined);

  process.stdout.write(JSON.stringify({
    schema: 'ores-chat-test/real-nats-context-receipt/v1',
    server,
    subject,
    messages: total,
    expectedFailures: outcomes.filter(value => value.state === 'expected-failure').length,
    invalidCarriersRejected: 4,
    mutationAfterCapture: 'pass',
    subscriberReuseCleanup: 'pass',
    crossTenantIsolation: 'pass',
    traceCarrierContinuity: 'pass',
  }) + '\n');
} finally {
  sub?.unsubscribe();
  await nc.drain().catch(() => undefined);
  uninstall();
  await logger.close();
}
