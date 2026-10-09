import { once } from 'node:events';
import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { setImmediate } from 'node:timers/promises';
import { SpanType, TracingEventType } from '@mastra/core/observability';
import { afterAll, it, vi } from 'vitest';
import { OtelExporter } from './tracing';

// The package shares a module cache across files, including tests with mocked SDKs.
vi.hoisted(() => vi.resetModules());
afterAll(() => vi.resetModules());

it('waits for in-flight trace exports and remains usable after flushing', async () => {
  const collector = createServer();
  let exporter: OtelExporter | undefined;
  let response: ServerResponse | undefined;

  try {
    collector.listen(0, '127.0.0.1');
    await once(collector, 'listening', { signal: AbortSignal.timeout(5000) });
    const address = collector.address();
    if (!address || typeof address === 'string') throw new Error('Missing collector port');

    exporter = new OtelExporter({
      provider: {
        custom: { endpoint: `http://127.0.0.1:${address.port}`, protocol: 'http/json' },
      },
      signals: { logs: false },
      batchSize: 1,
      timeout: 5000,
    });

    for (const id of ['0000000000000001', '0000000000000002']) {
      const received = once(collector, 'request', { signal: AbortSignal.timeout(5000) }) as Promise<
        [IncomingMessage, ServerResponse]
      >;
      const [, [request, pendingResponse]] = await Promise.all([
        exporter.onTracingEvent({
          type: TracingEventType.SPAN_ENDED,
          exportedSpan: {
            id,
            traceId: '0123456789abcdef0123456789abcdef',
            name: 'in-flight span',
            type: SpanType.GENERIC,
            startTime: new Date(),
            endTime: new Date(),
            isRootSpan: true,
            isEvent: false,
          },
        }),
        received,
      ]);
      response = pendingResponse;
      request.resume();
      if (request.method !== 'POST' || request.url !== '/v1/traces') {
        throw new Error('Expected an OTLP trace export');
      }

      const flushing = exporter.flush();
      // Drain ready work while the collector still holds the transport response.
      const first = await Promise.race([flushing.then(() => 'flushed'), setImmediate('pending')]);
      if (first === 'flushed') throw new Error('flush() returned before the collector responded');

      response.writeHead(200, { 'content-type': 'application/json' });
      response.end('{}');
      await flushing;
    }
  } finally {
    if (response && !response.writableEnded) response.end('{}');
    try {
      await exporter?.shutdown();
    } finally {
      collector.closeAllConnections();
      await new Promise<void>(resolve => collector.close(() => resolve()));
    }
  }
}, 15000);
