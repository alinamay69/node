'use strict';

const common = require('../common');
if (!common.hasCrypto)
  common.skip('missing crypto');

const assert = require('assert');
const http2 = require('http2');
const { once } = require('events');

const { NGHTTP2_ENHANCE_YOUR_CALM } = http2.constants;

// maxSessionMemory is documented to reject new streams while the limit is
// exceeded. Make sure that also holds for requests initiated by a client
// session, so that a peer that does not read cannot make the client queue
// frames without bound. As for response headers, request bodies that are
// queued waiting for flow control are not counted, so a large upload does not
// prevent unrelated requests on the same session.

async function queuedUploadDoesNotBlockRequests() {
  // The server never opens its flow-control window, so the upload stays queued
  // in the client session.
  const server = http2.createServer({ settings: { initialWindowSize: 0 } });
  server.on('stream', common.mustCall((stream) => {
    stream.on('error', () => {});
  }, 2));
  server.listen(0);
  await once(server, 'listening');

  const client = http2.connect(`http://localhost:${server.address().port}`, {
    maxSessionMemory: 1,
  });
  client.on('error', common.mustNotCall());
  await once(client, 'remoteSettings');

  const uploadArrived = once(server, 'stream');
  const upload = client.request({ ':method': 'POST', ':path': '/upload' });
  upload.on('error', () => {});
  upload.end(Buffer.alloc(4 * 1024 * 1024));
  await uploadArrived;

  const other = client.request({ ':path': '/other' });
  other.on('error', () => {});
  await once(server, 'stream');

  other.destroy();
  upload.destroy();
  client.destroy();
  server.close();
}

async function manyRequestsAreBounded() {
  // A peer that accepts the connection and then stops reading. Before the
  // session memory check, every request was queued regardless of the limit.
  const server = http2.createServer();
  server.on('connection', (socket) => setTimeout(() => socket.pause(), 30));
  server.on('stream', (stream) => stream.on('error', () => {}));
  server.listen(0);
  await once(server, 'listening');

  const client = http2.connect(`http://localhost:${server.address().port}`, {
    maxSessionMemory: 1,
  });
  client.on('error', () => {});
  await once(client, 'connect');

  const total = 30_000;
  let rejected = 0;
  for (let i = 0; i < total; i++) {
    const req = client.request({ ':path': `/${i}` });
    req.on('error', (err) => {
      if (err.code === 'ERR_HTTP2_STREAM_ERROR' &&
          err.message.includes('NGHTTP2_ENHANCE_YOUR_CALM')) {
        rejected++;
      }
    });
    req.end();
  }
  await new Promise(setImmediate);

  assert.ok(rejected > 0, 'expected some requests to be rejected');
  // Without the check all of the requests (plus the connection preface) were
  // queued: the number of queued frames grew with the number submitted.
  assert.ok(client.state.outboundQueueSize < total,
            `queued ${client.state.outboundQueueSize} of ${total}`);
  assert.strictEqual(NGHTTP2_ENHANCE_YOUR_CALM, 11);

  client.destroy();
  server.close();
}

(async () => {
  await queuedUploadDoesNotBlockRequests();
  await manyRequestsAreBounded();
})().then(common.mustCall());
