import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL = 30000;
const MESSAGE_SCHEMA = {
  morph: { fields: [] },
  material: {
    fields: ['board', 'material'],
    validate: {
      board: ['A', 'C'],
      material: ['A', 'B', 'C'],
    },
  },
};

const wss = new WebSocketServer({ port: PORT });

console.log(`[ws-server] listening on port ${PORT}`);

function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', (ws, req) => {
  const addr = req.socket.remoteAddress;
  console.log(`[ws-server] connected: ${addr} (clients: ${wss.clients.size})`);

  ws.isAlive = true;
  ws.on('pong', heartbeat);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      console.warn(`[ws-server] invalid JSON from ${addr}:`, String(raw).slice(0, 100));
      return;
    }

    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) {
      console.warn(`[ws-server] invalid message format from ${addr}`);
      return;
    }

    const schema = MESSAGE_SCHEMA[msg.type];
    if (!schema) {
      console.warn(`[ws-server] unknown type from ${addr}:`, msg.type);
      return;
    }

    let valid = true;
    for (const field of schema.fields) {
      if (msg[field] === undefined) {
        console.warn(`[ws-server] missing field '${field}' from ${addr}`);
        valid = false;
        break;
      }
      if (schema.validate && schema.validate[field]) {
        if (!schema.validate[field].includes(msg[field])) {
          console.warn(`[ws-server] invalid ${field}='${msg[field]}' from ${addr}`);
          valid = false;
          break;
        }
      }
    }
    if (!valid) return;

    console.log(`[ws-server] relay: ${msg.type} from ${addr}`);

    const payload = { type: msg.type };
    for (const field of schema.fields) {
      payload[field] = msg[field];
    }
    const payloadStr = JSON.stringify(payload);
    for (const client of wss.clients) {
      if (client !== ws && client.readyState === 1) {
        client.send(payloadStr);
      }
    }
  });

  ws.on('close', () => {
    console.log(`[ws-server] disconnected: ${addr} (clients: ${wss.clients.size})`);
  });
});

const interval = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      console.log('[ws-server] terminating unresponsive client');
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL);

wss.on('close', () => {
  clearInterval(interval);
});
