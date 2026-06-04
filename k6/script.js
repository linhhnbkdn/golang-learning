import http from 'k6/http';
import { check } from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const JWT_SECRET = __ENV.JWT_SECRET || 'secret';

function makeToken(userId) {
  const header = encoding.b64encode(
    JSON.stringify({ alg: 'HS256', typ: 'JWT' }),
    'rawurl',
  );
  const payload = encoding.b64encode(
    JSON.stringify({ user_id: userId, exp: Math.floor(Date.now() / 1000) + 86400 }),
    'rawurl',
  );
  const data = `${header}.${payload}`;
  const rawSig = crypto.hmac('sha256', JWT_SECRET, data, 'base64');
  const sig = rawSig.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  return `${data}.${sig}`;
}

export const options = {
  vus: 1400,
  duration: '60s',
};

export default function () {
  const sessionId = `bench-${__VU}`;
  const token = makeToken(`user-${__VU}`);

  const res = http.post(
    `http://api:8000/chat/${sessionId}`,
    JSON.stringify({ content: 'xin chao' }),
    {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    },
  );

  check(res, {
    'status 200': (r) => r.status === 200,
    'has done': (r) => r.body.includes('"done":true'),
  });
}
