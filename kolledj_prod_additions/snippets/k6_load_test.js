import http from 'k6/http';
import { sleep, check } from 'k6';

export let options = {
  stages: [
    { duration: '30s', target: 200 },
    { duration: '60s', target: 1000 },
    { duration: '60s', target: 1000 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(90)<300', 'p(99)<1000'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const base = __ENV.BASE_URL || 'http://localhost';
  const res = http.get(`${base}/api/healthz`);
  check(res, { 'status is 200': (r) => r.status === 200 });
  sleep(1);
}
