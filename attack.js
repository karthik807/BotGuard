import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: 1000,          // 1,000 requests per second
      timeUnit: '1s',
      duration: '30s',     // Run for 30 seconds
      preAllocatedVUs: 50, // 50 "bots" making the requests
      maxVUs: 200,
    },
  },
};

export default function () {
  // Hit your local FastAPI endpoint
  const res = http.get('http://127.0.0.1:8000/checkout');
  
  // We don't care if it's 200 OK or 429 Blocked, we just want to stress test
  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });
}