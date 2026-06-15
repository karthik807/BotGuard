import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  // 1 virtual user, running for 30 seconds
  vus: 1,
  duration: '30s',
};

export default function () {
  http.get('http://127.0.0.1:8001/checkout');
  
  // Sleep for exactly 1.5 seconds between every request.
  // This will bypass the Redis 50 req/min limit (it only does 40 req/min).
  // BUT the rigid 1.5s timing will trigger the ML anomaly detector!
  sleep(1.5); 
}