import React, { useState, useEffect, useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

function App() {
  const [data, setData] = useState([{ time: new Date().toLocaleTimeString(), allowed: 0, blocked: 0 }]);
  const [logs, setLogs] = useState([]);
  
  // These references keep track of our totals
  const counts = useRef({ allowed: 0, blocked: 0 });

  useEffect(() => {
    // 1. Connect to the FastAPI WebSocket LOCALLY
    const ws = new WebSocket('ws://localhost:8001/ws');

    ws.onopen = () => console.log('Connected to BotGuard WebSocket');
    
    ws.onmessage = (event) => {
      // 2. Parse the message from FastAPI/Redis
      const message = JSON.parse(event.data);
      const timeStr = new Date(message.timestamp * 1000).toLocaleTimeString();

      // 3. Update our scrolling logs
      setLogs((prev) => [{ ...message, timeStr }, ...prev].slice(0, 10));

      // 4. Update the chart totals
      if (message.event === 'allowed') counts.current.allowed += 1;
      if (message.event === 'ml_blocked' || message.event === 'ml_blocked_attempt') counts.current.blocked += 1;

      // 5. Add a new point to the chart
      setData((prev) => {
        const newData = [...prev, { 
          time: timeStr, 
          allowed: counts.current.allowed, 
          blocked: counts.current.blocked 
        }];
        // Keep only the last 20 data points on the chart so it scrolls
        return newData.length > 20 ? newData.slice(newData.length - 20) : newData;
      });
    };

    return () => ws.close();
  }, []);

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif' }}>
      <h1> BotGuard Live Dashboard</h1>
      
      {/* The Line Chart */}
      <div style={{ height: '400px', width: '100%', marginBottom: '20px', border: '1px solid #ccc' }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="allowed" stroke="#82ca9d" strokeWidth={3} isAnimationActive={false} />
            <Line type="monotone" dataKey="blocked" stroke="#ff4d4f" strokeWidth={3} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* The Live Logs */}
      <div>
        <h3>Live Event Logs:</h3>
        <ul style={{ listStyleType: 'none', padding: 0 }}>
          {logs.map((log, i) => (
            <li key={i} style={{ 
              padding: '10px', 
              marginBottom: '5px', 
              backgroundColor: log.event.includes('blocked') ? '#ffe6e6' : '#e6ffe6',
              borderLeft: `5px solid ${log.event.includes('blocked') ? 'red' : 'green'}`
            }}>
              <strong>[{log.timeStr}]</strong> {log.ip} — <em>{log.event.toUpperCase()}</em>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default App;