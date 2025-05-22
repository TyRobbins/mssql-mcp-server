import fetch from 'node-fetch';
import * as EventSourceModule from 'eventsource';
const EventSource = EventSourceModule.default || EventSourceModule;

const SERVER_URL = 'http://localhost:3333';
const SSE_ENDPOINT = `${SERVER_URL}/sse`;

// Setup variables
let sessionId = null;
let messagesEndpoint = null;

console.log('Starting SSE connection test...');

// 1. Connect to SSE endpoint
const eventSource = new EventSource(SSE_ENDPOINT);

// Handle SSE connection
eventSource.onopen = () => {
  console.log('✅ SSE connection established');
};

// Handle errors
eventSource.onerror = (error) => {
  console.error('❌ SSE connection error:', error);
};

// Handle the endpoint event (receives the messages endpoint)
eventSource.addEventListener('endpoint', (event) => {
  try {
    messagesEndpoint = decodeURI(event.data);
    console.log(`✅ Received messages endpoint: ${messagesEndpoint}`);
    
    // Extract session ID from the URL
    const url = new URL(messagesEndpoint, SERVER_URL);
    sessionId = url.searchParams.get('sessionId');
    console.log(`✅ Session ID: ${sessionId}`);
    
    // Wait a bit then send a test query
    setTimeout(sendTestQuery, 2000);
  } catch (err) {
    console.error('❌ Error processing endpoint event:', err);
  }
});

// Listen for all messages from the server
eventSource.addEventListener('message', (event) => {
  try {
    console.log('📥 Received SSE message:');
    const data = JSON.parse(event.data);
    console.log(JSON.stringify(data, null, 2));
    
    // Check if this is a response to our tool call
    if (data.id === '1' && data.result) {
      console.log('✅ Received response to our tool call!');
      
      // Close the connection after receiving the response
      setTimeout(() => {
        console.log('Closing connection...');
        eventSource.close();
        process.exit(0);
      }, 2000);
    }
  } catch (err) {
    console.error('❌ Error processing message event:', err.message);
    console.log('Raw message data:', event.data);
  }
});

// Function to send a test query
async function sendTestQuery() {
  try {
    console.log('📤 Sending test SQL query...');
    
    const response = await fetch(messagesEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '1',
        method: 'tools/call',
        params: {
          name: 'mcp_execute_query', // Updated name
          parameters: {
            sql: 'SELECT TOP 5 name FROM sys.tables'
          }
        }
      })
    });
    
    const responseText = await response.text();
    console.log(`✅ Query sent, server responded with status ${response.status}:`, responseText);
    
    if (response.status !== 202) {
      console.error('❌ Expected status 202 (Accepted)');
    }
  } catch (err) {
    console.error('❌ Error sending test query:', err.message);
  }
}

// Exit handler
process.on('SIGINT', () => {
  console.log('Closing SSE connection...');
  eventSource.close();
  process.exit();
}); 