import fetch from 'node-fetch';

const SERVER_URL = 'http://localhost:3333';
const MESSAGES_ENDPOINT = `${SERVER_URL}/messages`;

async function main() {
  console.log('Sending SQL query directly to the messages endpoint...');
  
  try {
    const response = await fetch(MESSAGES_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '123',
        method: 'tools/call',
        params: {
          name: 'mcp_execute_query', // Updated name
          parameters: {
            sql: 'SELECT TOP 5 name FROM sys.tables'
          }
        }
      })
    });
    
    const responseStatus = response.status;
    const responseText = await response.text();
    
    console.log(`Response status: ${responseStatus}`);
    console.log(`Response text: ${responseText}`);
    
    if (responseStatus === 202) {
      console.log('Request accepted. Response should be delivered over SSE connection.');
    } else {
      console.log('Unexpected response. Check server configuration.');
    }
  } catch (err) {
    console.error('Error:', err.message);
  }
}

main().catch(console.error); 