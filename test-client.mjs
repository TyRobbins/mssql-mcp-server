import { MCPClient } from "@modelcontextprotocol/sdk";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/dist/esm/client/sse.js";

// Configure client
const SERVER_URL = 'http://localhost:3333';
const SSE_ENDPOINT = `${SERVER_URL}/sse`;

console.log('Starting MCP Client test...');

async function main() {
  try {
    // Create SSE transport
    console.log(`Connecting to SSE endpoint: ${SSE_ENDPOINT}`);
    const transport = new SSEClientTransport(new URL(SSE_ENDPOINT));
    
    // Handle connection errors
    transport.onerror = (error) => {
      console.error('❌ Transport error:', error);
    };

    // Create MCP client
    const client = new MCPClient(transport);
    
    // Connect to server
    console.log('Connecting to MCP server...');
    await client.connect();
    console.log('✅ Connected successfully');
    
    // Wait a bit to ensure connection is stable
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Call SQL tool
    console.log('Executing SQL query...');
    try {
      const result = await client.callTool('mcp_execute_query', { // Updated name
        sql: 'SELECT TOP 5 name FROM sys.tables'
      });
      
      console.log('✅ Query executed successfully:');
      console.log(JSON.stringify(result, null, 2));
    } catch (toolError) {
      console.error('❌ Tool call failed:', toolError);
    }
    
    // Close the connection
    console.log('Closing connection...');
    await client.close();
    console.log('✅ Connection closed');
    
  } catch (error) {
    console.error('❌ Error:', error);
  }
}

// Run the main function
main().catch(console.error); 