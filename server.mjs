// server.js - Main MCP Server Implementation
import dotenv from 'dotenv';
import express from 'express';
import bodyParser from 'body-parser';
import sql from 'mssql';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { z } from 'zod';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import crypto from 'crypto';
import cors from 'cors';
import helmet from 'helmet';
import { rateLimit } from 'express-rate-limit';

// Import database utilities
import { initializeDbPool, executeQuery, getDbConfig } from './Lib/database.mjs';

// Import tool implementations
import { registerDatabaseTools } from './Lib/tools.mjs';

// Import resource implementations
import { registerDatabaseResources } from './Lib/resources.mjs';

// Import prompt implementations
import { registerPrompts } from './Lib/prompts.mjs';

// Import utilities
import { logger } from './Lib/logger.mjs';
import { getReadableErrorMessage, createJsonRpcError } from './Lib/errors.mjs';

// Load environment variables
dotenv.config();

// Get the directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const PORT = process.env.PORT || 3333;
const TRANSPORT = process.env.TRANSPORT || 'stdio';
const HOST = process.env.HOST || '0.0.0.0';
const QUERY_RESULTS_PATH = process.env.QUERY_RESULTS_PATH || path.join(__dirname, 'query_results');
const PING_INTERVAL = process.env.PING_INTERVAL || 60000; // Ping every 60 seconds by default

// Create results directory if it doesn't exist
if (!fs.existsSync(QUERY_RESULTS_PATH)) {
    fs.mkdirSync(QUERY_RESULTS_PATH, { recursive: true });
    logger.info(`Created results directory: ${QUERY_RESULTS_PATH}`);
}

// Create Express app to handle HTTP requests for SSE transport
const app = express();
const httpServer = http.createServer(app);

// Security middleware
app.use(helmet({ contentSecurityPolicy: false })); // Modified helmet config for SSE
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

// Rate limiting
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 100, // Limit each IP to 100 requests per minute
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        status: 429,
        message: 'Too many requests, please try again later.'
    }
});
app.use(limiter);

// Logging middleware
app.use((req, res, next) => {
    logger.info(`${req.method} ${req.url}`);
    next();
});

// Error handling middleware
app.use((err, req, res, next) => {
    logger.error(`Express error: ${err.message}`);
    res.status(500).json({
        jsonrpc: "2.0",
        error: createJsonRpcError(-32603, `Internal error: ${err.message}`)
    });
});

// Create MCP server instance
const server = new McpServer({
    name: "MSSQL-MCP-Server",
    version: "1.1.0",
    capabilities: {
        resources: {
            listChanged: true
        },
        tools: {
            listChanged: true
        },
        prompts: {
            listChanged: true
        }
    }
});

// Make sure server._tools exists
if (!server._tools) {
    server._tools = {};
}

// Add a helper method to the server to execute tools directly
server.executeToolCall = async function (toolName, args) {
    // Find the tool in the registered tools
    logger.info(`Looking for tool: ${toolName}`);
    const tool = this._tools ? this._tools[toolName] : null;

    if (!tool) {
        const availableTools = Object.keys(this._tools || {}).join(', ');
        logger.error(`Tool ${toolName} not found. Available tools: ${availableTools}`);
        throw new Error(`Tool ${toolName} not found. Available tools: ${availableTools.length > 100 ? availableTools.substring(0, 100) + '...' : availableTools}`);
    }

    try {
        logger.info(`Executing tool ${toolName} directly with args: ${JSON.stringify(args)}`);
        const result = await tool.handler(args);
        logger.info(`Tool ${toolName} executed successfully`);
        return result;
    } catch (err) {
        logger.error(`Error executing tool ${toolName}: ${err.message}`);
        throw err;
    }
};

// IMPORTANT: Register database tools BEFORE setting up HTTP routes
try {
    // Register database tools (execute-query, table-details, etc.)
    logger.info("Registering database tools...");
    registerDatabaseTools(server);

    // Register database resources (tables, schema, views, etc.)
    logger.info("Registering database resources...");
    registerDatabaseResources(server);

    // Register prompts (generate-query, etc.)
    logger.info("Registering prompts...");
    registerPrompts(server);

    // Debug log for tools after all registrations
    const registeredTools = Object.keys(server._tools || {});
    logger.info(`Registered tools (${registeredTools.length}): ${registeredTools.join(', ')}`);
    // console.log("DEBUG: Final registered tools:", registeredTools); // Optional: for more verbose debugging
} catch (error) {
    logger.error(`Failed to register MCP components: ${error.message}`);
    logger.error(error.stack);
}

// Transport variables
let currentTransport = null;
let activeConnections = new Set();
let pingIntervalId = null;

// Add HTTP server status endpoint
app.get('/', (req, res) => {
    const dbConfig = getDbConfig(true); // Get sanitized config (no password)

    res.status(200).json({
        status: 'ok',
        message: 'MCP Server is running',
        transport: TRANSPORT,
        endpoints: {
            sse: '/sse',
            messages: '/messages',
            diagnostics: '/diagnostic',
            query_results: {
                list: '/query-results',
                detail: '/query-results/:uuid'
            }
        },
        connection_info: {
            ping_interval_ms: PING_INTERVAL,
            active_connections: activeConnections.size
        },
        database_info: {
            server: dbConfig.server,
            database: dbConfig.database,
            user: dbConfig.user
        },
        version: server.options?.version || "1.1.0"
    });
});

// Add an endpoint to list all tools
app.get('/tools', (req, res) => {
    try {
        // Access tools directly from the server instance
        const tools = server._tools || {};

        const toolList = Object.keys(tools).map(name => {
            return {
                name,
                schema: tools[name].schema,
                source: 'internal'
            };
        });

        logger.info(`Tool listing requested. Found ${toolList.length} tools.`);
        logger.info(`Tools from internal: ${Object.keys(tools).join(', ')}`);

        res.status(200).json({
            count: toolList.length,
            tools: toolList,
            debug: {
                internalToolKeys: Object.keys(tools)
            }
        });
    } catch (error) {
        logger.error(`Error listing tools: ${error.message}`);
        res.status(500).json({
            error: `Failed to list tools: ${error.message}`,
            stack: error.stack
        });
    }
});

// Diagnostic endpoint 
app.get('/diagnostic', async (req, res) => {
    try {
        const dbConfig = getDbConfig(true); // Get sanitized config (no password)

        const diagnosticInfo = {
            status: 'ok',
            server: {
                version: process.version,
                platform: process.platform,
                arch: process.arch,
                uptime: process.uptime()
            },
            mcp: {
                transport: TRANSPORT,
                activeConnections: activeConnections.size,
                hasCurrentTransport: currentTransport !== null,
                version: server.options?.version || "1.1.0",
                pingIntervalMs: PING_INTERVAL,
                pingActive: pingIntervalId !== null
            },
            database: {
                server: dbConfig.server,
                database: dbConfig.database,
                user: dbConfig.user,
                port: dbConfig.port
            },
            endpoints: {
                sse: `${req.protocol}://${req.get('host')}/sse`,
                messages: `${req.protocol}://${req.get('host')}/messages`,
                queryResults: `${req.protocol}://${req.get('host')}/query-results`
            }
        };

        // Test database connection
        try {
            await executeQuery('SELECT 1 AS TestConnection');
            diagnosticInfo.database.connectionTest = 'successful';
        } catch (err) {
            diagnosticInfo.database.connectionTest = 'failed';
            diagnosticInfo.database.connectionError = err.message;
        }

        res.status(200).json(diagnosticInfo);
    } catch (error) {
        logger.error(`Diagnostic error: ${error.message}`);
        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

// Direct cursor guide endpoint
app.get('/cursor-guide', (req, res) => {
    // Comprehensive guide for cursor-based pagination
    const guideText = `
# SQL Cursor-Based Pagination Guide

Cursor-based pagination is an efficient approach for paginating through large datasets, especially when:
- You need stable pagination through frequently changing data
- You're handling very large datasets where OFFSET/LIMIT becomes inefficient
- You want better performance for deep pagination

## Key Concepts

1. **Cursor**: A pointer to a specific item in a dataset, typically based on a unique, indexed field
2. **Direction**: You can paginate forward (next) or backward (previous)
3. **Page Size**: The number of items to return per request

## Example Usage

Using cursor-based pagination with our SQL tools:

\`\`\`javascript
// First page (no cursor)
const firstPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at"
});

// Next page (using cursor from previous response)
const nextPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: firstPage.result.pagination.nextCursor,
  direction: "next"
});

// Previous page (going back)
const prevPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: nextPage.result.pagination.prevCursor,
  direction: "prev"
});
\`\`\`

## Best Practices

1. **Choose an appropriate cursor field**:
   - Should be unique or nearly unique (ideally indexed)
   - Common choices: timestamps, auto-incrementing IDs
   - Compound cursors can be used for non-unique fields (e.g., "timestamp:id")

2. **Order matters**:
   - Always include an ORDER BY clause that includes your cursor field
   - Consistent ordering is essential (always ASC or always DESC)

3. **Handle edge cases**:
   - First/last page detection
   - Empty result sets
   - Missing or invalid cursors

4. **Performance considerations**:
   - Use indexed fields for cursors
   - Avoid expensive joins in paginated queries
   - Consider caching results for frequently accessed pages
`;

    // Send both JSON and plain text formats
    if (req.headers.accept && req.headers.accept.includes('application/json')) {
        res.status(200).json({
            jsonrpc: "2.0",
            result: {
                content: [{
                    type: "text",
                    text: guideText
                }]
            }
        });
    } else {
        res.status(200).type('text/markdown').send(guideText);
    }
});

// SSE endpoint for client to connect
app.get('/sse', async (req, res) => {
    logger.info('New SSE connection request received');

    // Set headers for SSE
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // Prevents buffering in Nginx

    try {
        // Create new SSE transport for this connection
        const messagesEndpoint = `/messages`;
        logger.info(`Creating SSE transport with messages endpoint: ${messagesEndpoint}`);

        // Create the transport
        currentTransport = new SSEServerTransport(messagesEndpoint, res);

        // Set up message handlers before connecting
        currentTransport.onmessage = function (message) {
            logger.info(`Transport received message: ${JSON.stringify(message)}`);
        };

        // Error handler
        currentTransport.onerror = function (error) {
            logger.error(`Transport error: ${error}`);
        };

        // Close handler
        currentTransport.onclose = function () {
            logger.info(`Transport closed`);
        };

        // Connect the server to this transport
        await server.connect(currentTransport);

        logger.info('SSE transport connected successfully');

        // Add this connection to tracking
        activeConnections.add(res);
        logger.info(`Active SSE connections: ${activeConnections.size}`);

        // Clear any existing ping interval
        if (pingIntervalId) {
            clearInterval(pingIntervalId);
        }

        // Set up ping interval to keep connection alive
        pingIntervalId = setInterval(() => {
            if (res && !res.finished) {
                logger.debug('Sending ping to client');
                res.write('event: ping\n');
                res.write(`data: ${Date.now()}\n\n`);
            } else {
                // Connection is closed, clear interval
                clearInterval(pingIntervalId);
                pingIntervalId = null;
            }
        }, PING_INTERVAL);

        // Handle client disconnect
        req.on('close', () => {
            logger.info('SSE client disconnected');
            activeConnections.delete(res);
            currentTransport = null;

            // Clear ping interval when client disconnects
            if (pingIntervalId) {
                clearInterval(pingIntervalId);
                pingIntervalId = null;
            }

            logger.info(`Active SSE connections: ${activeConnections.size}`);
        });

        // Send a welcome message after connection is established
        setTimeout(async () => {
            try {
                if (!currentTransport) return;

                // Create a simple welcome notification
                const welcomeMessage = {
                    jsonrpc: "2.0",
                    method: "notification",
                    params: {
                        type: "info",
                        message: `# Welcome to MSSQL MCP Server v${server.options?.version || "1.1.0"} 🚀\n\n` +
                            `To explore the database, use a command like:\n\n` +
                            `\`\`\`javascript\n` +
                            `mcp_discover_database_overview()\n` + // Updated tool name
                            `\`\`\``
                    }
                };

                currentTransport.send(welcomeMessage);
                logger.info('Welcome message sent');

                // Try to get a sample table for additional guidance
                try {
                    const tablesResult = await executeQuery(`
                        SELECT TOP 1
                            TABLE_NAME 
                        FROM 
                            INFORMATION_SCHEMA.TABLES 
                        WHERE 
                            TABLE_TYPE = 'BASE TABLE' 
                        ORDER BY 
                            TABLE_NAME
                    `);

                    if (tablesResult.recordset?.length > 0) {
                        const sampleTable = tablesResult.recordset[0].TABLE_NAME;

                        // Send additional examples
                        const examplesMessage = {
                            jsonrpc: "2.0",
                            method: "notification",
                            params: {
                                type: "info",
                                message: `## Example Commands\n\n` +
                                    `Get table details:\n` +
                                    `\`\`\`javascript\n` +
                                    `mcp_table_details({ tableName: "${sampleTable}" }) // Assumes schema.table if not specified, or just table if in default schema \n` +
                                    `\`\`\`\n\n` +
                                    `Execute a query:\n` +
                                    `\`\`\`javascript\n` +
                                    `mcp_execute_query({ sql: "SELECT TOP 10 * FROM ${sampleTable}" })\n` +
                                    `\`\`\``
                            }
                        };

                        currentTransport.send(examplesMessage);
                    }
                } catch (dbErr) {
                    logger.warn(`Database query failed in welcome message: ${dbErr.message}`);
                    // Continue without table example
                }
            } catch (err) {
                logger.error(`Error sending welcome message: ${err.message}`);
                // Don't terminate connection on welcome message error
            }
        }, 1000);
    } catch (error) {
        logger.error(`Failed to set up SSE transport: ${error.message}`);
        res.status(500).end(`Error: ${error.message}`);
    }
});

// Messages endpoint for client to send messages
app.post('/messages', (req, res) => {
    logger.info('Received message from client');

    if (!currentTransport) {
        logger.error('No SSE transport available to process message');
        return res.status(503).json({
            jsonrpc: "2.0",
            id: req.body.id || null,
            error: {
                code: -32000,
                message: "Server transport not initialized. Connect to /sse endpoint first."
            }
        });
    }

    try {
        // Extract the request ID for better debugging
        const requestId = req.body.id || "unknown";
        const method = req.body.method || "unknown";

        logger.info(`Processing message ID: ${requestId}, method: ${method}`);
        logger.info(`Request body: ${JSON.stringify(req.body)}`);

        // Special handling for pagination guide tool
        if (method === 'tools/call' &&
            (req.body.params?.name === 'mcp_pagination_guide' || // Updated name
                req.body.params?.name === 'pagination_guide')) { // Alias for direct call

            logger.info('Direct handling for pagination guide tool');

            // This guide text should ideally be fetched from the tool's definition
            // or a shared constant to avoid duplication. For now, it's replicated.
            // This is the updated guide text from Lib/tools.mjs's mcp_pagination_guide.
            const guideText = `
# SQL Cursor-Based Pagination Guide

Cursor-based pagination is an efficient approach for paginating through large datasets, especially when:
- You need stable pagination through frequently changing data.
- You're handling very large datasets where OFFSET/LIMIT becomes inefficient.
- You want better performance for deep pagination.

## Key Concepts

1.  **Cursor**: A pointer to a specific item in a dataset, typically based on a unique, indexed field (or combination of fields).
2.  **Direction**: You can paginate forward ('next') or backward ('prev').
3.  **Page Size**: The number of items to return per request.
4.  **Cursor Field**: The field(s) used for ordering and creating the cursor. Must be consistently ordered.

## Example Usage with \`mcp_paginated_query\`

\`\`\`javascript
// First page (no cursor provided)
const firstPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC", // Consistent ORDER BY
  pageSize: 20,
  cursorField: "created_at,id" // Compound cursor field if created_at is not unique
});

// Response from firstPage might include:
// firstPage.result.metadata.pagination.nextCursor = "some_encoded_cursor_value"

// Next page (using cursor from previous response)
if (firstPage.result.metadata.pagination.nextCursor) {
  const nextPage = await tool.call("mcp_paginated_query", {
    sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC",
    pageSize: 20,
    cursorField: "created_at,id",
    cursor: firstPage.result.metadata.pagination.nextCursor,
    direction: "next" // Default is 'next', can be explicit
  });
}

// Previous page (going back from nextPage)
// nextPage.result.metadata.pagination.prevCursor = "another_encoded_cursor_value"
if (nextPage.result.metadata.pagination.prevCursor) {
  const prevPage = await tool.call("mcp_paginated_query", {
    sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC",
    pageSize: 20,
    cursorField: "created_at,id",
    cursor: nextPage.result.metadata.pagination.prevCursor,
    direction: "prev"
  });
}
\`\`\`

## Best Practices

1.  **Choose an appropriate \`cursorField\`**:
    *   Should be unique or a combination of fields that guarantees uniqueness (e.g., "timestamp,id").
    *   The field(s) must be indexed for performance.
    *   Common choices: creation/update timestamps, auto-incrementing IDs.

2.  **Consistent \`ORDER BY\` Clause**:
    *   The \`ORDER BY\` clause in your SQL query **must** match the \`cursorField\` and its order.
    *   For descending order on a field, the pagination logic will handle it correctly.
    *   If \`cursorField\` is compound (e.g., "field1,field2"), the \`ORDER BY\` should be \`ORDER BY field1, field2\`.

3.  **Use \`mcp_paginated_query\`**: This tool is specifically designed for robust cursor-based pagination.

4.  **Subqueries**: For complex queries, consider applying pagination to a simplified subquery that provides the ordered keys/cursor fields, then join the full data.
`;
            const result = {
                content: [{ type: "text", text: guideText }],
                result: { guide: "SQL pagination guide provided successfully." } // Matching result structure
            };

            if (currentTransport && currentTransport.res && !currentTransport.res.finished) {
                const sseResponse = { jsonrpc: "2.0", id: requestId, result };
                currentTransport.res.write(`event: message\n`);
                currentTransport.res.write(`data: ${JSON.stringify(sseResponse)}\n\n`);
                res.status(200).json({ success: true, message: "Response sent via SSE." });
            } else {
                res.status(200).json({ jsonrpc: "2.0", id: requestId, result });
            }
            return;
        }

        // General tool call handling
        if (method === 'tools/call') {
            const toolName = req.body.params?.name;
            const toolArgs = req.body.params?.arguments || {};

            logger.info(`Direct handling for tool call: ${toolName}`);

            // Standardized tool names are expected to be `mcp_verb_noun`.
            // This logic attempts to find the tool if the client provides the name
            // with or without the `mcp_` prefix.
            let foundToolName = null;
            if (server._tools && server._tools[toolName]) {
                foundToolName = toolName;
            } else if (!toolName.startsWith('mcp_') && server._tools && server._tools[`mcp_${toolName}`]) {
                foundToolName = `mcp_${toolName}`;
            }

            if (foundToolName) {
                logger.info(`Found tool handler for: ${foundToolName}`);
                server.executeToolCall(foundToolName, toolArgs) // Execute the tool
                    .then(result => {
                        logger.info(`Direct tool result obtained successfully`);

                        // Send result via SSE transport
                        if (currentTransport && currentTransport.res && !currentTransport.res.finished) {
                            // Proper JSON-RPC formatting
                            const sseResponse = {
                                jsonrpc: "2.0",
                                id: requestId,
                                result: result
                            };

                            // Write directly to the SSE connection with event: message format
                            currentTransport.res.write(`event: message\n`);
                            currentTransport.res.write(`data: ${JSON.stringify(sseResponse)}\n\n`);

                            // Respond to HTTP request
                            res.status(200).json({ success: true });
                        } else {
                            // Fallback to HTTP response if SSE not available
                            res.status(200).json({
                                jsonrpc: "2.0",
                                id: requestId,
                                result: result
                            });
                        }
                    })
                    .catch(err => {
                        logger.error(`Error executing tool directly: ${err.message}`);

                        // Send error via SSE
                        if (currentTransport && currentTransport.res && !currentTransport.res.finished) {
                            const errorResponse = {
                                jsonrpc: "2.0",
                                id: requestId,
                                error: {
                                    code: -32603,
                                    message: `Error executing tool: ${err.message}`
                                }
                            };

                            currentTransport.res.write(`event: message\n`);
                            currentTransport.res.write(`data: ${JSON.stringify(errorResponse)}\n\n`);

                            res.status(200).json({ success: true });
                        } else {
                            res.status(500).json({
                                jsonrpc: "2.0",
                                id: requestId,
                                error: {
                                    code: -32603,
                                    message: `Error executing tool: ${err.message}`
                                }
                            });
                        }
                    });

                // Return early - response will be sent by the promise
                return;
            } else {
                logger.error(`Tool not found with any name variant: ${toolName}`);
                logger.error(`Available tools: ${Object.keys(server._tools || {}).join(', ')}`);

                // Send error via SSE
                if (currentTransport && currentTransport.res && !currentTransport.res.finished) {
                    const errorResponse = {
                        jsonrpc: "2.0",
                        id: requestId,
                        error: {
                            code: -32601,
                            message: `Tool not found: ${toolName}`
                        }
                    };

                    currentTransport.res.write(`event: message\n`);
                    currentTransport.res.write(`data: ${JSON.stringify(errorResponse)}\n\n`);

                    res.status(200).json({ success: true });
                } else {
                    return res.status(404).json({
                        jsonrpc: "2.0",
                        id: requestId,
                        error: {
                            code: -32601,
                            message: `Tool not found: ${toolName}`
                        }
                    });
                }
                return;
            }
        }

        // Monkey-patch SSEServerTransport.send to ensure JSON-RPC responses are consistently formatted
        // with "event: message" and "data: {...}\n\n". This is crucial for compatibility with
        // clients that strictly expect this SSE message structure for MCP responses,
        // potentially due to interpretations of early MCP SSE guidelines or specific client libraries.
        // If the underlying SDK's SSEServerTransport.send method natively supports this format
        // reliably in the future, this patch may be re-evaluated.
        if (currentTransport && typeof currentTransport.send === 'function' && !currentTransport.isPatched) {
            const originalSend = currentTransport.send;
            currentTransport.send = function (message) {
                // logger.info(`Intercepting SSE transport send: ${JSON.stringify(message)}`); // Can be verbose

                // For JSON-RPC responses (identified by having an id and either result or error),
                // write directly to the SSE stream in the "event: message\ndata: {...}\n\n" format.
                if (message && message.jsonrpc === "2.0" && message.id && (message.hasOwnProperty('result') || message.hasOwnProperty('error'))) {
                    if (this.res && !this.res.finished) {
                        this.res.write(`event: message\n`);
                        this.res.write(`data: ${JSON.stringify(message)}\n\n`);
                        logger.info(`Sent JSON-RPC response via patched SSE send for ID: ${message.id}`);
                        return;
                    } else {
                        logger.warn(`SSE transport response stream closed or unavailable for message ID: ${message.id}`);
                    }
                } else {
                    // For other types of messages (e.g., notifications without ID, or non-JSON-RPC),
                    // fall back to the original send behavior.
                    logger.info(`Using original SSE send for non-standard JSON-RPC response or notification.`);
                    return originalSend.call(this, message);
                }
            };
            currentTransport.isPatched = true; // Mark as patched to avoid re-patching
        }

        // For standard message handling (non-tool calls or tools we couldn't handle directly)
        // Let the SSEServerTransport handle it. If patched, our custom send logic will be used for responses.
        currentTransport.handlePostMessage(req, res, req.body);
        logger.info(`Message delegated to SSE transport for request ID: ${requestId}`);

    } catch (error) {
        logger.error(`Error processing message: ${error.message}`);

        // Send error via SSE if possible
        if (currentTransport && currentTransport.res && !currentTransport.res.finished) {
            const errorResponse = {
                jsonrpc: "2.0",
                id: req.body.id || null,
                error: {
                    code: -32603,
                    message: "Internal server error: " + error.message
                }
            };

            currentTransport.res.write(`event: message\n`);
            currentTransport.res.write(`data: ${JSON.stringify(errorResponse)}\n\n`);

            res.status(200).json({ success: true });
        } else {
            return res.status(500).json({
                jsonrpc: "2.0",
                id: req.body.id || null,
                error: {
                    code: -32603,
                    message: "Internal server error: " + error.message
                }
            });
        }
    }
});

// Add HTTP endpoints to list and retrieve saved query results
app.get('/query-results', (req, res) => {
    try {
        if (!fs.existsSync(QUERY_RESULTS_PATH)) {
            return res.status(200).json({ results: [] });
        }

        // Read all JSON files in the results directory
        const files = fs.readdirSync(QUERY_RESULTS_PATH)
            .filter(file => file.endsWith('.json'))
            .map(file => {
                try {
                    const filepath = path.join(QUERY_RESULTS_PATH, file);
                    const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
                    return {
                        uuid: data.metadata.uuid,
                        timestamp: data.metadata.timestamp,
                        query: data.metadata.query,
                        rowCount: data.metadata.rowCount,
                        filename: file
                    };
                } catch (err) {
                    logger.error(`Error reading file ${file}: ${err.message}`);
                    return {
                        uuid: file.replace('.json', ''),
                        error: 'Could not read file metadata'
                    };
                }
            })
            // Sort by timestamp (most recent first)
            .sort((a, b) => {
                if (!a.timestamp) return 1;
                if (!b.timestamp) return -1;
                return new Date(b.timestamp) - new Date(a.timestamp);
            });

        res.status(200).json({ results: files });
    } catch (err) {
        logger.error(`Error listing query results: ${err.message}`);
        res.status(500).json({ error: err.message });
    }
});

app.get('/query-results/:uuid', (req, res) => {
    const { uuid } = req.params;
    const filepath = path.join(QUERY_RESULTS_PATH, `${uuid}.json`);

    if (!fs.existsSync(filepath)) {
        return res.status(404).json({ error: `Result with UUID ${uuid} not found` });
    }

    try {
        const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
        res.status(200).json(data);
    } catch (err) {
        logger.error(`Error retrieving query result ${uuid}: ${err.message}`);
        res.status(500).json({ error: err.message });
    }
});

// Add a debugging endpoint to directly register the pagination guide tool
app.get('/debug/register-pagination-guide', (req, res) => { // Renamed endpoint
    try {
        logger.info('Manually registering pagination guide tool');

        // Schema for mcp_pagination_guide (should match Lib/tools.mjs)
        const paginationGuideSchema = {
            random_string: z.string().optional().describe("Optional dummy parameter, not used by the tool's logic. Provided for compatibility if tools without parameters are not fully supported.")
        };

        // Handler for mcp_pagination_guide (should match Lib/tools.mjs)
        const paginationGuideHandler = async (args) => {
            const guideText = `
# SQL Cursor-Based Pagination Guide

Cursor-based pagination is an efficient approach for paginating through large datasets, especially when:
- You need stable pagination through frequently changing data.
- You're handling very large datasets where OFFSET/LIMIT becomes inefficient.
- You want better performance for deep pagination.

## Key Concepts

1.  **Cursor**: A pointer to a specific item in a dataset, typically based on a unique, indexed field (or combination of fields).
2.  **Direction**: You can paginate forward ('next') or backward ('prev').
3.  **Page Size**: The number of items to return per request.
4.  **Cursor Field**: The field(s) used for ordering and creating the cursor. Must be consistently ordered.

## Example Usage with \`mcp_paginated_query\`

\`\`\`javascript
// First page (no cursor provided)
const firstPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC", // Consistent ORDER BY
  pageSize: 20,
  cursorField: "created_at,id" // Compound cursor field if created_at is not unique
});

// Response from firstPage might include:
// firstPage.result.metadata.pagination.nextCursor = "some_encoded_cursor_value"

// Next page (using cursor from previous response)
if (firstPage.result.metadata.pagination.nextCursor) {
  const nextPage = await tool.call("mcp_paginated_query", {
    sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC",
    pageSize: 20,
    cursorField: "created_at,id",
    cursor: firstPage.result.metadata.pagination.nextCursor,
    direction: "next" // Default is 'next', can be explicit
  });
}

// Previous page (going back from nextPage)
// nextPage.result.metadata.pagination.prevCursor = "another_encoded_cursor_value"
if (nextPage.result.metadata.pagination.prevCursor) {
  const prevPage = await tool.call("mcp_paginated_query", {
    sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC, id DESC",
    pageSize: 20,
    cursorField: "created_at,id",
    cursor: nextPage.result.metadata.pagination.prevCursor,
    direction: "prev"
  });
}
\`\`\`

## Best Practices

1.  **Choose an appropriate \`cursorField\`**:
    *   Should be unique or a combination of fields that guarantees uniqueness (e.g., "timestamp,id").
    *   The field(s) must be indexed for performance.
    *   Common choices: creation/update timestamps, auto-incrementing IDs.

2.  **Consistent \`ORDER BY\` Clause**:
    *   The \`ORDER BY\` clause in your SQL query **must** match the \`cursorField\` and its order.
    *   For descending order on a field, the pagination logic will handle it correctly.
    *   If \`cursorField\` is compound (e.g., "field1,field2"), the \`ORDER BY\` should be \`ORDER BY field1, field2\`.

3.  **Use \`mcp_paginated_query\`**: This tool is specifically designed for robust cursor-based pagination.

4.  **Subqueries**: For complex queries, consider applying pagination to a simplified subquery that provides the ordered keys/cursor fields, then join the full data.
`;
            return {
                content: [{ type: "text", text: guideText }],
                result: { guide: "SQL pagination guide provided successfully." }
            };
        };

        // Register the tool using the canonical name
        server.tool("mcp_pagination_guide", paginationGuideSchema, paginationGuideHandler);

        // Ensure it's available via server._tools if other parts rely on this for direct access
        // This direct manipulation might be redundant if server.tool() correctly populates it.
        if (!server._tools) server._tools = {};
        server._tools["mcp_pagination_guide"] = { schema: paginationGuideSchema, handler: paginationGuideHandler };

        const toolNames = Object.keys(server._tools || {});

        res.status(200).json({
            success: true,
            message: 'Pagination guide tool (mcp_pagination_guide) manually registered.',
            tools: toolNames
        });
    } catch (error) {
        logger.error(`Error registering pagination guide tool: ${error.message}`);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add a debugging endpoint to list all tools and their details
app.get('/debug-tools', (req, res) => {
    try {
        // Examine server._tools directly
        const toolKeys = Object.keys(server._tools || {});

        // Build detailed response
        const toolDetails = {};
        for (const key of toolKeys) {
            try {
                const tool = server._tools[key];
                toolDetails[key] = {
                    hasHandler: !!tool.handler,
                    handlerType: typeof tool.handler,
                    hasSchema: !!tool.schema,
                    schemaKeys: tool.schema ? Object.keys(tool.schema) : []
                };
            } catch (err) {
                toolDetails[key] = { error: err.message };
            }
        }

        res.status(200).json({
            toolCount: toolKeys.length,
            toolNames: toolKeys,
            toolDetails,
            raw: server._tools
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

// Add debugging endpoint to list all registered tools
app.get('/debug/tools', (req, res) => {
    try {
        const allTools = server._tools || {};
        const toolNames = Object.keys(allTools);

        // Group tools by their base name (without prefix)
        const toolsByBaseName = {};

        toolNames.forEach(name => {
            let baseName = name;

            // Remove known prefixes
            if (name.startsWith('mcp_SQL_')) {
                baseName = name.substring(8);
            } else if (name.startsWith('mcp_')) {
                baseName = name.substring(4);
            } else if (name.startsWith('SQL_')) {
                baseName = name.substring(4);
            }

            if (!toolsByBaseName[baseName]) {
                toolsByBaseName[baseName] = [];
            }

            toolsByBaseName[baseName].push(name);
        });

        res.status(200).json({
            totalTools: toolNames.length,
            toolNamesByGroup: toolsByBaseName,
            allToolNames: toolNames
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

// Setup and start server
async function startServer() {
    try {
        logger.info(`Starting MS SQL MCP Server v${server.options?.version || "1.1.0"}...`);

        // Initialize database connection pool
        await initializeDbPool();

        // Select transport based on configuration
        if (TRANSPORT === 'sse') {
            logger.info(`Setting up SSE transport on port ${PORT}`);

            // Start HTTP server for SSE transport
            await new Promise((resolve, reject) => {
                httpServer.listen(PORT, HOST, () => {
                    logger.info(`HTTP server listening on port ${PORT} and host ${HOST}`);
                    logger.info(`SSE endpoint: http://${HOST}:${PORT}/sse`);
                    logger.info(`Messages endpoint: http://${HOST}:${PORT}/messages`);
                    resolve();
                });

                httpServer.on('error', (error) => {
                    logger.error(`Failed to start HTTP server: ${error.message}`);
                    reject(error);
                });
            });

            logger.info('Waiting for SSE client connection...');
        } else if (TRANSPORT === 'stdio') {
            logger.info('Setting up STDIO transport');

            // For stdio transport, we can set up and connect immediately
            const transport = new StdioServerTransport();
            await server.connect(transport);

            logger.info('STDIO transport ready');
        } else {
            throw new Error(`Unsupported transport type: ${TRANSPORT}`);
        }

        // Add graceful shutdown handler
        process.on('SIGINT', async () => {
            logger.info('Shutting down server gracefully...');

            // Clear ping interval if it exists
            if (pingIntervalId) {
                logger.info('Clearing ping interval');
                clearInterval(pingIntervalId);
                pingIntervalId = null;
            }

            // Close active connections
            if (activeConnections.size > 0) {
                logger.info(`Closing ${activeConnections.size} active SSE connections`);
                for (const connection of activeConnections) {
                    try {
                        connection.end();
                    } catch (error) {
                        logger.error(`Error closing SSE connection: ${error.message}`);
                    }
                }
                activeConnections.clear();
            }

            // Close HTTP server if it's running
            if (httpServer && httpServer.listening) {
                logger.info('Closing HTTP server');
                await new Promise(resolve => httpServer.close(resolve));
            }

            // Close database pool
            try {
                await sql.close();
                logger.info('Database connections closed');
            } catch (err) {
                logger.error(`Error closing database connections: ${err.message}`);
            }

            logger.info('Server shutdown complete');
            process.exit(0);
        });

        logger.info('MCP Server startup complete');
    } catch (err) {
        logger.error(`Failed to start MCP server: ${err.message}`);
        process.exit(1);
    }
}

// Start the server
startServer();

export { app, server, httpServer };