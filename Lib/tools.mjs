// lib/tools.mjs - Database tool implementations
import { z } from 'zod';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { executeQuery, tableExists, sanitizeSqlIdentifier, formatSqlError } from './database.mjs';
// Import new pagination utilities
import {
    paginateQuery,
    generateNextCursor,
    generatePrevCursor,
    formatPaginationMetadata,
    extractDefaultCursorField
} from './pagination.mjs';
import { logger } from './logger.mjs';
import { createJsonRpcError } from './errors.mjs';

dotenv.config();

// Get the directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const QUERY_RESULTS_PATH = process.env.QUERY_RESULTS_PATH || path.join(__dirname, '../query_results');

/**
 * Registers a tool with the MCP server.
 * Assumes server.tool() is the correct SDK method for registration.
 * Logs success or failure.
 * @param {object} server - MCP server instance.
 * @param {string} name - The tool name (e.g., "mcp_execute_query").
 * @param {object} schema - Zod schema for the tool's arguments.
 * @param {function} handler - The async function to handle the tool's execution.
 */
function registerTool(server, name, schema, handler) {
    try {
        server.tool(name, schema, handler);
        logger.info(`Registered tool: ${name}`);
    } catch (err) {
        logger.error(`Failed to register tool ${name}: ${err.message}`);
    }
}

/**
 * Register all database tools
 * @param {object} server - MCP server instance
 */
function registerDatabaseTools(server) {
    logger.info("Registering database tools...");

    // Register all database tools using the unified registration function
    registerExecuteQueryTool(server);
    registerTableDetailsTool(server);
    registerProcedureDetailsTool(server);
    registerFunctionDetailsTool(server);
    registerViewDetailsTool(server);
    registerIndexDetailsTool(server);
    registerDiscoverTablesTool(server);
    registerDiscoverDatabaseTool(server);
    registerGetQueryResultsTool(server);
    registerDiscoverDatabaseOverviewTool(server); // Renamed from registerDiscoverTool
    registerPaginationGuideTool(server); // Renamed from registerCursorGuideTool
    registerPaginatedQueryTool(server);
    registerQueryStreamerTool(server);

    // Note: Logging of all registered tools directly via server internals (server._tools) is removed
    // as we rely on the SDK's registration method. If detailed listing is needed,
    // the MCP SDK might offer a way to list registered tools.
    logger.info("Database tools registration process completed.");
}

/**
 * Register the execute-query tool
 * @param {object} server - MCP server instance
 */
function registerExecuteQueryTool(server) {
    const schema = {
            sql: z.string().min(1, "SQL query cannot be empty"),
            // returnResults is deprecated as results are now always in the 'result' object. Kept for compatibility.
            returnResults: z.boolean().optional().default(false).describe("Deprecated. Results are always in the 'result' object."),
            maxRows: z.number().min(1).max(10000).optional().default(1000).describe("Max rows to return directly in results. Does not apply to saved results."),
            parameters: z.record(z.any()).optional(),
            // The following pagination parameters are kept for potential backward compatibility
            // but are not the primary way this tool handles pagination.
            // For full pagination, use mcp_paginated_query.
            pageSize: z.number().min(1).max(1000).optional().describe("For simple pagination, not recommended for large datasets."),
            cursor: z.string().optional().describe("Cursor for simple pagination."),
            cursorField: z.string().optional().describe("Cursor field for simple pagination."),
            includeCount: z.boolean().optional().default(false).describe("Include total count for simple pagination.")
    };

    const handler = async (args) => {
        const {
            sql,
            // returnResults is effectively ignored now.
            maxRows = 1000, // This mainly applies if results are not being saved to a file but returned directly.
            parameters = {},
            // Simple pagination args - consider deprecating or clarifying relation to mcp_paginated_query
            pageSize,
            cursor,
            cursorField,
            includeCount = false
        } = args;

            // Basic validation to prevent destructive operations
            const lowerSql = sql.toLowerCase();
            const prohibitedOperations = ['drop ', 'delete ', 'truncate ', 'update ', 'alter '];

            if (prohibitedOperations.some(op => lowerSql.includes(op))) {
                return {
                    content: [{
                        type: "text",
                        text: "⚠️ Error: Data modification operations (DROP, DELETE, UPDATE, TRUNCATE, ALTER) are not allowed for safety reasons."
                    }],
                    isError: true,
                    error: createJsonRpcError(-32000, "Data modification operations are not allowed.")
                };
            }

            try {
                // Note: Validation for table existence or specific permissions before execution
                // could be added here if necessary, based on `extractTableNames(sql)`.

                let totalCountForSimplePagination = null;
                let paginatedSql = sql;
                let effectiveParams = { ...parameters };

                // Basic pagination handling (if args provided)
                // For robust pagination, mcp_paginated_query should be used.
                if (pageSize && cursorField && !lowerSql.includes(" offset ") && !lowerSql.includes(" fetch ")) {
                    logger.warn("Applying simple pagination to execute_query. For robust pagination, use mcp_paginated_query.");
                    const { paginatedSql: tempSql, parameters: tempParams } = paginateQuery(sql, {
                        cursorField,
                        pageSize,
                        cursor,
                        parameters,
                        defaultCursorField: cursorField // Assuming cursorField is explicitly given for this simple case
                    });
                    paginatedSql = tempSql;
                    effectiveParams = tempParams;

                    if (includeCount) {
                        const countSql = `SELECT COUNT(*) AS TotalCount FROM (${sql.replace(/\s+ORDER\s+BY\s+.*/i, '')}) AS CountQuery`;
                        const countResult = await executeQuery(countSql, parameters);
                        totalCountForSimplePagination = countResult.recordset[0]?.TotalCount || 0;
                    }
                }


                logger.info(`Executing SQL: ${paginatedSql}`);
                const startTime = Date.now();
                const result = await executeQuery(paginatedSql, effectiveParams, maxRows); // Pass maxRows to underlying executeQuery
                const executionTime = Date.now() - startTime;
                const rowCount = result.recordset?.length || 0;
                logger.info(`SQL executed successfully in ${executionTime}ms, returned ${rowCount} rows`);

                let responseText = '';
                if (rowCount === 0) {
                    responseText = "Query executed successfully, but returned no rows.";
                } else {
                    responseText = `Query executed successfully in ${executionTime}ms and returned ${rowCount} rows.`;
                    if (result.recordset && result.recordset.length > 0) {
                        responseText += `\n\nColumns: ${Object.keys(result.recordset[0]).join(', ')}\n\n`;
                        if (rowCount >= maxRows) {
                            responseText += `Result potentially truncated to ${maxRows} rows. Use mcp_paginated_query or mcp_query_streamer for large results.\n\n`;
                        }
                    }
                }

                const uuid = crypto.randomUUID();
                // TODO: Persist results associated with this UUID if they exceed a certain size or if returnResults=false
                // For now, results are returned directly.

                return {
                    content: [{
                        type: "text",
                        text: responseText
                    }],
                    result: {
                        rowCount: rowCount,
                        results: result.recordset || [], // Direct results up to maxRows
                        metadata: {
                            uuid: uuid, // UUID for this specific execution instance
                            sql: sql, // Original SQL
                            executionTimeMs: executionTime,
                            // Basic pagination info if applied
                            pagination: pageSize ? {
                                pageSize: pageSize,
                                cursor: cursor,
                                cursorField: cursorField,
                                totalCount: totalCountForSimplePagination
                            } : null
                        }
                    }
                };
            } catch (err) {
                logger.error(`SQL execution failed: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error executing query: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `SQL execution failed: ${formatSqlError(err)}`)
                };
            }
    };

    registerTool(server, "mcp_execute_query", schema, handler);
}

/**
 * Register the table_details tool
 * @param {object} server - MCP server instance
 */
function registerTableDetailsTool(server) {
    const schema = {
        tableName: z.string().min(1, "Table name cannot be empty")
    };

    const handler = async ({ tableName }) => {
        try {
            // Parse schema and table name
            let schemaName = 'dbo'; // Default schema
            let tableNameOnly = tableName;

            if (tableName.includes('.')) {
                const parts = tableName.split('.');
                schemaName = parts[0].replace(/[\[\]]/g, '');
                tableNameOnly = parts[1].replace(/[\[\]]/g, '');
            }

            const sanitizedSchema = sanitizeSqlIdentifier(schemaName);
            const sanitizedTable = sanitizeSqlIdentifier(tableNameOnly);

            if (sanitizedSchema !== schemaName || sanitizedTable !== tableNameOnly) {
                return {
                    content: [{
                        type: "text",
                        text: `Invalid table name components: ${tableName}. Table and schema names should only contain alphanumeric characters, underscores, and not start with numbers.`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32602, "Invalid table name components.")
                };
            }

            const result = await executeQuery(`
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM
                        INFORMATION_SCHEMA.COLUMNS
                    WHERE
                        TABLE_SCHEMA = @schemaName AND
                        TABLE_NAME = @tableName
                    ORDER BY
                        ORDINAL_POSITION
            `, {
                schemaName: sanitizedSchema,
                tableName: sanitizedTable
            });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `Table '${sanitizedSchema}.${sanitizedTable}' not found.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32002, `Table '${sanitizedSchema}.${sanitizedTable}' not found.`)
                    };
                }

                let markdown = `# Table: ${sanitizedSchema}.${sanitizedTable}\n\n`;
                markdown += `## Columns\n\n`;
                markdown += `| Column Name | Data Type | Max Length | Nullable | Default |\n`;
                markdown += `|-------------|-----------|------------|----------|----------|\n`;

                result.recordset.forEach(column => {
                    markdown += `| ${column.COLUMN_NAME} | ${column.DATA_TYPE} | ${column.CHARACTER_MAXIMUM_LENGTH || 'N/A'} | ${column.IS_NULLABLE} | ${column.COLUMN_DEFAULT || 'NULL'} |\n`;
                });

                return {
                    content: [{
                        type: "text",
                        text: markdown
                    }],
                    result: {
                        columns: result.recordset || [],
                        metadata: {
                            rowCount: result.recordset.length,
                            tableName: `${sanitizedSchema}.${sanitizedTable}`
                        }
                    }
                };
            } catch (err) {
                logger.error(`Error getting table details: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error getting table details: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error getting table details: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_table_details", schema, handler);
}

/**
 * Register the procedure_details tool
 * @param {object} server - MCP server instance
 */
function registerProcedureDetailsTool(server) {
    const schema = {
        procedureName: z.string().min(1, "Procedure name cannot be empty")
    };

    const handler = async ({ procedureName }) => {
            try {
                const sanitizedProcName = sanitizeSqlIdentifier(procedureName);

                if (sanitizedProcName !== procedureName) {
                    return {
                        content: [{
                            type: "text",
                            text: `Invalid procedure name: ${procedureName}. Procedure names should only contain alphanumeric characters and underscores.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32602, "Invalid procedure name.")
                    };
                }

                const result = await executeQuery(`
                    SELECT
                        ROUTINE_DEFINITION
                    FROM
                        INFORMATION_SCHEMA.ROUTINES
                    WHERE
                        ROUTINE_TYPE = 'PROCEDURE' AND
                        ROUTINE_NAME = @procedureName
                `, { procedureName: sanitizedProcName });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `Stored procedure '${sanitizedProcName}' not found.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32002, `Stored procedure '${sanitizedProcName}' not found.`)
                    };
                }

                const paramResult = await executeQuery(`
                    SELECT
                        PARAMETER_NAME,
                        DATA_TYPE,
                        PARAMETER_MODE
                    FROM
                        INFORMATION_SCHEMA.PARAMETERS
                    WHERE
                        SPECIFIC_NAME = @procedureName
                    ORDER BY
                        ORDINAL_POSITION
                `, { procedureName: sanitizedProcName });

                let markdown = `# Stored Procedure: ${sanitizedProcName}\n\n`;

                if (paramResult.recordset.length > 0) {
                    markdown += '## Parameters\n\n';
                    markdown += '| Name | Type | Mode |\n';
                    markdown += '|------|------|------|\n';
                    paramResult.recordset.forEach(param => {
                        markdown += `| ${param.PARAMETER_NAME} | ${param.DATA_TYPE} | ${param.PARAMETER_MODE} |\n`;
                    });
                    markdown += '\n';
                }

                markdown += '## Definition\n\n';
                markdown += '```sql\n';
                markdown += result.recordset[0].ROUTINE_DEFINITION || 'Definition not available';
                markdown += '\n```\n';

                return {
                    content: [{
                        type: "text",
                        text: markdown
                    }],
                    result: {
                        procedureName: sanitizedProcName,
                        parameters: paramResult.recordset || [],
                        definition: result.recordset[0].ROUTINE_DEFINITION || 'Definition not available'
                    }
                };
            } catch (err) {
                logger.error(`Error getting procedure details: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error getting procedure details: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error getting procedure details: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_procedure_details", schema, handler);
}

/**
 * Register the function_details tool
 * @param {object} server - MCP server instance
 */
function registerFunctionDetailsTool(server) {
    const schema = {
        functionName: z.string().min(1, "Function name cannot be empty")
    };
    const handler = async ({ functionName }) => {
            try {
                const sanitizedFuncName = sanitizeSqlIdentifier(functionName);

                if (sanitizedFuncName !== functionName) {
                    return {
                        content: [{
                            type: "text",
                            text: `Invalid function name: ${functionName}. Function names should only contain alphanumeric characters and underscores.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32602, "Invalid function name.")
                    };
                }

                const result = await executeQuery(`
                    SELECT
                        ROUTINE_DEFINITION,
                        DATA_TYPE AS RETURN_TYPE
                    FROM
                        INFORMATION_SCHEMA.ROUTINES
                    WHERE
                        ROUTINE_TYPE = 'FUNCTION' AND
                        ROUTINE_NAME = @functionName
                `, { functionName: sanitizedFuncName });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `Function '${sanitizedFuncName}' not found.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32002, `Function '${sanitizedFuncName}' not found.`)
                    };
                }

                const paramResult = await executeQuery(`
                    SELECT
                        PARAMETER_NAME,
                        DATA_TYPE,
                        PARAMETER_MODE
                    FROM
                        INFORMATION_SCHEMA.PARAMETERS
                    WHERE
                        SPECIFIC_NAME = @functionName
                    ORDER BY
                        ORDINAL_POSITION
                `, { functionName: sanitizedFuncName });

                let markdown = `# Function: ${sanitizedFuncName}\n\n`;
                markdown += `**Return Type**: ${result.recordset[0].RETURN_TYPE || 'Unknown'}\n\n`;

                if (paramResult.recordset.length > 0) {
                    markdown += '## Parameters\n\n';
                    markdown += '| Name | Type | Mode |\n';
                    markdown += '|------|------|------|\n';
                    paramResult.recordset.forEach(param => {
                        markdown += `| ${param.PARAMETER_NAME} | ${param.DATA_TYPE} | ${param.PARAMETER_MODE} |\n`;
                    });
                    markdown += '\n';
                }

                markdown += '## Definition\n\n';
                markdown += '```sql\n';
                markdown += result.recordset[0].ROUTINE_DEFINITION || 'Definition not available';
                markdown += '\n```\n';

                markdown += '\n## Usage Example\n\n';
                markdown += '```sql\n';
                if (paramResult.recordset.length === 0) {
                    markdown += `-- Call scalar function\n`;
                    markdown += `SELECT dbo.${sanitizedFuncName}() AS Result\n`;
                } else {
                    markdown += `-- Call function with parameters\n`;
                    markdown += `SELECT dbo.${sanitizedFuncName}(${paramResult.recordset.map(() => '?').join(', ')}) AS Result\n`;
                }
                markdown += '```\n';

                return {
                    content: [{
                        type: "text",
                        text: markdown
                    }],
                    result: {
                        functionName: sanitizedFuncName,
                        returnType: result.recordset[0].RETURN_TYPE || 'Unknown',
                        parameters: paramResult.recordset || [],
                        definition: result.recordset[0].ROUTINE_DEFINITION || 'Definition not available'
                    }
                };
            } catch (err) {
                logger.error(`Error getting function details: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error getting function details: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error getting function details: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_function_details", schema, handler);
}

/**
 * Register the view_details tool
 * @param {object} server - MCP server instance
 */
function registerViewDetailsTool(server) {
    const schema = {
        viewName: z.string().min(1, "View name cannot be empty")
    };
    const handler = async ({ viewName }) => {
            try {
                const sanitizedViewName = sanitizeSqlIdentifier(viewName);

                if (sanitizedViewName !== viewName) {
                    return {
                        content: [{
                            type: "text",
                            text: `Invalid view name: ${viewName}. View names should only contain alphanumeric characters and underscores.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32602, "Invalid view name.")
                    };
                }

                const result = await executeQuery(`
                    SELECT
                        VIEW_DEFINITION
                    FROM
                        INFORMATION_SCHEMA.VIEWS
                    WHERE
                        TABLE_NAME = @viewName
                `, { viewName: sanitizedViewName });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `View '${sanitizedViewName}' not found.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32002, `View '${sanitizedViewName}' not found.`)
                    };
                }

                const columnResult = await executeQuery(`
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE
                    FROM
                        INFORMATION_SCHEMA.COLUMNS
                    WHERE
                        TABLE_NAME = @viewName
                    ORDER BY
                        ORDINAL_POSITION
                `, { viewName: sanitizedViewName });

                let markdown = `# View: ${sanitizedViewName}\n\n`;

                if (columnResult.recordset.length > 0) {
                    markdown += '## Columns\n\n';
                    markdown += '| Name | Type | Nullable |\n';
                    markdown += '|------|------|----------|\n';
                    columnResult.recordset.forEach(col => {
                        markdown += `| ${col.COLUMN_NAME} | ${col.DATA_TYPE} | ${col.IS_NULLABLE} |\n`;
                    });
                    markdown += '\n';
                }

                markdown += '## Definition\n\n';
                markdown += '```sql\n';
                markdown += result.recordset[0].VIEW_DEFINITION || 'Definition not available';
                markdown += '\n```\n';

                markdown += '\n## Usage Example\n\n';
                markdown += '```sql\n';
                markdown += `-- Query the view\n`;
                markdown += `SELECT TOP 100 * FROM [${sanitizedViewName}]\n`;
                markdown += '```\n';

                return {
                    content: [{
                        type: "text",
                        text: markdown
                    }],
                    result: {
                        viewName: sanitizedViewName,
                        columns: columnResult.recordset || [],
                        definition: result.recordset[0].VIEW_DEFINITION || 'Definition not available'
                    }
                };
            } catch (err) {
                logger.error(`Error getting view details: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error getting view details: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error getting view details: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_view_details", schema, handler);
}

/**
 * Register the index_details tool
 * @param {object} server - MCP server instance
 */
function registerIndexDetailsTool(server) {
    const schema = {
        tableName: z.string().min(1, "Table name cannot be empty"),
        indexName: z.string().min(1, "Index name cannot be empty")
    };
    const handler = async ({ tableName, indexName }) => {
            try {
                const sanitizedTableName = sanitizeSqlIdentifier(tableName);
                const sanitizedIndexName = sanitizeSqlIdentifier(indexName);

                if (sanitizedTableName !== tableName || sanitizedIndexName !== indexName) {
                    return {
                        content: [{
                            type: "text",
                            text: `Invalid table or index name. Names should only contain alphanumeric characters and underscores.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32602, "Invalid table or index name.")
                    };
                }

                const result = await executeQuery(`
                    SELECT
                        i.name AS IndexName,
                        i.type_desc AS IndexType,
                        i.is_unique AS IsUnique,
                        i.is_primary_key AS IsPrimaryKey,
                        i.is_unique_constraint AS IsUniqueConstraint,
                        c.name AS ColumnName,
                        ic.is_descending_key AS IsDescending,
                        ic.is_included_column AS IsIncluded
                    FROM
                        sys.indexes i
                    INNER JOIN
                        sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    INNER JOIN
                        sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    INNER JOIN
                        sys.tables t ON i.object_id = t.object_id
                    WHERE
                        t.name = @tableName AND
                        i.name = @indexName
                    ORDER BY
                        ic.key_ordinal
                `, {
                    tableName: sanitizedTableName,
                    indexName: sanitizedIndexName
                });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `Index '${sanitizedIndexName}' on table '${sanitizedTableName}' not found.`
                        }],
                        isError: true,
                        error: createJsonRpcError(-32002, `Index '${sanitizedIndexName}' on table '${sanitizedTableName}' not found.`)
                    };
                }

                let markdown = `# Index: ${sanitizedIndexName}\n\n`;
                markdown += `**Table**: ${sanitizedTableName}\n\n`;
                markdown += `**Type**: ${result.recordset[0].IndexType}\n\n`;
                markdown += `**Unique**: ${result.recordset[0].IsUnique ? 'Yes' : 'No'}\n\n`;
                markdown += `**Primary Key**: ${result.recordset[0].IsPrimaryKey ? 'Yes' : 'No'}\n\n`;
                markdown += `**Unique Constraint**: ${result.recordset[0].IsUniqueConstraint ? 'Yes' : 'No'}\n\n`;

                const keyColumns = result.recordset.filter(r => !r.IsIncluded);
                const includedColumns = result.recordset.filter(r => r.IsIncluded);

                markdown += '## Key Columns\n\n';
                markdown += '| Column | Sort Direction |\n';
                markdown += '|--------|---------------|\n';
                keyColumns.forEach(col => {
                    markdown += `| ${col.ColumnName} | ${col.IsDescending ? 'Descending' : 'Ascending'} |\n`;
                });

                if (includedColumns.length > 0) {
                    markdown += '\n## Included Columns\n\n';
                    markdown += '| Column |\n';
                    markdown += '|--------|\n';
                    includedColumns.forEach(col => {
                        markdown += `| ${col.ColumnName} |\n`;
                    });
                }

                markdown += '\n## Index Usage Query\n\n';
                markdown += '```sql\n';
                markdown += `-- Get index usage statistics\n`;
                markdown += `SELECT\n`;
                markdown += `    s.name AS SchemaName,\n`;
                markdown += `    t.name AS TableName,\n`;
                markdown += `    i.name AS IndexName,\n`;
                markdown += `    ius.user_seeks AS Seeks,\n`;
                markdown += `    ius.user_scans AS Scans,\n`;
                markdown += `    ius.user_lookups AS Lookups,\n`;
                markdown += `    ius.user_updates AS Updates,\n`;
                markdown += `    ius.last_user_seek AS LastSeek,\n`;
                markdown += `    ius.last_user_scan AS LastScan,\n`;
                markdown += `    ius.last_user_lookup AS LastLookup,\n`;
                markdown += `    ius.last_user_update AS LastUpdate\n`;
                markdown += `FROM\n`;
                markdown += `    sys.indexes i\n`;
                markdown += `INNER JOIN\n`;
                markdown += `    sys.tables t ON i.object_id = t.object_id\n`;
                markdown += `INNER JOIN\n`;
                markdown += `    sys.schemas s ON t.schema_id = s.schema_id\n`;
                markdown += `LEFT JOIN\n`;
                markdown += `    sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id\n`;
                markdown += `WHERE\n`;
                markdown += `    t.name = '${sanitizedTableName}'\n`;
                markdown += `    AND i.name = '${sanitizedIndexName}'\n`;
                markdown += '```\n';

                return {
                    content: [{
                        type: "text",
                        text: markdown
                    }],
                    result: {
                        tableName: sanitizedTableName,
                        indexName: sanitizedIndexName,
                        type: result.recordset[0].IndexType,
                        isUnique: result.recordset[0].IsUnique,
                        isPrimaryKey: result.recordset[0].IsPrimaryKey,
                        isUniqueConstraint: result.recordset[0].IsUniqueConstraint,
                        keyColumns: keyColumns.map(col => col.ColumnName),
                        includedColumns: includedColumns.map(col => col.ColumnName)
                    }
                };
            } catch (err) {
                logger.error(`Error getting index details: ${err.message}`);
                return {
                    content: [{
                        type: "text",
                        text: `Error getting index details: ${formatSqlError(err)}`
                    }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error getting index details: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_index_details", schema, handler);
}

/**
 * Register the discover_tables tool
 * @param {object} server - MCP server instance
 */
function registerDiscoverTablesTool(server) {
    const schema = {
        namePattern: z.string().optional().default('%'),
        limit: z.number().min(1).max(1000).optional().default(100),
        includeRowCounts: z.boolean().optional().default(false)
    };
    const handler = async ({ namePattern = '%', limit = 100, includeRowCounts = false }) => {
            try {
                const sanitizedPattern = namePattern.replace(/[^a-zA-Z0-9_%]/g, '');
                let query = `
                    SELECT TOP ${limit}
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        TABLE_TYPE
                    FROM
                        INFORMATION_SCHEMA.TABLES
                    WHERE
                        TABLE_TYPE = 'BASE TABLE'
                `;
                if (sanitizedPattern !== '%') {
                    query += ` AND TABLE_NAME LIKE @namePattern`;
                }
                query += ` ORDER BY TABLE_SCHEMA, TABLE_NAME`;

                const result = await executeQuery(query, { namePattern: sanitizedPattern });

                if (result.recordset.length === 0) {
                    return {
                        content: [{
                            type: "text",
                            text: `No tables found${sanitizedPattern !== '%' ? ` matching pattern '${sanitizedPattern}'` : ''}.`
                        }],
                        result: { tables: [], rowCounts: [] } // Ensure result object is present
                    };
                }

                let markdown = `# Database Tables${sanitizedPattern !== '%' ? ` Matching '${sanitizedPattern}'` : ''}\n\n`;
                markdown += `Found ${result.recordset.length} tables.\n\n`;
                let tableData = result.recordset;

                if (includeRowCounts) {
                    const tablesToCount = result.recordset.slice(0, 20); // Limit row counts for performance
                    const countedTableData = await Promise.all(tablesToCount.map(async (table) => {
                        try {
                            const countResult = await executeQuery(`
                                SELECT SUM(p.rows) AS [RowCount]
                                FROM sys.partitions p
                                INNER JOIN sys.tables t ON p.object_id = t.object_id
                                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                                WHERE s.name = @schemaName AND t.name = @tableName AND p.index_id IN (0, 1)
                            `, { schemaName: table.TABLE_SCHEMA, tableName: table.TABLE_NAME });
                            return { ...table, RowCount: countResult.recordset[0]?.RowCount || 0 };
                        } catch (err) {
                            logger.warn(`Error getting row count for ${table.TABLE_SCHEMA}.${table.TABLE_NAME}: ${err.message}`);
                            return { ...table, RowCount: 'Error' };
                        }
                    }));
                    // Merge back with non-counted tables if limit was > 20
                    tableData = countedTableData.concat(result.recordset.slice(20).map(t => ({...t, RowCount: 'Not Calculated'})));

                    markdown += '| Schema | Table Name | Row Count |\n';
                    markdown += '|--------|------------|----------|\n';
                    tableData.forEach(table => {
                        markdown += `| ${table.TABLE_SCHEMA} | ${table.TABLE_NAME} | ${table.RowCount} |\n`;
                    });
                } else {
                    markdown += '| Schema | Table Name |\n';
                    markdown += '|--------|------------|\n';
                    tableData.forEach(table => {
                        markdown += `| ${table.TABLE_SCHEMA} | ${table.TABLE_NAME} |\n`;
                    });
                }

                markdown += '\n## Next Steps\n\n';
                markdown += '1. To view a table\'s structure, use:\n';
                markdown += '```javascript\n';
                markdown += `mcp_table_details({ tableName: "${result.recordset[0].TABLE_SCHEMA}.${result.recordset[0].TABLE_NAME}" })\n`;
                markdown += '```\n\n';
                markdown += '2. To query a table, use:\n';
                markdown += '```javascript\n';
                markdown += `mcp_execute_query({ sql: "SELECT TOP 100 * FROM [${result.recordset[0].TABLE_SCHEMA}].[${result.recordset[0].TABLE_NAME}]" })\n`;
                markdown += '```\n\n';
                if (sanitizedPattern === '%') {
                    markdown += '3. To find tables by name pattern, use:\n';
                    markdown += '```javascript\n';
                    markdown += `mcp_discover_tables({ namePattern: "%search_term%" })\n`;
                    markdown += '```\n\n';
                }
                if (!includeRowCounts) {
                    markdown += '4. To include row counts (may be slower for many tables):\n';
                    markdown += '```javascript\n';
                    markdown += `mcp_discover_tables({ namePattern: "${sanitizedPattern}", includeRowCounts: true })\n`;
                    markdown += '```\n';
                }

                return {
                    content: [{ type: "text", text: markdown }],
                    result: { tables: tableData } // tableData includes row counts if requested
                };
            } catch (err) {
                logger.error(`Error discovering tables: ${err.message}`);
                return {
                    content: [{ type: "text", text: `Error discovering tables: ${formatSqlError(err)}` }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error discovering tables: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_discover_tables", schema, handler);
}

/**
 * Register the discover_database tool
 * @param {object} server - MCP server instance
 */
function registerDiscoverDatabaseTool(server) {
    const schema = {
        type: z.enum(['tables', 'views', 'procedures', 'functions', 'all']).default('all'),
        limit: z.number().min(1).max(1000).optional().default(100)
    };
    const handler = async ({ type = 'all', limit = 100 }) => {
            try {
                let markdown = `# SQL Server Database Discovery\n\n`;
                const discoveryResult = {
                    tables: [],
                    views: [],
                    procedures: [],
                    functions: []
                };

                if (type === 'tables' || type === 'all') {
                    const tablesQuery = `
                        SELECT TOP ${limit} TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_SCHEMA, TABLE_NAME`;
                    const tablesResult = await executeQuery(tablesQuery);
                    discoveryResult.tables = tablesResult.recordset || [];
                    markdown += `## Tables (${discoveryResult.tables.length})\n\n`;
                    if (discoveryResult.tables.length > 0) {
                        markdown += '| Schema | Table Name |\n|--------|------------|\n';
                        discoveryResult.tables.forEach(t => markdown += `| ${t.TABLE_SCHEMA} | ${t.TABLE_NAME} |\n`);
                        markdown += `\n### Example Query:\n\`\`\`sql\nSELECT TOP 100 * FROM [${discoveryResult.tables[0].TABLE_SCHEMA}].[${discoveryResult.tables[0].TABLE_NAME}]\n\`\`\`\n\n`;
                    } else {
                        markdown += 'No tables found.\n\n';
                    }
                }

                if (type === 'views' || type === 'all') {
                    const viewsQuery = `
                        SELECT TOP ${limit} TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_SCHEMA, TABLE_NAME`;
                    const viewsResult = await executeQuery(viewsQuery);
                    discoveryResult.views = viewsResult.recordset || [];
                    markdown += `## Views (${discoveryResult.views.length})\n\n`;
                    if (discoveryResult.views.length > 0) {
                        markdown += '| Schema | View Name |\n|--------|----------|\n';
                        discoveryResult.views.forEach(v => markdown += `| ${v.TABLE_SCHEMA} | ${v.TABLE_NAME} |\n`);
                        markdown += `\n### Example Query:\n\`\`\`sql\nSELECT TOP 100 * FROM [${discoveryResult.views[0].TABLE_SCHEMA}].[${discoveryResult.views[0].TABLE_NAME}]\n\`\`\`\n\n`;
                    } else {
                        markdown += 'No views found.\n\n';
                    }
                }

                if (type === 'procedures' || type === 'all') {
                    const procsQuery = `
                        SELECT TOP ${limit} ROUTINE_SCHEMA, ROUTINE_NAME
                        FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'
                        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME`;
                    const procsResult = await executeQuery(procsQuery);
                    discoveryResult.procedures = procsResult.recordset || [];
                    markdown += `## Stored Procedures (${discoveryResult.procedures.length})\n\n`;
                    if (discoveryResult.procedures.length > 0) {
                        markdown += '| Schema | Procedure Name |\n|--------|---------------|\n';
                        discoveryResult.procedures.forEach(p => markdown += `| ${p.ROUTINE_SCHEMA} | ${p.ROUTINE_NAME} |\n`);
                        markdown += `\n### Example:\n\`\`\`sql\nEXEC sp_helptext '${discoveryResult.procedures[0].ROUTINE_SCHEMA}.${discoveryResult.procedures[0].ROUTINE_NAME}'\n\`\`\`\n\n`;
                    } else {
                        markdown += 'No stored procedures found.\n\n';
                    }
                }

                if (type === 'functions' || type === 'all') {
                    const funcsQuery = `
                        SELECT TOP ${limit} ROUTINE_SCHEMA, ROUTINE_NAME
                        FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION'
                        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME`;
                    const funcsResult = await executeQuery(funcsQuery);
                    discoveryResult.functions = funcsResult.recordset || [];
                    markdown += `## Functions (${discoveryResult.functions.length})\n\n`;
                    if (discoveryResult.functions.length > 0) {
                        markdown += '| Schema | Function Name |\n|--------|---------------|\n';
                        discoveryResult.functions.forEach(f => markdown += `| ${f.ROUTINE_SCHEMA} | ${f.ROUTINE_NAME} |\n`);
                        markdown += `\n### Example:\n\`\`\`sql\nEXEC sp_helptext '${discoveryResult.functions[0].ROUTINE_SCHEMA}.${discoveryResult.functions[0].ROUTINE_NAME}'\n\`\`\`\n\n`;
                    } else {
                        markdown += 'No functions found.\n\n';
                    }
                }

                markdown += '## Next Steps\n\n';
                markdown += '1. To query a table:\n```javascript\nmcp_execute_query({ sql: "SELECT TOP 100 * FROM [schema].[table_name]" })\n```\n\n';
                markdown += '2. To view table structure:\n```javascript\nmcp_table_details({ tableName: "table_name" })\n```\n\n';
                markdown += '3. To view view details:\n```javascript\nmcp_view_details({ viewName: "view_name" })\n```\n\n';
                markdown += '4. To view procedure details:\n```javascript\nmcp_procedure_details({ procedureName: "procedure_name" })\n```\n\n';

                return {
                    content: [{ type: "text", text: markdown }],
                    result: { databaseDiscovery: discoveryResult }
                };
            } catch (err) {
                logger.error(`Error discovering database: ${err.message}`);
                return {
                    content: [{ type: "text", text: `Error discovering database: ${formatSqlError(err)}` }],
                    isError: true,
                    error: createJsonRpcError(-32001, `Error discovering database: ${formatSqlError(err)}`)
                };
            }
    };
    registerTool(server, "mcp_discover_database", schema, handler);
}

/**
 * Register the get_query_results tool
 * @param {object} server - MCP server instance
 */
function registerGetQueryResultsTool(server) {
    const schema = {
        uuid: z.string().uuid("Invalid UUID format").optional(),
        limit: z.number().min(1).max(100).optional().default(10) // Limit for preview in markdown
    };
    const handler = async ({ uuid, limit = 10 }) => {
            try {
                if (!fs.existsSync(QUERY_RESULTS_PATH)) {
                    return {
                        content: [{ type: "text", text: "No query results directory found." }],
                        result: { recentResults: [] } // Ensure result object is present
                    };
                }

                if (uuid) {
                    const filepath = path.join(QUERY_RESULTS_PATH, `${uuid}.json`);
                    if (!fs.existsSync(filepath)) {
                        return {
                            content: [{ type: "text", text: `Query result with UUID ${uuid} not found.` }],
                            isError: true,
                            error: createJsonRpcError(-32002, `Query result with UUID ${uuid} not found.`)
                        };
                    }
                    try {
                        const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
                        let markdown = `# Query Result: ${uuid}\n\n`;
                        markdown += `**Executed**: ${data.metadata.timestamp}\n\n`;
                        markdown += `**Query**: \`\`\`sql\n${data.metadata.query}\n\`\`\`\n\n`;
                        markdown += `**Row Count**: ${data.metadata.rowCount}\n\n`;
                        if (data.metadata.executionTimeMs) {
                            markdown += `**Execution Time**: ${data.metadata.executionTimeMs}ms\n\n`;
                        }

                        if (data.results && data.results.length > 0) {
                            markdown += `## Results Preview (first ${limit} rows)\n\n`;
                            const previewRowCount = Math.min(data.results.length, limit);
                            const previewRows = data.results.slice(0, previewRowCount);
                            markdown += '| ' + Object.keys(previewRows[0]).join(' | ') + ' |\n';
                            markdown += '| ' + Object.keys(previewRows[0]).map(() => '---').join(' | ') + ' |\n';
                            previewRows.forEach(row => {
                                markdown += '| ' + Object.values(row).map(v => v === null ? 'NULL' : (typeof v === 'object' ? JSON.stringify(v) : String(v))).join(' | ') + ' |\n';
                            });
                            if (data.results.length > previewRowCount) {
                                markdown += `\n_Showing first ${previewRowCount} of ${data.results.length} rows. Full data in result object._\n`;
                            }
                        } else {
                            markdown += `No results data found in the file.\n`;
                        }

                        return {
                            content: [{ type: "text", text: markdown }],
                            // The full results are in the data object
                            result: data
                        };
                    } catch (err) {
                        logger.error(`Error reading query result file ${uuid}.json: ${err.message}`);
                        return {
                            content: [{ type: "text", text: `Error reading query result: ${err.message}` }],
                            isError: true,
                            error: createJsonRpcError(-32001, `Error reading query result: ${err.message}`)
                        };
                    }
                } else {
                    // List recent results
                    try {
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
                                        executionTimeMs: data.metadata.executionTimeMs
                                    };
                                } catch (err) {
                                    return { uuid: file.replace('.json', ''), error: 'Could not read file metadata' };
                                }
                            })
                            .sort((a, b) => (a.timestamp && b.timestamp) ? new Date(b.timestamp) - new Date(a.timestamp) : 0)
                            .slice(0, limit);

                        let markdown = `# Recent Query Results (up to ${limit})\n\n`;
                        if (files.length === 0) {
                            markdown += 'No saved query results found.\n';
                        } else {
                            markdown += '| UUID | Timestamp | Query Preview | Row Count |\n';
                            markdown += '|------|-----------|---------------|----------|\n';
                            files.forEach(f => {
                                const queryPreview = f.query ? (f.query.length > 50 ? f.query.substring(0, 47) + '...' : f.query) : 'N/A';
                                markdown += `| ${f.uuid} | ${f.timestamp || 'N/A'} | \`${queryPreview}\` | ${f.rowCount === undefined ? 'N/A' : f.rowCount} |\n`;
                            });
                            if (files.length > 0 && files[0].uuid) {
                                markdown += `\n## Viewing Specific Results\n\nTo view details for a specific result, use:\n\`\`\`javascript\nmcp_get_query_results({ uuid: "${files[0].uuid}" })\n\`\`\`\n`;
                            }
                        }
                        return {
                            content: [{ type: "text", text: markdown }],
                            result: { recentResults: files }
                        };
                    } catch (err) {
                        logger.error(`Error listing query results: ${err.message}`);
                        return {
                            content: [{ type: "text", text: `Error listing query results: ${err.message}` }],
                            isError: true,
                            error: createJsonRpcError(-32001, `Error listing query results: ${err.message}`)
                        };
                    }
                }
            } catch (err) {
                logger.error(`Error processing query results: ${err.message}`);
                return {
                    content: [{ type: "text", text: `Error processing query results: ${err.message}` }],
                    isError: true,
                    error: createJsonRpcError(-32000, `Error processing query results: ${err.message}`)
                };
            }
    };
    registerTool(server, "mcp_get_query_results", schema, handler);
}

/**
 * Register the discover_database_overview tool - provides a general database overview.
 * Renamed from registerDiscoverTool for clarity.
 * @param {object} server - MCP server instance
 */
function registerDiscoverDatabaseOverviewTool(server) {
    const schema = {
        // Retaining optional random_string for compatibility if SDK requires parameters,
        // but clarifying its purpose. Ideally, SDK supports parameterless tools.
        random_string: z.string().optional().describe("Optional dummy parameter, not used by the tool's logic. Provided for compatibility if tools without parameters are not fully supported.")
    };
    const handler = async (args) => {
        try {
            const tablesResult = await executeQuery("SELECT TOP 100 TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME");
            const procsResult = await executeQuery("SELECT TOP 100 ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME");
            const funcsResult = await executeQuery("SELECT TOP 100 ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME");
            const viewsResult = await executeQuery("SELECT TOP 100 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_SCHEMA, TABLE_NAME");

            let markdown = `# Database Overview\n\n`;
            markdown += `## Tables (Top 100)\n| Schema | Table | Type |\n| ------ | ----- | ---- |\n`;
            (tablesResult.recordset || []).forEach(t => markdown += `| ${t.TABLE_SCHEMA} | ${t.TABLE_NAME} | ${t.TABLE_TYPE} |\n`);
            if ((tablesResult.recordset || []).length === 100) markdown += `\n_Showing first 100 tables. There may be more._\n\n`;

            markdown += `\n## Stored Procedures (Top 100)\n| Schema | Procedure |\n| ------ | --------- |\n`;
            (procsResult.recordset || []).forEach(p => markdown += `| ${p.ROUTINE_SCHEMA} | ${p.ROUTINE_NAME} |\n`);
            if ((procsResult.recordset || []).length === 100) markdown += `\n_Showing first 100 procedures. There may be more._\n\n`;

            markdown += `\n## Functions (Top 100)\n| Schema | Function |\n| ------ | -------- |\n`;
            (funcsResult.recordset || []).forEach(f => markdown += `| ${f.ROUTINE_SCHEMA} | ${f.ROUTINE_NAME} |\n`);
            if ((funcsResult.recordset || []).length === 100) markdown += `\n_Showing first 100 functions. There may be more._\n\n`;

            markdown += `\n## Views (Top 100)\n| Schema | View |\n| ------ | ---- |\n`;
            (viewsResult.recordset || []).forEach(v => markdown += `| ${v.TABLE_SCHEMA} | ${v.TABLE_NAME} |\n`);
            if ((viewsResult.recordset || []).length === 100) markdown += `\n_Showing first 100 views. There may be more._\n\n`;

            markdown += `## Usage Examples\n\n`;
            markdown += `### Get Table Details:\n\`\`\`javascript\nmcp_table_details({ tableName: "schema.TableName" })\n\`\`\`\n\n`;
            markdown += `### Execute Query:\n\`\`\`javascript\nmcp_execute_query({ sql: "SELECT TOP 10 * FROM schema.TableName" })\n\`\`\`\n\n`;
            markdown += `### Discover Database Objects (more detailed):\n\`\`\`javascript\nmcp_discover_database({ type: "all", limit: 100 })\n\`\`\`\n`;

            return {
                content: [{ type: "text", text: markdown }],
                result: {
                    tables: tablesResult.recordset || [],
                    procedures: procsResult.recordset || [],
                    functions: funcsResult.recordset || [],
                    views: viewsResult.recordset || []
                }
            };
        } catch (err) {
            logger.error(`Error in discover_database_overview tool: ${err.message}`);
            return {
                content: [{ type: "text", text: `Error getting database overview: ${formatSqlError(err)}` }],
                isError: true,
                error: createJsonRpcError(-32001, `Error getting database overview: ${formatSqlError(err)}`)
            };
        }
    };
    registerTool(server, "mcp_discover_database_overview", schema, handler);
}

/**
 * Register the pagination_guide tool - provides a guide to cursor-based pagination.
 * Renamed from registerCursorGuideTool for clarity.
 * @param {object} server - MCP server instance
 */
function registerPaginationGuideTool(server) {
    const schema = {
        // Retaining optional random_string for compatibility if SDK requires parameters,
        // but clarifying its purpose. Ideally, SDK supports parameterless tools.
        random_string: z.string().optional().describe("Optional dummy parameter, not used by the tool's logic. Provided for compatibility if tools without parameters are not fully supported.")
    };
    const handler = async (args) => {
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
    registerTool(server, "mcp_pagination_guide", schema, handler);
}

/**
 * Register the paginated_query tool
 * @param {object} server - MCP server instance
 */
function registerPaginatedQueryTool(server) {
    const schema = {
        sql: z.string().min(1, "SQL query cannot be empty"),
        cursorField: z.string().optional().describe("Comma-separated field(s) for cursor. Must match ORDER BY clause."),
        pageSize: z.number().min(1).max(1000).optional().default(50),
        cursor: z.string().optional().describe("Encoded cursor value from previous page."),
        parameters: z.record(z.any()).optional(),
        includeCount: z.boolean().optional().default(true).describe("Whether to include total record count (can be expensive)."),
        direction: z.enum(['next', 'prev']).optional().default('next'),
        returnTotals: z.boolean().optional().default(true).describe("Legacy. Use includeCount. If false, totalCount might be null.") // Consider merging with includeCount
    };

    const handler = async ({
        sql,
        cursorField,
        pageSize = 50,
        cursor,
        parameters = {},
        includeCount = true, // Prioritize this over returnTotals
        direction = 'next',
        returnTotals = true // Kept for now, but logic uses includeCount
    }) => {
        const lowerSql = sql.toLowerCase();
        const prohibitedOperations = ['drop ', 'delete ', 'truncate ', 'update ', 'alter '];
        if (prohibitedOperations.some(op => lowerSql.includes(op))) {
            return {
                content: [{ type: "text", text: "⚠️ Error: Data modification operations are not allowed." }],
                isError: true,
                error: createJsonRpcError(-32000, "Data modification operations are not allowed.")
            };
        }

        try {
            let totalCount = null;
            let estimatedTotalPages = null;

            const actualIncludeCount = includeCount || returnTotals; // Combine logic

            if (actualIncludeCount) {
                try {
                    let countSql = sql;
                    countSql = countSql.replace(/\s+ORDER\s+BY\s+.+?(?:(?:OFFSET|FETCH|$))/i, ' ');
                    countSql = countSql.replace(/\s+OFFSET\s+.+?(?:FETCH|$)/i, ' ');
                    countSql = countSql.replace(/\s+FETCH\s+.+?$/i, ' ');
                    countSql = `SELECT COUNT_BIG(*) AS TotalCount FROM (${countSql}) AS CountQuery`; // Use COUNT_BIG for large tables
                    logger.info(`Executing count query: ${countSql}`);
                    const countResult = await executeQuery(countSql, parameters);
                    if (countResult.recordset && countResult.recordset.length > 0) {
                        totalCount = Number(countResult.recordset[0].TotalCount); // Ensure it's a number
                        estimatedTotalPages = Math.ceil(totalCount / pageSize);
                        logger.info(`Total count query returned: ${totalCount} rows (${estimatedTotalPages} pages)`);
                    }
                } catch (countErr) {
                    logger.warn(`Error executing count query: ${countErr.message}. Proceeding without total count.`);
                    // Do not set totalCount to null here if it was already calculated from a previous page in the cursor
                }
            }

            const defaultCursorField = extractDefaultCursorField(sql); // Extracts from ORDER BY
            const effectiveCursorField = cursorField || defaultCursorField;
            if (!effectiveCursorField) {
                throw new Error("Cursor field is required for pagination. Provide cursorField or ensure SQL has an ORDER BY clause.");
            }
            logger.info(`Using cursor field: ${effectiveCursorField}`);

            const { paginatedSql, parameters: paginatedParams } =
                paginateQuery(sql, {
                    cursorField: effectiveCursorField,
                    pageSize,
                    cursor,
                    parameters,
                    defaultCursorField // Already determined
                });
            logger.info(`Paginated SQL: ${paginatedSql}`);

            const startTime = Date.now();
            const result = await executeQuery(paginatedSql, paginatedParams);
            const executionTime = Date.now() - startTime;
            const rowCount = result.recordset?.length || 0;

            let nextCursorValue = null;
            let prevCursorValue = null;

            if (rowCount > 0) {
                const hasMore = rowCount >= pageSize; // If we got a full page, there might be more
                nextCursorValue = hasMore ? generateNextCursor(result.recordset[rowCount - 1], effectiveCursorField) : null;
                // Previous cursor should ideally be generated based on the *first* item of the current set,
                // but only if a cursor was provided (i.e., not on the first page).
                prevCursorValue = cursor ? generatePrevCursor(result.recordset[0], effectiveCursorField) : null;
            }


            const uuid = crypto.randomUUID();
            const filename = `${uuid}.json`;
            const filepath = path.join(QUERY_RESULTS_PATH, filename);

            const paginationMeta = formatPaginationMetadata({
                cursorField: effectiveCursorField,
                pageSize,
                returnedRows: rowCount,
                nextCursor: nextCursorValue,
                prevCursor: prevCursorValue, // Use the generated one
                direction,
                totalCount: totalCount, // Use the determined totalCount
                estimatedTotalPages,
                originalCursor: cursor // Include the cursor that generated this page
            });
            
            if (result.recordset && result.recordset.length > 0) {
                 try {
                    const resultWithMetadata = {
                        metadata: {
                            uuid,
                            timestamp: new Date().toISOString(),
                            query: sql, // Original SQL
                            parameters: Object.keys(parameters).length > 0 ? parameters : undefined,
                            rowCount, // Rows in this page
                            executionTimeMs: executionTime,
                            pagination: paginationMeta
                        },
                        results: result.recordset || []
                    };
                    if (!fs.existsSync(QUERY_RESULTS_PATH)) fs.mkdirSync(QUERY_RESULTS_PATH, { recursive: true });
                    fs.writeFileSync(filepath, JSON.stringify(resultWithMetadata, null, 2));
                    logger.info(`Paginated query results saved to ${filepath}`);
                } catch (writeError) {
                    logger.error(`Error saving query results to file: ${writeError.message}`);
                }
            }

            let markdown = `# Paginated Query Results\n\n`;
            markdown += `Query executed in ${executionTime}ms, returned ${rowCount} rows.\n`;
            if (paginationMeta.totalCount !== null) {
                markdown += ` Total records: ${paginationMeta.totalCount}. Page ${paginationMeta.currentPage || '-'} of ${paginationMeta.totalPages || '-'}.\n`;
            }
            markdown += `Cursor field: \`${effectiveCursorField}\`. Page size: ${pageSize}.\n\n`;


            if (rowCount > 0) {
                markdown += `## Results Preview (first 10 rows)\n\n`;
                const previewRows = result.recordset.slice(0, 10);
                markdown += '| ' + Object.keys(previewRows[0]).join(' | ') + ' |\n';
                markdown += '| ' + Object.keys(previewRows[0]).map(() => '---').join(' | ') + ' |\n';
                previewRows.forEach(row => {
                    markdown += '| ' + Object.values(row).map(v => v === null ? 'NULL' : (typeof v === 'object' ? JSON.stringify(v) : String(v))).join(' | ') + ' |\n';
                });
                if (result.recordset.length > 10) {
                    markdown += `\n_Showing first 10 of ${result.recordset.length} rows._\n\n`;
                }
            } else {
                markdown += `\n**No results returned for this page.**\n\n`;
            }

            markdown += `\n📄 Complete results for this page saved with ID: \`${uuid}\`\n`;
            markdown += `To view these results: \`mcp_get_query_results({ uuid: "${uuid}" })\`\n\n`;

            markdown += `## Navigation\n\n`;
            const navBaseArgs = {
                sql: JSON.stringify(sql),
                pageSize: pageSize,
                cursorField: `"${effectiveCursorField}"`,
                includeCount: actualIncludeCount,
                // parameters: parameters // Note: parameters can be large, consider omitting from examples for brevity
            };
            if (Object.keys(parameters).length > 0) {
                 markdown += `// Original parameters were: ${JSON.stringify(parameters)}\n`;
            }


            if (paginationMeta.nextCursor) {
                markdown += `### Next Page:\n\`\`\`javascript\nmcp_paginated_query({\n  sql: ${navBaseArgs.sql},\n  pageSize: ${navBaseArgs.pageSize},\n  cursorField: ${navBaseArgs.cursorField},\n  cursor: "${paginationMeta.nextCursor}",\n  direction: "next",\n  includeCount: ${navBaseArgs.includeCount}\n})\n\`\`\`\n\n`;
            } else {
                markdown += `**No more results (next page).**\n\n`;
            }
            if (paginationMeta.prevCursor) {
                markdown += `### Previous Page:\n\`\`\`javascript\nmcp_paginated_query({\n  sql: ${navBaseArgs.sql},\n  pageSize: ${navBaseArgs.pageSize},\n  cursorField: ${navBaseArgs.cursorField},\n  cursor: "${paginationMeta.prevCursor}",\n  direction: "prev",\n  includeCount: ${navBaseArgs.includeCount}\n})\n\`\`\`\n\n`;
            }
            if (cursor) { // Only show "first page" if not already on it (i.e., if a cursor was used)
                markdown += `### Return to First Page:\n\`\`\`javascript\nmcp_paginated_query({\n  sql: ${navBaseArgs.sql},\n  pageSize: ${navBaseArgs.pageSize},\n  cursorField: ${navBaseArgs.cursorField},\n  includeCount: ${navBaseArgs.includeCount}\n})\n\`\`\`\n`;
            }

            return {
                content: [{ type: "text", text: markdown }],
                result: {
                    rowCount: rowCount, // Rows in current page
                    results: result.recordset || [],
                    metadata: {
                        uuid: uuid, // UUID for this page's saved file
                        pagination: paginationMeta,
                        executionTimeMs: executionTime
                        // totalCount is now part of paginationMeta
                    }
                }
            };
        } catch (err) {
            logger.error(`Error executing paginated query: ${err.message}`);
            const formattedError = formatSqlError(err);
            return {
                content: [{ type: "text", text: `Error executing paginated query: ${formattedError}` }],
                isError: true,
                error: createJsonRpcError(-32001, `Paginated query failed: ${formattedError}`)
            };
        }
    };
    registerTool(server, "mcp_paginated_query", schema, handler);
}

/**
 * Register the query_streamer tool
 * @param {object} server - MCP server instance
 */
function registerQueryStreamerTool(server) {
    const schema = {
        sql: z.string().min(1, "SQL query cannot be empty"),
        batchSize: z.number().min(100).max(10000).optional().default(1000),
        maxRows: z.number().min(1).max(1000000).optional().default(100000).describe("Maximum total rows to process across all batches."),
        parameters: z.record(z.any()).optional(),
        cursorField: z.string().optional().describe("Field for cursor pagination; determined from ORDER BY if not set."),
        outputType: z.enum(['json', 'csv', 'summary']).optional().default('summary').describe("Format for saved results file."),
        aggregations: z.array(
            z.object({
                field: z.string(),
                operation: z.enum(['sum', 'avg', 'min', 'max', 'count', 'countDistinct'])
            })
        ).optional().describe("Aggregations to perform over the streamed results.")
    };

    const handler = async ({
        sql,
        batchSize = 1000,
        maxRows = 100000,
        parameters = {},
        cursorField,
        outputType = 'summary',
        aggregations
    }) => {
        const lowerSql = sql.toLowerCase();
        const prohibitedOperations = ['drop ', 'delete ', 'truncate ', 'update ', 'alter '];
        if (prohibitedOperations.some(op => lowerSql.includes(op))) {
            return {
                content: [{ type: "text", text: "⚠️ Error: Data modification operations are not allowed." }],
                isError: true,
                error: createJsonRpcError(-32000, "Data modification operations are not allowed.")
            };
        }

        try {
            const defaultCursorField = extractDefaultCursorField(sql);
            const effectiveCursorField = cursorField || defaultCursorField;
            if (!effectiveCursorField) {
                throw new Error("Cursor field is required for streaming. Provide cursorField or ensure SQL has an ORDER BY clause.");
            }
            logger.info(`Starting query streamer with cursor field: ${effectiveCursorField}, batch size: ${batchSize}, max rows: ${maxRows}`);

            const aggregationResults = {};
            if (aggregations) {
                aggregations.forEach(agg => {
                    const key = `${agg.operation}:${agg.field}`;
                    if (agg.operation === 'sum' || agg.operation === 'avg') aggregationResults[key] = 0;
                    else if (agg.operation === 'min') aggregationResults[key] = Number.MAX_VALUE;
                    else if (agg.operation === 'max') aggregationResults[key] = Number.MIN_VALUE;
                    else if (agg.operation === 'count') aggregationResults[key] = 0;
                    else if (agg.operation === 'countDistinct') aggregationResults[key] = new Set();
                });
            }

            let currentCursor = null;
            let totalProcessedRows = 0;
            let hasMoreData = true;
            let batchCount = 0;
            let allResultsForJson = []; // Only used if outputType is 'json'
            let csvHeaderWritten = false;
            let csvWriteStream; // For CSV streaming to file

            const uuid = crypto.randomUUID();
            const fileExtension = outputType === 'csv' ? 'csv' : 'json';
            const outputPath = path.join(QUERY_RESULTS_PATH, `${uuid}.${fileExtension}`);
            if (!fs.existsSync(QUERY_RESULTS_PATH)) fs.mkdirSync(QUERY_RESULTS_PATH, { recursive: true });

            if (outputType === 'csv') {
                csvWriteStream = fs.createWriteStream(outputPath);
            }

            logger.info(`Beginning streaming query execution. Output to: ${outputPath}`);
            const overallStartTime = Date.now();

            while (hasMoreData && totalProcessedRows < maxRows) {
                batchCount++;
                const remainingRowsToFetch = maxRows - totalProcessedRows;
                const currentBatchSize = Math.min(batchSize, remainingRowsToFetch);

                if (currentBatchSize <= 0) { // Should not happen if maxRows logic is correct
                    hasMoreData = false;
                    break;
                }

                const { paginatedSql, parameters: paginatedParams } =
                    paginateQuery(sql, {
                        cursorField: effectiveCursorField,
                        pageSize: currentBatchSize,
                        cursor: currentCursor,
                        parameters,
                        defaultCursorField
                    });

                logger.info(`Executing batch ${batchCount} (size ${currentBatchSize}) with cursor: ${currentCursor || 'initial'}`);
                const batchStartTime = Date.now();
                const batchResult = await executeQuery(paginatedSql, paginatedParams);
                const batchTime = Date.now() - batchStartTime;
                const batchRows = batchResult.recordset || [];
                const batchRowCount = batchRows.length;

                totalProcessedRows += batchRowCount;
                hasMoreData = batchRowCount >= currentBatchSize; // If we got less than requested, no more data

                logger.info(`Batch ${batchCount} returned ${batchRowCount} rows in ${batchTime}ms. Total rows processed: ${totalProcessedRows}. Has more: ${hasMoreData}`);

                if (batchRowCount > 0) {
                    if (hasMoreData) {
                        currentCursor = generateNextCursor(batchRows[batchRowCount - 1], effectiveCursorField);
                    }

                    if (outputType === 'json') {
                        allResultsForJson.push(...batchRows);
                    } else if (outputType === 'csv') {
                        if (!csvHeaderWritten && batchRows.length > 0) {
                            const headers = Object.keys(batchRows[0]);
                            await new Promise(resolve => csvWriteStream.write(headers.join(',') + '\n', resolve));
                            csvHeaderWritten = true;
                        }
                        for (const row of batchRows) {
                            const values = Object.values(row).map(v => {
                                if (v === null || v === undefined) return '';
                                if (typeof v === 'string') return `"${v.replace(/"/g, '""')}"`;
                                return String(v);
                            });
                            await new Promise(resolve => csvWriteStream.write(values.join(',') + '\n', resolve));
                        }
                    }

                    if (aggregations) {
                        batchRows.forEach(row => {
                            aggregations.forEach(agg => {
                                const { field, operation } = agg;
                                const value = row[field];
                                const key = `${operation}:${field}`;
                                if (value !== null && value !== undefined) {
                                    switch (operation) {
                                        case 'sum': if (typeof value === 'number') aggregationResults[key] += value; break;
                                        case 'avg': if (typeof value === 'number') aggregationResults[key] += value; break; // Sum for now, avg calculated at the end
                                        case 'min': if (typeof value === 'number' && value < aggregationResults[key]) aggregationResults[key] = value; break;
                                        case 'max': if (typeof value === 'number' && value > aggregationResults[key]) aggregationResults[key] = value; break;
                                        case 'count': aggregationResults[key]++; break;
                                        case 'countDistinct': aggregationResults[key].add(value); break;
                                    }
                                }
                            });
                        });
                    }
                }
                 if (batchRowCount < currentBatchSize) { // If we fetched less than batch size, it means no more data
                    hasMoreData = false;
                }
            }

            const totalTime = Date.now() - overallStartTime;
            logger.info(`Streaming query completed in ${totalTime}ms, processed ${totalProcessedRows} rows in ${batchCount} batches.`);

            if (aggregations) {
                aggregations.forEach(agg => {
                    const { field, operation } = agg;
                    const key = `${operation}:${field}`;
                    if (operation === 'avg') {
                        const countKey = `count:${field}`;
                        const count = aggregationResults[countKey] || (aggregations.find(a => a.field === field && a.operation === 'count') ? 0 : totalProcessedRows); // Fallback to totalProcessedRows if no explicit count agg
                        if (count > 0) aggregationResults[key] /= count; else aggregationResults[key] = 0;
                    } else if (operation === 'countDistinct') {
                        aggregationResults[key] = aggregationResults[key].size;
                    }
                });
            }
            
            const streamMetadata = {
                uuid,
                timestamp: new Date().toISOString(),
                query: sql,
                totalRowsProcessed: totalProcessedRows,
                batchCount,
                executionTimeMs: totalTime,
                outputType,
                outputPath, // Include output path
                aggregations: Object.keys(aggregationResults).length > 0 ? aggregationResults : undefined
            };

            if (outputType === 'json') {
                fs.writeFileSync(outputPath, JSON.stringify({ metadata: streamMetadata, results: allResultsForJson }, null, 2));
            } else if (outputType === 'csv') {
                await new Promise(resolve => csvWriteStream.end(resolve)); // Close stream
                 // For CSV, we might just save metadata separately or include it in summary
                const metaFilePath = path.join(QUERY_RESULTS_PATH, `${uuid}.metadata.json`);
                fs.writeFileSync(metaFilePath, JSON.stringify({metadata: streamMetadata }, null, 2));
                logger.info(`CSV data saved to ${outputPath}, metadata to ${metaFilePath}`);
            } else { // summary
                fs.writeFileSync(outputPath, JSON.stringify({ metadata: streamMetadata }, null, 2));
            }
            logger.info(`Streaming results processing complete. Output available at: ${outputPath}`);

            let markdown = `# Streamed Query Summary\n\n`;
            markdown += `## Overview\n\n`;
            markdown += `- **Total Rows Processed**: ${totalProcessedRows.toLocaleString()}\n`;
            markdown += `- **Batches**: ${batchCount}\n`;
            markdown += `- **Execution Time**: ${totalTime.toLocaleString()}ms\n`;
            if (totalTime > 0) markdown += `- **Average Rate**: ${Math.round(totalProcessedRows / (totalTime / 1000)).toLocaleString()} rows/second\n`;
            markdown += `- **Output Type**: ${outputType}\n`;
            markdown += `- **Output File ID**: \`${uuid}\` (File: \`${path.basename(outputPath)}\`)\n\n`;

            if (aggregations && Object.keys(aggregationResults).length > 0) {
                markdown += `## Aggregation Results\n\n| Field | Operation | Result |\n|-------|-----------|--------|\n`;
                aggregations.forEach(agg => {
                    const key = `${agg.operation}:${agg.field}`;
                    let value = aggregationResults[key];
                    if (typeof value === 'number') value = value.toLocaleString(undefined, { maximumFractionDigits: 4 });
                    markdown += `| ${agg.field} | ${agg.operation} | ${value} |\n`;
                });
                markdown += '\n';
            }

            markdown += `## Accessing Results\n\n`;
            markdown += `Results data saved with ID: \`${uuid}\`.\n`;
            if (outputType === 'json' || outputType === 'summary') {
                 markdown += `To load metadata (and results if JSON): \`mcp_get_query_results({ uuid: "${uuid}" })\`\n`;
            } else if (outputType === 'csv') {
                markdown += `CSV data saved to file. Metadata accessible via: \`mcp_get_query_results({ uuid: "${uuid}.metadata" })\` (or similar, check actual file name for metadata)\nAlternatively, the file path is: ${outputPath}\n`;
            }
             markdown += `The raw file path is: \`${outputPath}\` (access might be restricted).\n`;


            return {
                content: [{ type: "text", text: markdown }],
                result: { // Return metadata about the stream process itself
                    streamingSummary: streamMetadata
                }
            };
        } catch (err) {
            logger.error(`Error executing streaming query: ${err.message}`);
            const formattedError = formatSqlError(err);
            return {
                content: [{ type: "text", text: `Error executing streaming query: ${formattedError}` }],
                isError: true,
                error: createJsonRpcError(-32001, `Streaming query failed: ${formattedError}`)
            };
        }
    };
    registerTool(server, "mcp_query_streamer", schema, handler);
}

// Export the database tools for use in the server
export {
    registerDatabaseTools,
    // Individual registration functions are not typically exported if registerDatabaseTools is the main entry point
    // registerExecuteQueryTool,
    // registerTableDetailsTool,
    // registerProcedureDetailsTool,
    // registerFunctionDetailsTool,
    // registerViewDetailsTool,
    // registerIndexDetailsTool,
    // registerDiscoverTablesTool,
    // registerDiscoverDatabaseTool,
    // registerGetQueryResultsTool,
    // registerDiscoverDatabaseOverviewTool, // Renamed
    // registerPaginationGuideTool,      // Renamed
    // registerPaginatedQueryTool,
    // registerQueryStreamerTool
};
