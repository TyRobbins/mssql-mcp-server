// lib/prompts.mjs - Prompt implementations
import { z } from 'zod';
import { logger } from './logger.mjs';

/**
 * Register all prompts with the MCP server
 * @param {object} server - MCP server instance
 */
export function registerPrompts(server) {
    logger.info('Registering prompts...');
    
    // Register prompts
    registerGenerateQueryPrompt(server);
    registerExplainQueryPrompt(server);
    registerCreateTablePrompt(server);
    
    logger.info('Prompts registered successfully.');
}

/**
 * Register the generate-query prompt
 * @param {object} server - MCP server instance
 */
function registerGenerateQueryPrompt(server) {
    server.prompt(
        "generate_query", // Changed to snake_case
        {
            description: z.string().min(1, "Description cannot be empty"),
            tables: z.array(z.string()).optional().describe("Optional list of table names to focus the query on."),
            limit: z.number().int().positive().optional().default(100).describe("Maximum number of rows to return (e.g., using TOP).")
        },
        ({ description, tables, limit = 100 }) => {
            const tablesContext = tables && tables.length > 0
                ? `The query should primarily involve these tables: ${tables.join(', ')}.`
                : 'The query should use appropriate tables from the database schema to satisfy the request.';
            
            return {
                messages: [{
                    role: "user", // Assuming "user" is the standard role for prompts requiring model generation
                    content: {
                        type: "text",
                        text: `Generate a Microsoft SQL Server query that: ${description}.
${tablesContext}

Query Requirements:
1.  The query should be compatible with SQL Server.
2.  If querying for data, limit results to ${limit} rows using the \`TOP\` clause for safety and performance, unless a different limit is explicitly part of the description.
3.  Use clear and specific column names. Avoid using \`SELECT *\` where possible.
4.  If multiple tables are involved, ensure correct \`JOIN\` conditions are used based on likely relationships.
5.  Employ informative column aliases for any complex expressions or derived values.
6.  Include brief comments in the SQL if the logic is complex or non-obvious.
7.  The query should be read-only (no data modification statements like INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE).

Provide only the SQL query as a plain text response, without any surrounding explanations, markdown code blocks, or additional text.
Example:
SELECT TOP 10 column1, column2 FROM your_table WHERE column1 = 'value';`
                    }
                }]
            };
        }
    );
}

/**
 * Register the explain_query prompt
 * @param {object} server - MCP server instance
 */
function registerExplainQueryPrompt(server) {
    server.prompt(
        "explain_query", // Changed to snake_case
        {
            query: z.string().min(1, "SQL query cannot be empty")
        },
        ({ query }) => {
            return {
                messages: [{
                    role: "user", // Assuming "user" is the standard role
                    content: {
                        type: "text",
                        text: `Please provide a detailed explanation for the following SQL Server query:

\`\`\`sql
${query}
\`\`\`

The explanation should cover:
1.  **Purpose**: A high-level summary of what the query achieves.
2.  **Clause Breakdown**: Step-by-step explanation of each clause (SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, etc.).
3.  **JOINs**: If JOINs are present, describe the types of JOINs and the relationships they establish between tables.
4.  **Filters & Conditions**: Explanation of any filtering logic in WHERE or HAVING clauses.
5.  **Aggregations & Grouping**: If applicable, describe how data is aggregated and grouped.
6.  **Subqueries**: Explain any subqueries and their role in the main query.
7.  **Potential Performance Considerations**: Briefly mention any obvious performance aspects, such as the use of indexes, or potential bottlenecks if discernible.
8.  **Potential Issues or Edge Cases**: Highlight any non-obvious behaviors, assumptions made by the query, or potential edge cases.

Format the explanation clearly. You can use markdown for structure if it helps readability.`
                    }
                }]
            };
        }
    );
}

/**
 * Register the create_table prompt
 * @param {object} server - MCP server instance
 */
function registerCreateTablePrompt(server) {
    server.prompt(
        "create_table", // Changed to snake_case
        {
            tableName: z.string().min(1, "Table name cannot be empty"),
            description: z.string().min(1, "A natural language description of the table's purpose and columns."),
            schema: z.string().optional().default("dbo").describe("Optional schema name for the table.")
        },
        ({ tableName, description, schema = "dbo" }) => {
            return {
                messages: [{
                    role: "user", // Assuming "user" is the standard role
                    content: {
                        type: "text",
                        text: `Generate a Microsoft SQL Server \`CREATE TABLE\` DDL statement for a table named \`${schema}\`.\`${tableName}\`.
The table should be designed based on the following description:
${description}

The \`CREATE TABLE\` statement must:
1.  Define appropriate column names and SQL Server data types (e.g., VARCHAR, INT, DATETIME2, DECIMAL, BIT).
2.  Specify \`NULL\` or \`NOT NULL\` constraints for each column based on the description.
3.  Identify and define a suitable Primary Key, potentially using an \`IDENTITY\` column for auto-incrementing IDs if appropriate.
4.  Include Foreign Key constraints if relationships to other tables are implied or described. (Assume standard naming for referenced tables/columns if not specified).
5.  Suggest appropriate non-clustered Indexes for columns that are likely to be frequently used in search conditions (WHERE clauses) or JOINs.
6.  Add Default values for columns where applicable (e.g., \`GETDATE()\` for a creation timestamp, 0 for a counter).
7.  Incorporate Check constraints for data validation if clearly implied by the description (e.g., a status column having specific allowed values).
8.  Adhere to SQL Server best practices for table creation.
9.  The generated DDL should be a single, complete \`CREATE TABLE\` statement.

Provide only the SQL \`CREATE TABLE\` DDL statement as a plain text response, without any surrounding explanations, markdown code blocks, or additional text.
Example:
CREATE TABLE [${schema}].[${tableName}] (
    [ID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [AnotherColumn] VARCHAR(100) NOT NULL
    -- ... other columns based on description
);`
                    }
                }]
            };
        }
    );
}