// lib/database.js - Database utilities
import sql from 'mssql';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { logger } from './logger.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, '..', '.env') });

// Primary Database configuration - ARCUSYM
const dbConfig = {
    server: process.env.DB_SERVER || 'prodarcu',
    database: process.env.DB_DATABASE || 'ARCUSYM000',
    port: parseInt(process.env.DB_PORT) || 1433,
    options: {
        encrypt: process.env.DB_ENCRYPT === 'true', 
        trustServerCertificate: true, // Always trust server certificate to handle cert issues
        enableArithAbort: process.env.DB_ENABLE_ARIAN !== 'false',
        connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 15000,
        requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 15000,
        pool: {
            max: parseInt(process.env.DB_POOL_MAX) || 10,
            min: parseInt(process.env.DB_POOL_MIN) || 0,
            idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT) || 30000
        }
    }
};

// Configure authentication for primary DB
if (process.env.DB_OPTIONS_TRUSTED_CONNECTION === 'true') {
    // Use Windows Authentication with NTLM
    dbConfig.authentication = {
        type: 'ntlm',
        options: {
            domain: process.env.DB_DOMAIN || '',
            userName: process.env.DB_WINDOWS_USER || '',
            password: process.env.DB_WINDOWS_PASSWORD || ''
        }
    };
} else {
    // Use SQL Server Authentication
    dbConfig.user = process.env.DB_USER || 'sa';
    dbConfig.password = process.env.DB_PASSWORD || 'YourStrong@Passw0rd';
}

// Secondary Database configuration - Temenos
const dbConfigTemenos = {
    server: process.env.DB_SERVER_TEMENOS || 'decsql4',
    database: process.env.DB_DATABASE_TEMENOS || 'Temenos',
    port: parseInt(process.env.DB_PORT_TEMENOS) || 1433,
    options: {
        encrypt: process.env.DB_ENCRYPT === 'true', 
        trustServerCertificate: true, // Always trust server certificate to handle cert issues
        enableArithAbort: process.env.DB_ENABLE_ARIAN !== 'false',
        connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 15000,
        requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 15000,
        pool: {
            max: parseInt(process.env.DB_POOL_MAX) || 10,
            min: parseInt(process.env.DB_POOL_MIN) || 0,
            idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT) || 30000
        }
    }
};

// Configure authentication for Temenos DB
if (process.env.DB_OPTIONS_TRUSTED_CONNECTION_TEMENOS === 'true') {
    // Use Windows Authentication with NTLM
    dbConfigTemenos.authentication = {
        type: 'ntlm',
        options: {
            domain: process.env.DB_DOMAIN_TEMENOS || '',
            userName: process.env.DB_WINDOWS_USER_TEMENOS || '',
            password: process.env.DB_WINDOWS_PASSWORD_TEMENOS || ''
        }
    };
} else {
    // Use SQL Server Authentication
    dbConfigTemenos.user = process.env.DB_USER_TEMENOS || 'sa';
    dbConfigTemenos.password = process.env.DB_PASSWORD_TEMENOS || 'YourStrong@Passw0rd';
}

// Global SQL pools
let sqlPool = null;
let sqlPoolTemenos = null;

/**
 * Initialize the SQL connection pools for both databases
 * @returns {Promise<boolean>} - True if successful
 */
export async function initializeDbPool() {
    try {
        logger.info('Initializing SQL Server connection pools...');
        
        // Create and connect the primary pool (ARCUSYM)
        sqlPool = await new sql.ConnectionPool(dbConfig).connect();
        
        // Setup pool error handler
        sqlPool.on('error', err => {
            logger.error(`ARCUSYM SQL Pool Error: ${err.message}`);
        });
        
        logger.info(`ARCUSYM connection pool initialized successfully (${dbConfig.server}/${dbConfig.database})`);
        
        // Create and connect the Temenos pool
        try {
            sqlPoolTemenos = await new sql.ConnectionPool(dbConfigTemenos).connect();
            
            // Setup pool error handler
            sqlPoolTemenos.on('error', err => {
                logger.error(`Temenos SQL Pool Error: ${err.message}`);
            });
            
            logger.info(`Temenos connection pool initialized successfully (${dbConfigTemenos.server}/${dbConfigTemenos.database})`);
        } catch (temerr) {
            logger.warn(`Failed to initialize Temenos connection pool: ${temerr.message}`);
            // Continue with just ARCUSYM connection
        }
        
        return true;
    } catch (err) {
        logger.error(`Failed to initialize SQL Server connection pools: ${err.message}`);
        throw err;
    }
}

/**
 * Check if the SQL pool is connected and initialize if necessary
 * @returns {Promise<void>}
 */
async function ensurePoolConnected() {
    if (!sqlPool) {
        await initializeDbPool();
    } else if (!sqlPool.connected) {
        logger.warn('SQL Pool disconnected, reconnecting...');
        try {
            await sqlPool.connect();
        } catch (err) {
            logger.error(`Failed to reconnect SQL pool: ${err.message}`);
            // Create a new pool if reconnect fails
            sqlPool = null;
            await initializeDbPool();
        }
    }
}

/**
 * Execute a SQL query with retry logic
 * @param {string} sqlQuery - SQL query to execute
 * @param {object} parameters - Query parameters
 * @param {number} retryCount - Number of retries on transient errors
 * @returns {Promise<object>} - Query result
 */
export async function executeQuery(sqlQuery, parameters = {}, retryCount = 3) {
    if (sqlQuery.length > 100) {
        logger.info(`Executing SQL: ${sqlQuery.substring(0, 100)}...`);
    } else {
        logger.info(`Executing SQL: ${sqlQuery}`);
    }
    
    await ensurePoolConnected();
    
    try {
        const request = sqlPool.request();
        
        // Add parameters if provided
        for (const [key, value] of Object.entries(parameters)) {
            request.input(key, value);
        }
        
        const startTime = Date.now();
        const result = await request.query(sqlQuery);
        const executionTime = Date.now() - startTime;
        
        logger.info(`SQL executed successfully in ${executionTime}ms, returned ${result.recordset?.length || 0} rows`);
        
        // Add execution time to result
        result.executionTime = executionTime;
        
        return result;
    } catch (err) {
        logger.error(`SQL execution failed: ${err.message}`);
        
        // Handle transient errors with retry logic
        const transientErrors = ['ETIMEOUT', 'ECONNCLOSED', 'ECONNRESET', 'ESOCKET'];
        if (transientErrors.includes(err.code) && retryCount > 0) {
            logger.info(`Retrying SQL execution (${retryCount} attempts left)...`);
            
            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // Force pool reconnection for connection-related errors
            if (['ECONNCLOSED', 'ECONNRESET'].includes(err.code)) {
                sqlPool = null;
            }
            
            return executeQuery(sqlQuery, parameters, retryCount - 1);
        }
        
        throw err;
    }
}

/**
 * Execute multiple SQL queries in a transaction
 * @param {Array<{sql: string, parameters: object}>} queries - Array of queries
 * @returns {Promise<Array<object>>} - Array of results
 */
export async function executeTransaction(queries) {
    if (!Array.isArray(queries) || queries.length === 0) {
        throw new Error('No queries provided for transaction');
    }
    
    logger.info(`Starting transaction with ${queries.length} queries`);
    
    await ensurePoolConnected();
    
    const transaction = new sql.Transaction(sqlPool);
    
    try {
        await transaction.begin();
        logger.info('Transaction started');
        
        const results = [];
        
        for (let i = 0; i < queries.length; i++) {
            const { sql: sqlQuery, parameters = {} } = queries[i];
            
            logger.info(`Executing transaction query ${i + 1}/${queries.length}`);
            
            const request = new sql.Request(transaction);
            
            // Add parameters if provided
            for (const [key, value] of Object.entries(parameters)) {
                request.input(key, value);
            }
            
            const result = await request.query(sqlQuery);
            results.push(result);
        }
        
        await transaction.commit();
        logger.info('Transaction committed successfully');
        
        return results;
    } catch (err) {
        logger.error(`Transaction failed: ${err.message}`);
        
        // Try to roll back the transaction
        try {
            await transaction.rollback();
            logger.info('Transaction rolled back');
        } catch (rollbackErr) {
            logger.error(`Failed to roll back transaction: ${rollbackErr.message}`);
        }
        
        throw err;
    }
}

/**
 * Check if a table exists in the database
 * @param {string} tableName - Table name to check
 * @returns {Promise<boolean>} - True if table exists
 */
export async function tableExists(tableName) {
    try {
        const result = await executeQuery(`
            SELECT COUNT(*) AS TableCount
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = @tableName
        `, { 
            tableName 
        });
        
        return result.recordset[0].TableCount > 0;
    } catch (err) {
        logger.error(`Error checking if table exists: ${err.message}`);
        return false;
    }
}

/**
 * Sanitize SQL identifier to prevent SQL injection
 * @param {string} identifier - Identifier to sanitize
 * @returns {string} - Sanitized identifier
 */
export function sanitizeSqlIdentifier(identifier) {
    if (!identifier) return '';
    
    // Remove brackets if present
    identifier = identifier.replace(/^\[|\]$/g, '');
    
    // Remove SQL injection characters and non-alphanumeric characters
    return identifier.replace(/[^a-zA-Z0-9_]/g, '');
}

/**
 * Get database configuration with optional password masking
 * @param {boolean} maskPassword - Whether to mask the password
 * @returns {object} - Database configuration
 */
export function getDbConfig(maskPassword = false) {
    const config = { ...dbConfig };
    
    if (maskPassword) {
        config.password = '********';
    }
    
    return config;
}

/**
 * Format SQL error for human-readable output
 * @param {Error} error - SQL error
 * @returns {string} - Formatted error message
 */
export function formatSqlError(error) {
    if (!error) return 'Unknown error';
    
    // Special handling for SQL Server errors
    if (error.number) {
        return `SQL Error ${error.number}: ${error.message}`;
    }
    
    return error.message || 'Unknown SQL error';
}
