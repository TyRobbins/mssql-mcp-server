import dotenv from 'dotenv';
import sql from 'mssql';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, '.env') });

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

console.log('Environment variables:');
console.log('DB_OPTIONS_TRUSTED_CONNECTION:', process.env.DB_OPTIONS_TRUSTED_CONNECTION);
console.log('DB_USER:', process.env.DB_USER);
console.log('DB_PASSWORD:', process.env.DB_PASSWORD ? '[SET]' : '[NOT SET]');

console.log('\nFinal dbConfig:');
console.log(JSON.stringify(dbConfig, null, 2));

// Test connection
async function testConnection() {
    try {
        console.log('\nTesting database connection...');
        const pool = await new sql.ConnectionPool(dbConfig).connect();
        console.log('✅ Connection successful!');
        
        const result = await pool.request().query('SELECT 1 as test');
        console.log('✅ Query successful:', result.recordset);
        
        await pool.close();
        console.log('✅ Connection closed');
    } catch (err) {
        console.error('❌ Connection failed:', err.message);
        console.error('Error details:', err);
    }
}

testConnection();
