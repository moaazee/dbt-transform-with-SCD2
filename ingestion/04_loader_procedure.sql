CREATE OR REPLACE PROCEDURE COMMON_DATA.load_data_from_stage(
    table_name VARCHAR DEFAULT 'ALL_TABLES',
    stage_name VARCHAR DEFAULT 'ETL_S3_STAGE_DATA',
    file_format_name VARCHAR DEFAULT 'csv_format',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var ALL_TABLES = [
        "MANUFACTURERS", "PRODUCTS", "CUSTOMERS", "INVENTORY", "SALES",
        "SALES_LINE_ITEMS"
    ];

    var results = [];
    var tablesToLoad = [];
    var inputTableName = TABLE_NAME ? TABLE_NAME.toUpperCase() : 'ALL_TABLES';

    function getQualifiedTableName(dbName, schemaName, tableName) {
        let name = '';
        if (dbName) name += dbName + '.';
        if (schemaName) name += schemaName + '.';
        name += tableName;
        return name;
    }

    if (inputTableName === 'ALL_TABLES') {
        tablesToLoad = ALL_TABLES;
        results.push("Mode: Attempting to load ALL tables: " + ALL_TABLES.join(", "));
        if (DATABASE_NAME || SCHEMA_NAME) {
            results.push("Target Schema Prefix: " + getQualifiedTableName(DATABASE_NAME, SCHEMA_NAME, '') + " (NULL = current context if missing)");
        }
    } else if (ALL_TABLES.includes(inputTableName)) {
        tablesToLoad = [inputTableName];
        results.push("Mode: Attempting to load single table: " + inputTableName);
    } else {
        return "ERROR: Invalid table name ('" + TABLE_NAME + "'). Supported: " + ALL_TABLES.join(", ") + " or 'ALL_TABLES'.";
    }

    for (var i = 0; i < tablesToLoad.length; i++) {
        var tableName = tablesToLoad[i];
        var qualifiedTableName = getQualifiedTableName(DATABASE_NAME, SCHEMA_NAME, tableName);

        var copy_sql = `
            COPY INTO ${qualifiedTableName}
            FROM @${STAGE_NAME}/${tableName.toLowerCase()}.csv
            FILE_FORMAT = (FORMAT_NAME = '${FILE_FORMAT_NAME}')
            ON_ERROR = 'ABORT_STATEMENT'
        `;

        try {
            var stmt = snowflake.createStatement({ sqlText: copy_sql });
            var res = stmt.execute();
            var load_details = [];

            if (res.next()) {
                load_details.push("Rows loaded: " + res.getColumnValue('rows_loaded'));
                load_details.push("Status: " + res.getColumnValue('status'));
            }

            results.push("SUCCESS: Data loaded into " + qualifiedTableName + ". Details: " + load_details.join(', ') + ".");

        } catch (err) {
            results.push("FAILURE: Failed to load data into " + qualifiedTableName + ". Error: " + err.message + ".");
            results.push("SQL executed: " + copy_sql.trim());
        }
    }

    return results.join('\n');
$$;
