CREATE SCHEMA database_migration; 

--/
CREATE OR REPLACE SCRIPT database_migration.OPENEDGE_TO_EXASOL(
	CONNECTION_NAME,				-- name of the database connection inside exasol, e.g. openedge_db
	SCHEMA_FILTER,					-- filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
	TARGET_SCHEMA,					-- Target_Schema on Exasol side, set to empty string to use values from souce database
	TABLE_FILTER,					-- filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
	INCLUDEDBNAME,					-- if true then OPENEDGE: schema.table => EXASOL: schema.database_table; if false then OPENEDGE: schema.table => EXASOL: schema.table
	CASE_SENSITIVE, 				-- false if identifiers should be put in uppercase
	EXCLUDECONSTRAINTS				-- 
) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''
if CASE_SENSITIVE ~= true then
	exa_upper_begin='UPPER('
	exa_upper_end=')'
end

if string.match(SCHEMA_FILTER, '%%') then
	SCHEMA_STR = [[like '']]..SCHEMA_FILTER..[['']]
else
	SCHEMA_STR = [[in ('']]..SCHEMA_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

output(SCHEMA_STR)

if string.match(TABLE_FILTER, '%%') then
	TABLE_STR = [[like '']]..TABLE_FILTER..[['']]
else
	TABLE_STR = [[in ('']]..TABLE_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

output(TABLE_STR)

if (TARGET_SCHEMA == null) then -- if no target schema set, use the original schema_name
	TARGET_SCHEMA = 'schema_name'
else
	TARGET_SCHEMA = [[']]..TARGET_SCHEMA..[[']]
end

if INCLUDEDBNAME then
	tbl_def = [["' || ]]..exa_upper_begin.. TARGET_SCHEMA ..exa_upper_end..[[ || '"."' || ]]..exa_upper_begin.. [[ db_name ]] ..exa_upper_end..[[ || '_' ||  ]]..exa_upper_begin..[[  table_name ]] ..exa_upper_end..[[  || '"]]
	tbl_group = [[db_name,schema_name,table_name]]
else
	tbl_def = [["' || ]]..exa_upper_begin.. TARGET_SCHEMA ..exa_upper_end..[[ || '"."' || ]]..exa_upper_begin..[[ table_name ]] ..exa_upper_end..[[  || '"]]
	tbl_group = [[schema_name,table_name]]
end

if EXCLUDECONSTRAINTS then
	con_def_start = '/*'
	con_def_end = '*/'
else
	con_def_start = ''
	con_def_end = ''
end

suc, res = pquery([[
	WITH oe_columns AS (
		SELECT * FROM (
			IMPORT FROM jdbc AT ]]..CONNECTION_NAME..[[ STATEMENT 
				'SELECT
					UPPER(db_name()) AS db_name,
					t.OWNER AS schema_name,
					t.TBL AS table_name,
					c.ID AS column_position,
					]]..exa_upper_begin..[[c.COL]]..exa_upper_end..[[ AS column_name,
					UPPER(c.COLTYPE) AS column_type,
					c.WIDTH AS column_max_length,
					c.WIDTH AS column_precision,
					c."SCALE" AS column_scale,
					--CASE WHEN c.NULLFLAG = ''N'' THEN '' NOT NULL'' ELSE '''' END AS column_nullable,
					c.NULLFLAG AS column_nullable,
					--CASE WHEN c.DFLT_VALUE IS NOT NULL AND c.DFLT_VALUE != '''' THEN CASE WHEN c.DFLT_VALUE = ''sysdate'' THEN '' DEFAULT CURRENT_DATE'' WHEN c.COLTYPE LIKE ''%CHAR%'' OR c.COLTYPE LIKE ''%TIME%'' OR c.COLTYPE = ''DATE'' THEN '' DEFAULT '''''' + c.DFLT_VALUE + '''''''' ELSE '' DEFAULT '' + c.DFLT_VALUE + '''' END ELSE '''' END AS column_default,
					c.DFLT_VALUE AS column_default
				FROM SYSPROGRESS.SYSTABLES t
				JOIN SYSPROGRESS.SYSCOLUMNS c ON (t.TBL = c.TBL)
				WHERE TBLTYPE = ''T''
					AND t.OWNER ]]..SCHEMA_STR..[[
					AND t.TBL ]]..TABLE_STR..[[
				'
		)
	), 
	oe_indexes AS (
		SELECT * FROM (
			IMPORT FROM jdbc AT ]]..CONNECTION_NAME..[[ STATEMENT 
				'SELECT 
					UPPER(db_name()) AS db_name,
					i.TBLOWNER AS schema_name,
					i.TBL AS table_name,
					i.IDXNAME AS index_name,
					IDXSEQ AS column_position,
					i.COLNAME AS column_name
				FROM SYSPROGRESS.SYSTABLES t
				JOIN SYSPROGRESS.SYSINDEXES i ON (t.TBL = i.TBL AND t.OWNER = i.TBLOWNER)
				WHERE TBLTYPE = ''T''
					AND i.IDXTYPE = ''U''
					AND i.ACTIVE = 1
					AND t.OWNER ]]..SCHEMA_STR..[[
					AND t.TBL ]]..TABLE_STR..[[
				'
		)
	), 
	create_schemas AS ( 
		WITH all_schemas AS (
			SELECT DISTINCT ]]..TARGET_SCHEMA..[[ AS schema_name FROM oe_columns
		)
		SELECT 'CREATE SCHEMA IF NOT EXISTS "' || ]]..exa_upper_begin..[[ schema_name ]]..exa_upper_end..[[ ||'";' AS cr_schema FROM all_schemas ORDER BY schema_name
	),
	create_tables AS (
		SELECT 'CREATE OR REPLACE TABLE ]]..tbl_def..[[ (' || cols || '); ' AS tbls
		FROM (
			SELECT ]]..tbl_group..[[,
				GROUP_CONCAT(
					'"' || column_name || '"' || ' ' ||
					CASE
						WHEN column_type = 'BIT' THEN 'BOOLEAN'
						-- ### numeric types ###
						WHEN column_type = 'INTEGER' THEN 'DECIMAL(18, 0)'
						WHEN column_type = 'NUMERIC' THEN 'DECIMAL(' || column_precision || ', ' || column_scale || ')'
						WHEN column_type = 'BIGINT' THEN 'DECIMAL(' || column_precision || ', ' || column_scale || ')'
						WHEN column_type = 'SMALLINT' THEN 'DECIMAL(' || column_precision || ', ' || column_scale || ')'
						WHEN column_type = 'FLOAT' THEN 'DOUBLE PRECISION'
						WHEN column_type = 'REAL' THEN 'DOUBLE PRECISION'
						-- ### date and time types ###
						WHEN column_type = 'DATE' THEN 'DATE'--|| CASE WHEN column_default IS NOT NULL OR column_default != '' THEN CASE WHEN column_default = 'sysdate' THEN ' DEFAULT CURRENT_DATE' ELSE '/* DEFAULT ''' || column_default || '''*/' END END
						WHEN column_type = 'TIME' THEN 'TIMESTAMP'
						WHEN column_type = 'TIMESTAMP' THEN 'TIMESTAMP'
						WHEN column_type = 'TIMESTAMP_TIMEZONE' THEN 'VARCHAR(200)'
						-- ### string types ###
						WHEN column_type = 'CHARACTER' THEN 'CHAR(' || column_max_length || ')'
						WHEN column_type = 'VARCHAR' THEN 'VARCHAR(' || column_max_length || ')'
						WHEN column_type = 'LVARCHAR' THEN 'VARCHAR(' || column_max_length || ')'
						WHEN column_type = 'VARBINARY' THEN 'VARCHAR(' || column_max_length || ')'
						WHEN column_type = 'LVARBINARY' THEN 'VARCHAR(' || column_max_length || ')'
						ELSE
							column_type
					END
					]]..con_def_start..[[ ||
					CASE
						WHEN UPPER(column_default) = 'SYSDATE' THEN ' DEFAULT CURRENT_DATE'
						WHEN UPPER(column_default) = 'SYSTIME' THEN ' DEFAULT CURRENT_TIMESTAMP'
						WHEN UPPER(column_default) = 'SYSTIMESTAMP' THEN ' DEFAULT CURRENT_TIMESTAMP'
						WHEN column_default IS NOT NULL THEN
							CASE
								WHEN column_type LIKE '%CHAR%' OR column_type LIKE 'TIME%' OR column_type = 'DATE' THEN ' DEFAULT ''' || column_default || ''''
								ELSE ' DEFAULT ' || column_default || ''
							END
					END ||
					CASE WHEN column_nullable = 'N' THEN ' NOT NULL' ELSE '' END
					]]..con_def_end..[[
				ORDER BY column_position SEPARATOR ',') AS cols
 			FROM oe_columns
			GROUP BY ]]..tbl_group..[[ 
		)
		ORDER BY tbls
	),
	import_statements as (
		SELECT 'IMPORT INTO ]]..tbl_def..[[(' || GROUP_CONCAT( '"' || column_name || '"' ORDER BY column_position SEPARATOR ', ' ) || ') FROM jdbc AT ]]..CONNECTION_NAME..[[ STATEMENT ''SELECT ' || GROUP_CONCAT( '"' || column_name || '"' ORDER BY column_position SEPARATOR ', ') || ' FROM ' ||  schema_name || '."' || table_name || '"'';' AS imp
		FROM oe_columns
		GROUP BY ]]..tbl_group..[[
		ORDER BY imp
	)]]..con_def_start..[[,
	create_indexes as (
		SELECT 'ALTER TABLE ]]..tbl_def..[[ ADD CONSTRAINT ' || idx || ' PRIMARY KEY (' || cols || ') ENABLE;' AS idxs
		FROM (
			SELECT ]]..tbl_group..[[,
				'"' || index_name || '"' AS idx, 
				GROUP_CONCAT(
					'"' || column_name || '"'
				ORDER BY column_position SEPARATOR ',') AS cols
 			FROM oe_indexes
			GROUP BY ]]..tbl_group..[[, index_name
		)
		ORDER BY idxs
	)]]..con_def_end..[[

SELECT sql_text FROM (
	SELECT 1 AS ord, CAST('-- ### SCHEMAS ###' as varchar(2000000)) AS sql_text
	UNION ALL
	SELECT 2 AS ord, a.* FROM create_schemas a
	UNION ALL
	SELECT 3 AS ord, CAST('-- ### TABLES ###' as varchar(2000000)) AS sql_text
	UNION ALL
	SELECT 4 AS ord, b.* FROM create_tables b
	WHERE b.tbls NOT LIKE '%();%'
	UNION ALL
	SELECT 5 AS ord, CAST('-- ### IMPORTS ###' as varchar(2000000)) AS sql_text
	UNION ALL
	SELECT 6 AS ord, c.* from import_statements c
	WHERE c.imp NOT LIKE '%() FROM%'
	]]..con_def_start..[[
	UNION ALL
	SELECT 7 AS ord, CAST('-- ### PRIMARY INDEXES ###' as varchar(2000000)) AS sql_text
	UNION ALL
	SELECT 8 AS ord, d.* from create_indexes d
	WHERE d.idxs NOT LIKE '%() ENABLE%'
	]]..con_def_end..[[
) ORDER BY ord
]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

-- Create a connection to the OpenEdge database
CREATE OR REPLACE CONNECTION openedge_connection 
	TO 'jdbc:datadirect:openedge://localhost:5000;databaseName=sports2000'
	USER 'sysprogress'
	IDENTIFIED BY 'sysprogress';

-- Finally start the import process
EXECUTE SCRIPT database_migration.OPENEDGE_TO_EXASOL(
	'openedge_connection',	-- CONNECTION_NAME:			name of the database connection inside exasol, e.g. openedge_dbf false then OPENEDGE: schema.table => EXASOL: schema.table
	'%',					-- SCHEMA_FILTER:			filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
	'WAREHOUSE',	-- TARGET_SCHEMA:			Target_Schema on Exasol side, set to empty string to use values from souce database
	'%',					-- TABLE_FILTER:			filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
	true,					-- INCLUDEDBNAME:			if true then OPENEDGE: schema.table => EXASOL: schema.database_table; if false then OPENEDGE: schema.table => EXASOL: schema.table
	true,					-- CASE_SENSITIVE:			false if identifiers should be put in uppercase
	true					-- EXCLUDECONSTRAINTS
);
