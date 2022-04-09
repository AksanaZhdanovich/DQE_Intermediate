CREATE PROCEDURE [hr].[System_info_tables]
(
  @p_DatabaseName AS NVARCHAR(MAX)
, @p_SchemaName AS NVARCHAR(MAX) 
, @p_TableName AS NVARCHAR(MAX))
AS

BEGIN
	DECLARE @v_Query NVARCHAR(MAX), @v_Query_db NVARCHAR(MAX);
	SELECT @v_Query_db = 'USE ' + @p_DatabaseName;
	EXEC SP_EXECUTESQL @v_Query_db;

	WITH
		list_of_tables AS
    (SELECT
	  [all_objects].[name] as [Table_name], [sys].[columns].[name] as [Column_name], [sys].[types].[name] as [Data_type]
				, LEAD([all_objects].[name]) OVER (ORDER BY [all_objects].[name]) [next_name]
	 FROM [sys].[all_objects]
		INNER JOIN [sys].[schemas] on [sys].[schemas].[schema_id] = [all_objects].[schema_id]
		INNER JOIN [sys].[columns] on [sys].[columns].[object_id] = [all_objects].[object_id]
		INNER JOIN [sys].[types]   ON [sys].[types].[user_type_id] = [sys].[columns].[user_type_id]
	 WHERE  
			  [sys].[schemas].name =  @p_SchemaName 
		  AND [all_objects].[type_desc] = 'USER_TABLE'
		  AND [all_objects].[name] like '%'
		),
		query_list AS 
		(
			SELECT
				CASE 
					WHEN [next_name] IS NOT NULL 
						THEN 'SELECT   
								''' + @p_DatabaseName + ''' as [Database Name] 
							   ,''' + @p_SchemaName + ''' as [Schema name]
							   ,''' + list_of_tables.[Table_Name] + ''' AS [Table Name] 
							   ,COUNT(*) AS [Total row count]
							   ,''' + list_of_tables.[Column_Name] + ''' AS [Column Name] 
							   ,''' + list_of_tables.[Data_type] + ''' AS [Data Type] 
							   , COUNT(DISTINCT ' + list_of_tables.[Column_Name] + ') AS [Count of DISTINCT values]
							   , SUM(CASE WHEN ' + list_of_tables.[Column_Name] + ' IS NULL THEN 1 ELSE 0 END ) AS [Count of NULL values]
							   , SUM(CASE WHEN (CAST(''' + list_of_tables.[Data_type] + ''' AS varchar(8)) LIKE '+'''int'''+' AND CAST(' + list_of_tables.[Column_Name] + ' AS varchar(8)) = ''0'' )
								    OR (CAST(''' + list_of_tables.[Data_type] + ''' AS varchar(8)) LIKE '+'''%char'''+' AND LEN(CAST(' + list_of_tables.[Column_Name] + ' AS varchar(8))) = 0)
								             THEN 1 ELSE 0 END ) AS [Count of empty/zero values]
							   , SUM(CASE WHEN CAST(UPPER(' + list_of_tables.[Column_Name] + ') AS binary)  = CAST(' + list_of_tables.[Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only UPPERCASE strings]	
							   , SUM(CASE WHEN CAST(LOWER(' + list_of_tables.[Column_Name] + ') AS binary)  = CAST(' + list_of_tables.[Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only LOWERCASE strings]	
							   , SUM(CASE WHEN ASCII(RIGHT(' + list_of_tables.[Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) OR  ASCII(LEFT(' + list_of_tables.[Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) THEN 1 ELSE 0 END)  AS [Rows with non-printable characters at the beginning/end]
							   , (SELECT TOP 1 CAST('+ list_of_tables.[Column_Name] +' AS VARCHAR) FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + ']  GROUP BY '+ list_of_tables.[Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc) AS [Most_used_value]
							   ,CAST((SELECT TOP 1 count(*) FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + ']  GROUP BY '+ list_of_tables.[Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc)*100/COUNT(*) as Varchar(8))   AS [% rows with most used value]
							   , CAST(MIN(CASE WHEN CAST('''+ list_of_tables.[Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + list_of_tables.[Column_Name] + ' , 112) ELSE ' + list_of_tables.[Column_Name] + '  END) AS VARCHAR) as [MIN value]
							   , CAST(MAX(CASE WHEN CAST('''+ list_of_tables.[Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + list_of_tables.[Column_Name] + ' , 112) ELSE ' + list_of_tables.[Column_Name] + '  END) AS VARCHAR) as [MAX value]
							  FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + '] 
								UNION ALL '
						ELSE 'SELECT   
								''' + @p_DatabaseName + ''' as [Database Name] 
							   ,''' + @p_SchemaName + ''' as [Schema name]
							   ,''' + list_of_tables.[Table_Name] + ''' AS [Table Name] 
							   ,COUNT(*) AS [Total row count]
							   ,''' + list_of_tables.[Column_Name] + ''' AS [Column Name] 
							   ,''' + list_of_tables.[Data_type] + ''' AS [Data Type] 
							   , COUNT(DISTINCT ' + list_of_tables.[Column_Name] + ') AS [Count of DISTINCT values]
							   , SUM(CASE WHEN ' + list_of_tables.[Column_Name] + ' IS NULL THEN 1 ELSE 0 END ) AS [Count of NULL values]
							   , SUM(CASE WHEN (CAST(''' + list_of_tables.[Data_type] + ''' AS varchar(8)) LIKE '+'''int'''+' AND CAST(' + list_of_tables.[Column_Name] + ' AS varchar(8)) = ''0'' )
								    OR (CAST(''' + list_of_tables.[Data_type] + ''' AS varchar(8)) LIKE '+'''%char'''+' AND LEN(CAST(' + list_of_tables.[Column_Name] + ' AS varchar(8))) = 0)
								             THEN 1 ELSE 0 END ) AS [Count of empty/zero values]
							   , SUM(CASE WHEN CAST(UPPER(' + list_of_tables.[Column_Name] + ') AS binary)  = CAST(' + list_of_tables.[Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only UPPERCASE strings]	
							   , SUM(CASE WHEN CAST(LOWER(' + list_of_tables.[Column_Name] + ') AS binary)  = CAST(' + list_of_tables.[Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only LOWERCASE strings]	
							   , SUM(CASE WHEN ASCII(RIGHT(' + list_of_tables.[Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) OR  ASCII(LEFT(' + list_of_tables.[Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) THEN 1 ELSE 0 END)  AS [Rows with non-printable characters at the beginning/end]
							   , (SELECT TOP 1 CAST('+ list_of_tables.[Column_Name] +' AS VARCHAR) FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + ']  GROUP BY '+ list_of_tables.[Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc) AS [Most_used_value]
							     ,CAST((SELECT TOP 1 count(*) FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + ']  GROUP BY '+ list_of_tables.[Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc)*100/COUNT(*) as Varchar(8)) AS [% rows with most used value]
							   , CAST(MIN(CASE WHEN CAST('''+ list_of_tables.[Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + list_of_tables.[Column_Name] + ' , 112) ELSE ' + list_of_tables.[Column_Name] + '  END) AS VARCHAR) as [MIN value]
							   , CAST(MAX(CASE WHEN CAST('''+ list_of_tables.[Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + list_of_tables.[Column_Name] + ' , 112) ELSE ' + list_of_tables.[Column_Name] + '  END) AS VARCHAR) as [MAX value]
							  FROM [' + @p_SchemaName + '].[' + list_of_tables.[Table_Name] + '] '
				END [text_of_query]
			FROM list_of_tables 
		)
	SELECT 
		@v_Query = STRING_AGG([text_of_query], '') WITHIN GROUP (ORDER BY [text_of_query])
	FROM query_list;

	EXEC SP_EXECUTESQL @v_Query;

END;
