CREATE PROCEDURE [hr].[System_info_tables_n]
(
  @p_DatabaseName AS NVARCHAR(MAX)
, @p_SchemaName AS NVARCHAR(MAX) 
, @p_TableName AS NVARCHAR(MAX))

AS
BEGIN
    DECLARE @v_Query NVARCHAR(MAX), @v_Query_db NVARCHAR(MAX)
	
   DECLARE @tabless TABLE  (Table_name  NVARCHAR(100) ,Column_name NVARCHAR(100),	Data_type NVARCHAR(100),	next_name  NVARCHAR(100)) 
	SELECT @v_Query_db = 'USE ' + @p_DatabaseName + '
		    SELECT
	  TABLE_NAME as [Table_name], COLUMN_NAME as [Column_name], DATA_TYPE as [Data_type]
				, LEAD(COLUMN_NAME ) OVER (ORDER BY COLUMN_NAME ) [next_name]
	 FROM INFORMATION_SCHEMA.COLUMNS
	  WHERE  
			   TABLE_SCHEMA =   '''+@p_SchemaName+'''
			   and TABLE_NAME like '''+ @p_TableName + ''''
		  
		insert into @tabless
			EXEC SP_EXECUTESQL @v_Query_db;
WITH query_list AS 
		(
			SELECT
				CASE 
					WHEN [next_name] IS NOT NULL 
						THEN ' SELECT   
								''' + @p_DatabaseName + ''' as [Database Name] 
							   ,''' + @p_SchemaName + ''' as [Schema name]
							   ,''' + [Table_Name] + ''' AS [Table Name] 
							   ,COUNT(*) AS [Total row count]
							   ,''' + [Column_Name] + ''' AS [Column Name] 
							   ,''' + [Data_type] + ''' AS [Data Type] 
							   , COUNT(DISTINCT ' + [Column_Name] + ') AS [Count of DISTINCT values]
							   , SUM(CASE WHEN ' + [Column_Name] + ' IS NULL THEN 1 ELSE 0 END ) AS [Count of NULL values]
							   , SUM(CASE WHEN (CAST(''' + [Data_type] + ''' AS varchar(8)) LIKE '+'''int'''+' AND CAST(' + [Column_Name] + ' AS varchar(8)) = ''0'' )
								    OR (CAST(''' + [Data_type] + ''' AS varchar(8)) LIKE '+'''%char'''+' AND LEN(CAST(' + [Column_Name] + ' AS varchar(8))) = 0)
								             THEN 1 ELSE 0 END ) AS [Count of empty/zero values]
							   , SUM(CASE WHEN CAST(UPPER(' + [Column_Name] + ') AS binary)  = CAST(' + [Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only UPPERCASE strings]	
							   , SUM(CASE WHEN CAST(LOWER(' + [Column_Name] + ') AS binary)  = CAST(' + [Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only LOWERCASE strings]	
							   , SUM(CASE WHEN ASCII(RIGHT(' + [Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) OR  ASCII(LEFT(' + [Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) THEN 1 ELSE 0 END)  AS [Rows with non-printable characters at the beginning/end]
							   , (SELECT TOP 1 CAST('+ [Column_Name] +' AS VARCHAR) FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + ']  GROUP BY '+ [Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc) AS [Most_used_value]
							   ,CAST((SELECT TOP 1 count(*) FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + ']  GROUP BY '+ [Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc)*100/COUNT(*) as Varchar(8))   AS [% rows with most used value]
							   , CAST(MIN(CASE WHEN CAST('''+ [Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + [Column_Name] + ' , 112) ELSE ' + [Column_Name] + '  END) AS VARCHAR) as [MIN value]
							   , CAST(MAX(CASE WHEN CAST('''+ [Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' +[Column_Name] + ' , 112) ELSE ' + [Column_Name] + '  END) AS VARCHAR) as [MAX value]
							  FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + '] UNION ALL '
						ELSE 'SELECT   
								''' + @p_DatabaseName + ''' as [Database Name] 
							   ,''' + @p_SchemaName + ''' as [Schema name]
							   ,''' + [Table_Name] + ''' AS [Table Name] 
							   ,COUNT(*) AS [Total row count]
							   ,''' + [Column_Name] + ''' AS [Column Name] 
							   ,''' + [Data_type] + ''' AS [Data Type] 
							   , COUNT(DISTINCT ' + [Column_Name] + ') AS [Count of DISTINCT values]
							   , SUM(CASE WHEN ' + [Column_Name] + ' IS NULL THEN 1 ELSE 0 END ) AS [Count of NULL values]
							   , SUM(CASE WHEN (CAST(''' + [Data_type] + ''' AS varchar(8)) LIKE '+'''int'''+' AND CAST(' + [Column_Name] + ' AS varchar(8)) = ''0'' )
								    OR (CAST(''' + [Data_type] + ''' AS varchar(8)) LIKE '+'''%char'''+' AND LEN(CAST(' + [Column_Name] + ' AS varchar(8))) = 0)
								             THEN 1 ELSE 0 END ) AS [Count of empty/zero values]
							   , SUM(CASE WHEN CAST(UPPER(' + [Column_Name] + ') AS binary)  = CAST(' + [Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only UPPERCASE strings]	
							   , SUM(CASE WHEN CAST(LOWER(' + [Column_Name] + ') AS binary)  = CAST(' + [Column_Name] + ' AS binary) THEN 1 ELSE 0 END)  AS [Only LOWERCASE strings]	
							   , SUM(CASE WHEN ASCII(RIGHT(' + [Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) OR  ASCII(LEFT(' + [Column_Name] + ', 1)) IN (0, 7, 9, 10, 13, 16) THEN 1 ELSE 0 END)  AS [Rows with non-printable characters at the beginning/end]
							   , (SELECT TOP 1 CAST('+ [Column_Name] +' AS VARCHAR) FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + ']  GROUP BY '+ [Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc) AS [Most_used_value]
							   ,CAST((SELECT TOP 1 count(*) FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + ']  GROUP BY '+ [Column_Name] +' having count(*)>=1
									ORDER BY count(*) desc)*100/COUNT(*) as Varchar(8))   AS [% rows with most used value]
							   , CAST(MIN(CASE WHEN CAST('''+ [Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' + [Column_Name] + ' , 112) ELSE ' + [Column_Name] + '  END) AS VARCHAR) as [MIN value]
							   , CAST(MAX(CASE WHEN CAST('''+ [Data_type] + ''' AS binary) = CAST(''date'' AS binary) THEN CONVERT(VARCHAR(8), ' +[Column_Name] + ' , 112) ELSE ' + [Column_Name] + '  END) AS VARCHAR) as [MAX value]
							  FROM [' + @p_DatabaseName + '].[' + @p_SchemaName + '].[' + [Table_Name] + '];'
				END [text_of_query]
			FROM @tabless
		)
		
	SELECT 
	@v_Query = STRING_AGG([text_of_query], '') WITHIN GROUP (ORDER BY [text_of_query] ASC)
	FROM query_list;
	EXEC SP_EXECUTESQL @v_Query;
END;
GO
