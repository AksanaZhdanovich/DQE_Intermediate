WITH
	json_string AS
	(
		SELECT '[{"employee_id": "5181816516151", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "925155", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "815153", "department_id": "2", "class": "src\bin\comp\json"}, {"employee_id": "967", "department_id": "", "class": "src\bin\comp\json"}]' [str]
	)
	, parsing as(
		SELECT 
			 substring([str],CHARINDEX('{', [str]),CHARINDEX('}', [str]) - 1) [string_item]
			,substring([str],CHARINDEX('}', [str]) + 3, len([str])) [rest_string]
		FROM   json_string
		UNION ALL
		SELECT 
	  		 substring([rest_string],CHARINDEX('{', [rest_string]),CHARINDEX('}', [rest_string])) [string_item]
			,substring([rest_string],CHARINDEX('}', [rest_string]) + 3, len([rest_string])) [rest_string]
		FROM parsing
		WHERE LEN([rest_string]) > 0)
	, parsed_data AS (
		SELECT 
			 CAST(substring([string_item], 
									CHARINDEX('employee_id', [string_item]) + 15, 
									CHARINDEX(', ', [string_item], CHARINDEX('employee_id', [string_item])) - CHARINDEX('employee_id', [string_item]) - 16) AS BIGINT) AS [employee_id] ,
			 CAST(
					CASE WHEN substring([string_item], CHARINDEX('department_id', [string_item]) + 17, 
									CHARINDEX(', ', [string_item], CHARINDEX('department_id', [string_item])) - CHARINDEX('department_id', [string_item]) - 18) = ''
					THEN  NULL 
					ELSE substring([string_item], CHARINDEX('department_id', [string_item]) + 17, 
									CHARINDEX(', ', [string_item], CHARINDEX('department_id', [string_item])) - CHARINDEX('department_id', [string_item]) - 18) END AS INT) AS [department_id] 
		FROM  parsing)
	SELECT 
	     [employee_id] 
		,[department_id] 
	FROM parsed_data;