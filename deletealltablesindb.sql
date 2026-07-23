DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql +=
    'DROP TABLE [' + s.name + '].[' + t.name + '];' + CHAR(13)
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
WHERE s.name = 'dbo';

EXEC sp_executesql @sql;