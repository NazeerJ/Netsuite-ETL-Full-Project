    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'netsuite')
    BEGIN
        CREATE DATABASE netsuite;
    END