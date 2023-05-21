IF OBJECT_ID('student', 'U') IS NULL 
BEGIN
    CREATE TABLE student (
        [Id] [int] NOT NULL,
        [Name] [nvarchar](256) NOT NULL 
    )
END
GO