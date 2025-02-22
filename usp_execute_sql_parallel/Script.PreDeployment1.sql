/*
 Pre-Deployment Script Template                            
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be executed before the build script.    
 Use SQLCMD syntax to include a file in the pre-deployment script.            
 Example:      :r .\myfile.sql                                
 Use SQLCMD syntax to reference a variable in the pre-deployment script.        
 Example:      :setvar TableName MyTable                            
               SELECT * FROM [$(TableName)]                    
--------------------------------------------------------------------------------------
*/

--ignore for pre-SQL 2016
if cast(serverproperty('ProductMajorVersion') as int) < 14 return;


DECLARE @asmBin varbinary(max) = (
		SELECT BulkColumn 
		FROM OPENROWSET (BULK '$(assemblyLocation)', SINGLE_BLOB) a
		);

DECLARE @hash varbinary(64);
 
SELECT @hash = HASHBYTES('SHA2_512', @asmBin);


declare @description nvarchar(4000) = N'usp_execute_sql_parallel';

if not exists (select * from sys.trusted_assemblies where hash = @hash)
begin
  EXEC sys.sp_add_trusted_assembly @hash = @hash,
                                 @description = @description;
end
