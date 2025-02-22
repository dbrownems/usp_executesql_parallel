drop table if exists ##t

create table ##t(id int identity, a varchar(200))

declare @sql nvarchar(max) = '
declare @s varchar(20) = concat(''Beginning session'',@@spid);
print @s;

insert into ##t(a) values (''hello'');

waitfor delay ''0:0:01'';

set @s = concat(''finished session '',@@spid);
';

--exec (@sql)



select top 20 @sql sqlBatch into #batches from sys.objects

declare @constr nvarchar(max) = N'Data Source=(localdb)\ProjectsV13;Initial Catalog=usp_execute_sql_parallel;Integrated Security=True;Pooling=False;Connect Timeout=30';

exec usp_executesql_parallel @queryForBatchList= "select * from #batches", @maxDegreeOfParalellism = 1, @connectionString = @constr, @debug=true;

select * from ##t

drop table ##t
drop table #batches




