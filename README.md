# usp_executesql_parallel

This is a rough sample of how to to do parallel server-side TSQL execution.

The entry point looks like:

 

```SQL
exec usp_executesql_parallel @queryForBatchList= "select * from #batches", @maxDegreeOfParalellism = 4, @connectionString = null, @debug=true;
```

 
The first argument is a query that is executed in the current session, and returns the list of batches to execute, and optionally the connectionString to execute each one.  You’d normally just load the queries into a local temp table, and the proc will read them from there.
