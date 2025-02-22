using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

static class SqlConnectionExtensions
{
    public static DataTable ExecuteDataTable(this SqlConnection con, string sql, params SqlParameter[] parameters)
    {
        var cmd = new SqlCommand(sql, con);
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }
        using (var dr = cmd.ExecuteReader())
        {
            var dt = new DataTable();
            dt.Load(dr);
            return dt;
        }
    }

    public static Task<int> ExecuteNonQueryAsync(this SqlConnection con, string sql,CancellationToken cancelationToken, params SqlParameter[] parameters)
    {
        var cmd = new SqlCommand(sql, con);
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }
        return cmd.ExecuteNonQueryAsync(cancelationToken);
    }
}

class BatchRequest
{
    public enum Outcomes
    {
        Canceled,
        Completed,
        Error
    }
    public BatchRequest(String sql, int BatchNumber, string connectionString)
    {
        this.Sql = sql;
        this.BatchNumber = BatchNumber;
        this.ConnectionString = connectionString;

    }
    public int BatchNumber { get; }
    public string ConnectionString { get; }

    public List<SqlError> ErrorsAndMessages { get; } = new List<SqlError>();
    public DateTime? StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Sql { get; }
    public Task ExecutionTask { get; set; }

    public Exception Exception { get; set; }
    public Outcomes Outcome { get; internal set; }

    public override string ToString()
    {
        return $"Batch {BatchNumber}";
    }
    public string FormatMessage(string msg)
    {
        return $"[Batch {BatchNumber}] - ";
    }
}


public partial class StoredProcedures
{


    [Microsoft.SqlServer.Server.SqlProcedure( Name = "usp_executesql_parallel")]
    public static void ExecuteSqlParallel(
        string queryForBatchList, 
        int maxDegreeOfParalellism, 
        string connectionString = null, 
        bool debug = true)
    {

   
        var pendingMessages = new ConcurrentQueue<String>();

        Action SendPendingMessages = () =>
        {
            while (!pendingMessages.IsEmpty)
            {
                string msg;
                if (pendingMessages.TryDequeue(out msg))
                {
                    SqlContext.Pipe.Send(msg);
                }

            }
        };

        
        Action<string> DebugMessage = s => 
        {
            if (debug)
            {
                pendingMessages.Enqueue(s);
                SendPendingMessages();
            }
        };

        try
        {
           

            var batches = new List<BatchRequest>();
            using (var con = new SqlConnection("context connection=true"))
            {
                con.Open();

                if (string.IsNullOrEmpty(connectionString))
                {
                    var pConnectionString = new SqlParameter("@connectionString", SqlDbType.NVarChar, 200);
                    pConnectionString.Direction = ParameterDirection.Output;
                    var sql = "set @connectionString = concat('server=', cast(SERVERPROPERTY('ServerName') as nvarchar(200)),';Integrated Security=true;database=',DB_NAME());";
                    con.ExecuteNonQueryAsync(sql, CancellationToken.None, pConnectionString);
                    connectionString = (string)pConnectionString.Value;

                    DebugMessage($"Using Connection String: {connectionString}");
                }

                var dt = con.ExecuteDataTable(queryForBatchList);
                int bn = 0;
                foreach (DataRow r in dt.Rows)
                {
                    var batch = r[0] as string;
                    if (batch == null)
                    {
                        throw new InvalidOperationException("QueryForBatchList return null batch or first column was not a string");
                    }
                    var constr = connectionString;
                    if (dt.Columns.Count > 1 && dt.Columns[1].DataType == typeof(string))
                    {
                        constr = (string)r[1];
                    }
                    batches.Add(new BatchRequest(batch, bn++, connectionString));
                }


            }

            var cancelationTokenSource = new CancellationTokenSource();

            var batchesToRun = new Queue<BatchRequest>(batches);
            var runningBatches = new List<BatchRequest>(maxDegreeOfParalellism);
            var completedBatches = new List<BatchRequest>(batches.Count);
            

            while (batchesToRun.Count > 0 || runningBatches.Count > 0)
            {
                while (runningBatches.Count < runningBatches.Capacity)
                {
                    var batch = batchesToRun.Dequeue();

                    DebugMessage($"-------Starting Batch {batch.BatchNumber} --------");
                    DebugMessage(batch.Sql);
                    DebugMessage($"-------End Batch {batch.BatchNumber} Listing --------");

                    batch.ExecutionTask = RunBatch(batch, cancelationTokenSource.Token, s => pendingMessages.Enqueue(s));
                    
                    runningBatches.Add(batch);
                    SendPendingMessages();
                }

                var tasks = runningBatches.Select(b => b.ExecutionTask).ToArray();

                int i = Task.WaitAny(tasks, 1000);
                SendPendingMessages();

                if (i > -1)
                {
                    var completedBatch = runningBatches[i];
                    runningBatches.Remove(completedBatch);
                    completedBatches.Add(completedBatch);

                    if (completedBatch.Outcome == BatchRequest.Outcomes.Error || completedBatch.ExecutionTask.IsFaulted)
                    {
                        var ex = completedBatch.Exception ?? completedBatch.ExecutionTask.Exception;

                        DebugMessage($"Batch {completedBatch} Failed: {ex.Message}");

                        DebugMessage($"-----------Failed batch Output--------------");
                        foreach (var s in completedBatch.ErrorsAndMessages)
                        {
                            DebugMessage($"Error {s.Message} Line {s.LineNumber} Severity {s.Class}");
                        }
                        DebugMessage($"-----------Failed batch Output--------------");

                        cancelationTokenSource.Cancel();
                        while (! Task.WaitAll(tasks,5000) )
                        {
                            SendPendingMessages();
                            DebugMessage("Waiting for completion of canceled batches");
                        }

                        SendPendingMessages();
                        throw ex;
                    }
                    else if (completedBatch.Outcome == BatchRequest.Outcomes.Canceled)
                    {
                        DebugMessage($"{completedBatch} Canceled. Duration {completedBatch.EndTime?.Subtract(completedBatch.StartTime.Value).Seconds} sec");
                    }
                    else
                    {
                        DebugMessage($"{completedBatch} Completed. Duration {completedBatch.EndTime?.Subtract(completedBatch.StartTime.Value).Seconds} sec");
                    }
                }


            }
            SendPendingMessages();


        }
        catch(Exception ex)
        {
            SendPendingMessages();
            SqlContext.Pipe.Send(ex.ToString());
            
            throw;
        }

    }

    static async Task RunBatch( BatchRequest batch, CancellationToken cancellationToken, Action<string> onMessage)
    {
        
            
        using (var con = new SqlConnection(batch.ConnectionString))
        {

            con.InfoMessage += (s, a) =>
            {
                foreach (SqlError e in a.Errors)
                {
                    batch.ErrorsAndMessages.Add(e);
                    onMessage($"Batch {batch.BatchNumber} InfoMessage: {e.Message}");
                }
                
            };
            await con.OpenAsync();

            batch.StartTime = DateTime.Now;
            var execTask = con.ExecuteNonQueryAsync(batch.Sql, cancellationToken);

            try
            {
                await execTask;
            }
            catch (SqlException ex)
            {
                batch.ErrorsAndMessages.AddRange(ex.Errors.Cast<SqlError>());
                batch.Outcome = BatchRequest.Outcomes.Error;

            }
            

            if (execTask.IsCanceled)
            {
                batch.Outcome = BatchRequest.Outcomes.Canceled;
            }
            else if (execTask.IsFaulted)
            {
                batch.Outcome = BatchRequest.Outcomes.Error;
                batch.Exception = execTask.Exception;
                if (batch.Exception is SqlException sqlEx)
                {
                    batch.ErrorsAndMessages.AddRange(sqlEx.Errors.Cast<SqlError>());
                }
            }
            else
            {
                batch.Outcome = BatchRequest.Outcomes.Completed;
            }
            batch.EndTime = DateTime.Now;
            con.Close();
        }
    }

}
