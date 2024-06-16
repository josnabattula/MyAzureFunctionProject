using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BatchFunctions
{
    public class BatchFunctions
    {
        private readonly ILogger<BatchFunctions> _logger;
        private static string debug;

        public BatchFunctions(ILogger<BatchFunctions> logger)
        {
            _logger = logger;
        }
        private static string debug_here()
        {
            var frame = new System.Diagnostics.StackFrame(1, true);
            return $"Debug from line number {frame.GetFileLineNumber()}";
        }

        // private static string debug = debug_here();

        private SqlConnection GetEngine(string serverName, string databaseName, string userId)
        {
            string connectionString = $"DRIVER=ODBC Driver 18 for SQL Server;  SERVER={serverName};  DATABASE={databaseName};  UID={userId}; Authentication=ActiveDirectoryMsi; timeout=300; Encrypt=yes; TrustServerCertificate=yes;";
            _logger.LogInformation(connectionString);
            var connectionUrl = new SqlConnectionStringBuilder(connectionString);

            try
            {
                using var engine = new SqlConnection(connectionUrl.ConnectionString);
                engine.Open();
                _logger.LogInformation("Engine connection successful");
                return engine;
            }
            catch (Exception e)
            {
                throw new Exception($"FAIL: Failed to make connection to SQL Server: {e.Message}", e);
            }
        }

        private string RefreshToken(string url, string clientId, string clientSecret, string tenantId)
        {
            string tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/token";
            var tokenData = new Dictionary<string, string>
            {
                { "grant_type", "client_credentials" },
                { "client_id", clientId },
                { "client_secret", clientSecret },
                { "resource", url }
            };

            using (var client = new HttpClient())
            {
                var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint)
                {
                    Content = new FormUrlEncodedContent(tokenData)
                };

                var response = client.SendAsync(request).Result;
                response.EnsureSuccessStatusCode();

                var tokenResponse = response.Content.ReadAsStringAsync().Result;
                var token = JObject.Parse(tokenResponse)["access_token"].ToString();
                return token;
            }
        }

        private (HttpResponseMessage Response, string KeyColVal, string StatusCodeDescription) BatchUpdateEntityWithExceptionHandling(string updateUrl, object updatePayload, Dictionary<string, string> updateHeaders, string keyColVal)
        {
            try
            {
                return BatchUpdateEntity(updateUrl, updatePayload, updateHeaders, keyColVal);
            }
            catch (Exception e)
            {
                _logger.LogError($"An error occurred while processing batch update with arguments {updateUrl}, {updatePayload}, {string.Join(", ", updateHeaders)}, {keyColVal}: {e}   batch_update_entity_with_exception_handling");
                return (null, null, $"FAIL: An error occurred while processing batch update with arguments {updateUrl}, {updatePayload}, {string.Join(", ", updateHeaders)}, {keyColVal}: {e.Message}");
            }
        }

        private (HttpResponseMessage Response, string KeyColVal, string StatusCodeDescription) BatchUpdateEntity(string updateUrl, object updatePayload, Dictionary<string, string> updateHeaders, string keyColVal)
        {
            _logger.LogInformation(updateUrl);

            try
            {
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Clear();
                    foreach (var header in updateHeaders)
                    {
                        client.DefaultRequestHeaders.Add(header.Key, header.Value);
                    }

                    var response = client.PatchAsync(updateUrl, new StringContent(JsonConvert.SerializeObject(updatePayload), System.Text.Encoding.UTF8, "application/json")).Result;
                    _logger.LogInformation(response.ToString());
                    var statusCodeDescription = response.StatusCode.ToString();
                    return (response, keyColVal, statusCodeDescription);
                }
            }
            catch (Exception e)
            {
                return (null, keyColVal, $"FAIL: Update failed with error message {e.Message} for {keyColVal} batch_update_entity");
            }
        }

        private string BatchMultiselectUpdate(
            string entityName, int grpno, string tableName, string schemaName, string keycol, string selectsql, int threads, int batchsize, string jobId, string jocExeLogId, string apiVersion, string clientId, string clientSecret, string databaseName, string serverName, string tenantId, string url, string userId)
        {
            debug = "Started";
            int errorCount = 0;

            try
            {
                string batchGuid = Guid.NewGuid().ToString();
                using var engineConn = GetEngine(serverName, databaseName, userId);
                var metadata = new MetaData();
                metadata.Bind = engineConn;
                var updateLogTable = new Table("MultiselectColErrorLog", metadata, "ETL_Framework", engineConn);
                var deleteStatement = Queryable.Delete(updateLogTable).Where(row => row.Field<string>("Job_ID") == jobId && row.Field<string>("JocExeLogId") == jocExeLogId && row.Field<int>("ETL_Group_Number") == grpno);

                using (var connection = engineConn.Open())
                {
                    connection.Execute(deleteStatement);
                }

                var df = engineConn.ReadSql(selectsql);
                engineConn.Dispose();

                if (df.Rows.Count > 0)
                {
                    var updateHeaders = new Dictionary<string, string>
                        {
                            { "Authorization", $"Bearer {RefreshToken(url, clientId, clientSecret, tenantId)}" },
                            { "Content-Type", "application/json" },
                            { "OData-MaxVersion", "4.0" },
                            { "OData-Version", "4.0" },
                            { "Accept", "application/json" }
                        };

                    int totalRecordsUpdatedCounter = 0;
                    var batchUpdates = new List<(string UpdateUrl, object UpdatePayload, Dictionary<string, string> UpdateHeaders, string KeyColVal)>();
                    int batchUpdateSize = threads;
                    int updateHeadersCount = 0;

                    debug = "AT For Loop";

                    foreach (var row in df.Rows)
                    {
                        var idDict = new Dictionary<string, object> { { keycol, row[keycol] } };
                        var otherDict = row.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                        otherDict.Remove(keycol);

                        var updatePayload = otherDict;
                        string updateUrl = $"{url}/api/data/v{apiVersion}/{entityName}({idDict[keycol]})";
                        batchUpdates.Add((updateUrl, updatePayload, updateHeaders, idDict[keycol].ToString()));

                        totalRecordsUpdatedCounter++;
                        updateHeadersCount++;
                        _logger.LogInformation($"{batchUpdates.Count}  :  {batchUpdateSize}");

                        debug = "AT IF";

                        if (batchUpdates.Count >= batchUpdateSize || row.RowIndex == df.Rows.Count - 1)
                        {
                            Parallel.ForEach(batchUpdates, batchUpdate =>
                            {
                                var (response, keyColValue, responseMsg) = BatchUpdateEntityWithExceptionHandling(batchUpdate.UpdateUrl, batchUpdate.UpdatePayload, batchUpdate.UpdateHeaders, batchUpdate.KeyColVal);
                                _logger.LogInformation(responseMsg);

                                debug = "AT 2nd If";

                                if (response == null || (!new[] { 200, 201, 202, 204 }.Contains((int)response.StatusCode) && !new[] { "", "No Content", " Created", "OK", "Accepted" }.Contains(response.Content.ReadAsStringAsync().Result.Trim())))
                                {
                                    using (var connection = engineConn.Open())
                                    {
                                        var insertStatement = Queryable.Insert(updateLogTable, new
                                        {
                                            entity_name = entityName,
                                            keycol_name = keycol,
                                            keycol_val = keyColValue,
                                            response_code = response?.StatusCode.ToString(),
                                            response_text = response?.Content.ReadAsStringAsync().Result,
                                            response_result = responseMsg,
                                            Job_ID = jobId,
                                            JocExeLogId = jocExeLogId,
                                            BatchGuid = batchGuid,
                                            ETL_Group_Number = grpno
                                        });

                                        connection.Execute(insertStatement);
                                        connection.Commit();
                                    }
                                }
                            });

                            batchUpdates.Clear();
                        }

                        debug = "AT 3Rd If";

                        if (updateHeadersCount > batchsize)
                        {
                            _logger.LogInformation($"Successfully updated {totalRecordsUpdatedCounter} out of {df.Rows.Count} records");
                            _logger.LogInformation(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                            updateHeaders["Authorization"] = $"Bearer {RefreshToken(url, clientId, clientSecret, tenantId)}";
                            updateHeadersCount = 0;
                        }
                    }

                    using (var connection = engineConn.Open())
                    {
                        var errorResult = connection.ExecuteScalar<int>($"SELECT COUNT(*) FROM ETL_Framework.MultiselectColErrorLog WHERE BatchGuid = '{batchGuid}'");
                        errorCount = errorResult;
                    }

                    engineConn.Dispose();

                    if (errorCount > 0)
                    {
                        errorCount = 0;
                        return $"FAIL : {errorCount} Recorsd failed to insert form {grpno} failed to Insert";
                    }
                    else
                    {
                        return $"Successfully updated {totalRecordsUpdatedCounter} out of {df.Rows.Count} records.";
                    }
                }
                else
                {
                    debug = debug_here();
                    engineConn.Dispose();
                    return $"No records found to update {df.Rows.Count} records";
                }
            }
            catch (Exception e)
            {
                _logger.LogInformation($"Insert failed with error message {e.Message}     {debug}");
                return $"FAIL : Insert failed with error message {e.Message} **{debug}**";
            }
        }

        private static (HttpResponseMessage Response, string Entity1Val, string Entity2Val, string StatusCodeDescription) BatchAddNtoNRowWithExceptionHandling(string addUrl, object addPayload, Dictionary<string, string> addHeaders, string entity1Val, string entity2Val)
        {
            try
            {
                return BatchAddNtoNRow(addUrl, addPayload, addHeaders, entity1Val, entity2Val);
            }
            catch (Exception e)
            {
                return (null, entity1Val, entity2Val, $"FAIL : An error occurred while processing batch update with arguments {addUrl}, {addPayload}, {string.Join(", ", addHeaders)}, {entity1Val}, {entity2Val}: {e.Message} Batch_Add_NtoN_Row_with_exception_handling");
            }
        }

        private static (HttpResponseMessage Response, string Entity1Val, string Entity2Val, string StatusCodeDescription) BatchAddNtoNRow(string addUrl, object addPayload, Dictionary<string, string> addHeaders, string entity1Val, string entity2Val)
        {
            _logger.LogInformation(addUrl);

            try
            {
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Clear();
                    foreach (var header in addHeaders)
                    {
                        client.DefaultRequestHeaders.Add(header.Key, header.Value);
                    }

                    var response = client.PostAsync(addUrl, new StringContent(JsonConvert.SerializeObject(addPayload), System.Text.Encoding.UTF8, "application/json")).Result;
                    var statusCodeDescription = response.StatusCode.ToString();
                    return (response, entity1Val, entity2Val, statusCodeDescription);
                }
            }
            catch (Exception e)
            {
                return (null, entity1Val, entity2Val, $"FAIL : Insert failed with error message {e.Message} Batch_Add_NtoN_Row");
            }
        }

        private static string BatchAddNtoNEntity(
            string srcSchName, string srcTblName, string trgTblName, string targetTableNNSchemaName, string parentTable1CollectionName, string parentTable2CollectionName, string col1Name, string col2Name, int batchsize, int threads, int grpno, string jobId, string jocExeLogId, string apiVersion, string clientId, string clientSecret, string databaseName, string serverName, string tenantId, string url, string userId)
        {
            debug = debug_here();
            string batchGuid = Guid.NewGuid().ToString();
            int errorCount = 0;

            try
            {
                using (var engineConn = GetEngine(serverName, databaseName, userId))
                {
                    var metadata = new MetaData();
                    metadata.Bind = engineConn;
                    var updateLogTable = new Table("ManyToManyErrorLog", metadata, "ETL_Framework", engineConn);
                    var deleteStatement = Queryable.Delete(updateLogTable).Where(row => row.Field<string>("Job_ID") == jobId && row.Field<string>("JocExeLogId") == jocExeLogId && row.Field<int>("ETL_Group_Number") == grpno);

                    debug = debug_here();

                    using (var connection = engineConn.Open())
                    {
                        connection.Execute(deleteStatement);
                    }

                    string sqlQuery = $"SELECT [{col1Name}], [{col2Name}] FROM [{srcSchName}].[{srcTblName}] WHERE NOT ([{col1Name}] IS NULL AND [{col2Name}] IS NULL) AND [ETL_Group_Number] = {grpno}";
                    var df = engineConn.ReadSql(sqlQuery);

                    debug = debug_here();

                    if (df.Rows.Count > 0)
                    {
                        var addHeaders = new Dictionary<string, string>
                        {
                            { "Authorization", $"Bearer {RefreshToken(url, clientId, clientSecret, tenantId)}" },
                            { "Content-Type", "application/json" },
                            { "OData-MaxVersion", "4.0" },
                            { "OData-Version", "4.0" },
                            { "Accept", "application/json" }
                        };

                        int totalRecordsUpdatedCounter = 0;
                        var batchUpdates = new List<(string AddUrl, object AddPayload, Dictionary<string, string> AddHeaders, string Entity1Val, string Entity2Val)>();
                        int batchUpdateSize = threads;
                        int updateHeadersCount = 0;

                        debug = debug_here();

                        foreach (var row in df.Rows)
                        {
                            var ent1Dict = new Dictionary<string, object> { { col1Name, row[col1Name] } };
                            var ent2Dict = new Dictionary<string, object> { { col2Name, row[col[col2Name] } };

                            string addUrl = $"{url}api/data/v{apiVersion}/{parentTable1CollectionName}({ent1Dict[col1Name]})/{targetTableNNSchemaName}/$ref";
                            string addPayload = $"{{\"@odata.id\": \"{url}api/data/v9.2/{parentTable2CollectionName}({ent2Dict[col2Name]})\"}}";

                            _logger.LogInformation(addUrl);
                            _logger.LogInformation(addPayload);

                            batchUpdates.Add((addUrl, addPayload, addHeaders, ent1Dict[col1Name].ToString(), ent2Dict[col2Name].ToString()));
                            totalRecordsUpdatedCounter++;
                            updateHeadersCount++;

                            _logger.LogInformation($"{batchUpdates.Count}  :  {batchUpdateSize}");
                            debug = debug_here();

                            if (batchUpdates.Count >= batchUpdateSize || row.RowIndex == df.Rows.Count - 1)
                            {
                                debug = debug_here();

                                Parallel.ForEach(batchUpdates, batchUpdate =>
                                {
                                    var (response, keyCol1Value, keyCol2Value, responseMsg) = BatchAddNtoNRowWithExceptionHandling(batchUpdate.AddUrl, batchUpdate.AddPayload, batchUpdate.AddHeaders, batchUpdate.Entity1Val, batchUpdate.Entity2Val);
                                    _logger.LogInformation(responseMsg);

                                    debug = debug_here();

                                    if (response == null || (!new[] { 200, 201, 202, 204 }.Contains((int)response.StatusCode) && !new[] { "", "No Content", " Created", "OK", "Accepted" }.Contains(response.Content.ReadAsStringAsync().Result.Trim())))
                                    {
                                        using (var connection = engineConn.Open())
                                        {
                                            var insertStatement = Queryable.Insert(updateLogTable, new
                                            {
                                                Job_ID = jobId,
                                                JocExeLogId = jocExeLogId,
                                                ETL_Group_Number = grpno,
                                                BatchGuid = batchGuid,
                                                SrcSchName = srcSchName,
                                                SrcTblName = srcTblName,
                                                TrgTblName = targetTableNNSchemaName,
                                                keycol1_name = col1Name,
                                                keycol1_val = keyCol1Value,
                                                keycol2_name = col2Name,
                                                keycol2_val = keyCol2Value,
                                                response_code = response?.StatusCode.ToString(),
                                                response_text = response?.Content.ReadAsStringAsync().Result,
                                                xecuton_result = "Record Failed"
                                            });

                                            connection.Execute(insertStatement);
                                            connection.Commit();
                                        }
                                    }
                                });

                                batchUpdates.Clear();
                            }

                            debug = debug_here();

                            if (updateHeadersCount > batchsize)
                            {
                                log.LogInformation($"Successfully updated {totalRecordsUpdatedCounter} out of {df.Rows.Count} records");
                                log.LogInformation(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                                addHeaders["Authorization"] = $"Bearer {RefreshToken(url, clientId, clientSecret, tenantId)}";
                                updateHeadersCount = 0;
                            }
                        }

                        using (var connection = engineConn.Open())
                        {
                            var errorResult = connection.ExecuteScalar<int>($"SELECT COUNT(*) FROM ETL_Framework.ManyToManyErrorLog WHERE BatchGuid = '{batchGuid}'");
                            errorCount = errorResult;
                        }

                        engineConn.Dispose();

                        if (errorCount > 0)
                        {
                            errorCount = 0;
                            return $"FAIL : {errorCount} Records failed to insert from {grpno} failed to Insert";
                        }
                        else
                        {
                            return $"Successfully updated {totalRecordsUpdatedCounter} out of {df.Rows.Count} records.";
                        }
                    }
                    else
                    {
                        debug = debug_here();
                        engineConn.Dispose();
                        return $"No records found to update {df.Rows.Count} records";
                    }
                }
            }
            catch (Exception e)
            {
                log.LogInformation($"Insert failed with error message {e.Message}     {debug}");
                return $"FAIL : Insert failed with error message {e.Message} **{debug}**";
            }
        }

        private  (List<Dictionary<string, object>> PageData, string NextLink) FetchData(string query, Dictionary<string, string> getHeaders)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Clear();
                foreach (var header in getHeaders)
                {
                    client.DefaultRequestHeaders.Add(header.Key, header.Value);
                }

                var response = client.GetAsync(query).Result;
                response.EnsureSuccessStatusCode();

                var responseJson = response.Content.ReadAsStringAsync().Result;
                var responseObject = JObject.Parse(responseJson);

                var pageData = responseObject["value"].ToObject<List<Dictionary<string, object>>>();
                var nextLink = responseObject["@odata.nextLink"]?.ToString();

                return (pageData, nextLink);
            }
        }

        private  string BatchExtractMultiselect(
            string entityName, string tableName, string schemaName, string keycol, string selectedColumns, string nexturl, int runs, int threads, string apiVersion, string clientId, string clientSecret, string databaseName, string serverName, string tenantId, string url, string userId)
        {
            debug = debug_here();
            string baseQuery = nexturl == "Start" ? "" : nexturl;
            int runscount = 0;

            try
            {
                debug = debug_here();
                using (var engineConn = GetEngine(serverName, databaseName, userId))
                {
                    var conn = engineConn.Open();
                    var cursor = conn.CreateCommand();
                    var columns = selectedColumns.Split(',').ToList();
                    debug = debug_here();

                    var columnTypes = string.Join(", ", columns.Select(col => $"[{col}] nvarchar(850)"));
                    debug = debug_here();

                    var addColumnTypes = string.Join(", ", columns.Select(col => $"[{col}] nvarchar(850)"));
                    var updateString = string.Join(", ", columns.Select(col => $"[target].[{col}] = [source].[{col}]"));
                    var msCols = string.Join(", ", columns);
                    var filterQuery = string.Join(" or ", columns.Select(col => $"{col} ne null"));

                    log.LogInformation(string.Join(", ", columns));
                    debug = debug_here();

                    if (nexturl == "Start")
                    {
                        baseQuery = $"{url}api/data/v{apiVersion}/{entityName}?$select={msCols}&$filter={filterQuery}";
                    }

                    log.LogInformation(baseQuery);
                    var dataList = new List<Dictionary<string, object>>();
                    var apiUrl = $"{url}api/data/v{apiVersion}/";
                    var headers = new Dictionary<string, string>
                    {
                        { "Authorization", $"Bearer {RefreshToken(url, clientId, clientSecret, tenantId)}" },
                        { "Content-Type", "application/json" },
                        { "OData-MaxVersion", "4.0" },
                        { "OData-Version", "4.0" },
                        { "Accept", "application/json" }
                    };

                    debug = debug_here();

                    using (var executor = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = threads })
                    {
                        var futures = new List<Task<(List<Dictionary<string, object>> PageData, string NextLink)>>();

                        while (true)
                        {
                            futures.Add(Task.Run(() => FetchData(baseQuery, headers)));
                            var (responseData, nextLink) = futures.Last().Result;

                            debug = debug_here();

                            if (responseData == null || responseData.Count == 0)
                            {
                                break;
                            }

                            dataList.AddRange(responseData);
                            log.LogInformation($"Got data for run {runscount}");
                            debug = debug_here();

                            if (string.IsNullOrEmpty(nextLink))
                            {
                                baseQuery = "All pages read";
                                break;
                            }

                            baseQuery = nextLink;
                            log.LogInformation(baseQuery);
                            runscount++;

                            if (runs == runscount)
                            {
                                log.LogInformation($"Completed {runscount} RUNS");
                                break;
                            }
                        }
                    }

                    log.LogInformation(dataList.Take(10).ToString());
                    debug = debug_here();

                    columns.Insert(0, keycol);
                    var df = new DataTable();
                    foreach (var column in columns)
                    {
                        df.Columns.Add(column);
                    }

                    foreach (var data in dataList)
                    {
                        var row = df.NewRow();
                        foreach (var column in columns)
                        {
                            row[column] = data.ContainsKey(column) ? data[column] : DBNull.Value;
                        }
                        df.Rows.Add(row);
                    }

                    log.LogInformation($"{df.Rows.Count} : Records in dataframe");

                    string tempTable = $"{tableName}_TEMP";
                    debug = debug_here();
                    log.LogInformation(addColumnTypes);

                    string addColumnsSql = $"ALTER TABLE [{schemaName}].[{tableName}] ADD {addColumnTypes};";

                    try
                    {
                        debug = debug_here();

                        if (nexturl == "Start")
                        {
                            cursor.CommandText = addColumnsSql;
                            cursor.ExecuteNonQuery();
                            conn.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        log.LogInformation($"Error at {debug}  {addColumnsSql} with {e.Message}");
                    }

                    if (nexturl == "Start")
                    {
                        try
                        {
                            debug = debug_here();
                            string dropTempTableSql = $"DROP TABLE IF EXISTS [{schemaName}].[{tempTable}];";
                            log.LogInformation(dropTempTableSql);
                            debug = "create_temp_table_sql";

                            string createTempTableSql = $@"
                                CREATE TABLE [{schemaName}].[{tempTable}] (
                                    [{keycol}] Uniqueidentifier Primary Key not null,
                                    {columnTypes}
                                )";

                            log.LogInformation(createTempTableSql);
                            debug = debug_here();

                            cursor.CommandText = dropTempTableSql;
                            cursor.ExecuteNonQuery();
                            conn.Commit();
                            log.LogInformation($"TABLE DROPPED: {dropTempTableSql}");

                            debug = debug_here();
                            cursor.CommandText = createTempTableSql;
                            cursor.ExecuteNonQuery();
                            conn.Commit();
                            log.LogInformation($"TABLE CREATED: {createTempTableSql}");
                        }
                        catch (Exception e)
                        {
                            log.LogInformation($"Error at {debug}  with error: {e.Message}");
                            return $"FAIL :Error at {debug}  with error  {e.Message}";
                        }
                    }

                    log.LogInformation("Inserting data in SQL table");

                    using (var bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = $"[{schemaName}].[{tempTable}]";
                        bulkCopy.WriteToServer(df);
                    }

                    log.LogInformation("Completed Inserting data in SQL table");
                    log.LogInformation($"[{schemaName}].[{tableName}] data loaded");

                    try
                    {
                        debug = debug_here();

                        if (baseQuery == "All pages read")
                        {
                            debug = debug_here();

                            string mergeSql = $@"
                                MERGE INTO [{schemaName}].[{tableName}] AS target
                                USING [{schemaName}].[{tempTable}] AS source
                                ON target.[{keycol}] = source.[{keycol}]
                                WHEN MATCHED THEN
                                    UPDATE SET {updateString};";

                            cursor.CommandText = mergeSql;
                            cursor.ExecuteNonQuery();
                            conn.Commit();
                        }

                        debug = debug_here();

                        string updateNextUrl = $"UPDATE [ETL_Framework].[MultiselectNextUrl] SET NextUrl = '{baseQuery}' WHERE [Schema_name] = '{schemaName}' AND [Table_name] = '{tableName}';";
                        cursor.CommandText = updateNextUrl;
                        cursor.ExecuteNonQuery();
                        conn.Commit();
                        engineConn.Dispose();
                    }
                    catch (Exception e)
                    {
                        log.LogInformation($"Error at {debug}  with error: {e.Message}");
                        return $"FAIL :Error at {debug}   with error  {e.Message}";
                    }

                    log.LogInformation($"[{schemaName}].[{tempTable}] data updated");
                    log.LogInformation(baseQuery);

                    if (df.Rows.Count > 0)
                    {
                        return baseQuery;
                    }
                    else
                    {
                        return $"No records found to fetch {df.Rows.Count} records";
                    }
                }
            }
            catch (Exception e)
            {
                return $"FAIL : Fetch failed with error message {e.Message}  {debug}";
            }
        }

        [FunctionName("batch_fetch_multiselect_func")]
        public static async Task<HttpResponseMessage> FuncBatchFetchMultiselect(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "batch_fetch_multiselect")] HttpRequestMessage req,
            ILogger log)
        {
            try
            {
                var reqBody = await req.Content.ReadAsAsync<JObject>();

                if (reqBody == null || !reqBody.HasValues)
                {
                    return req.CreateResponse(HttpStatusCode.BadRequest, "Empty body passed");
                }

                var entityName = reqBody["entity_name"]?.ToString();
                if (entityName == "NoJson")
                {
                    return req.CreateResponse(HttpStatusCode.OK, "All pages read");
                }

                var selectedColumns = reqBody["selected_columns"]?.ToString();
                var tableName = reqBody["table_name"]?.ToString();
                var keycol = reqBody["keycol"]?.ToString();
                var schemaName = reqBody["schema_name"]?.ToString();
                var nexturl = reqBody["Nexturl"]?.ToString();
                var runs = int.Parse(reqBody["runs"]?.ToString() ?? "0");
                var threads = int.Parse(reqBody["threads"]?.ToString() ?? "0");
                var apiVersion = reqBody["api_version"]?.ToString();
                var clientId = reqBody["client_id"]?.ToString();
                var clientSecret = reqBody["client_secret"]?.ToString();
                var databaseName = reqBody["database_name"]?.ToString();
                var serverName = reqBody["server_name"]?.ToString();
                var tenantId = reqBody["tenant_id"]?.ToString();
                var url = reqBody["url"]?.ToString();
                var userId = reqBody["UserId"]?.ToString();
                var environment = reqBody["Environment"]?.ToString();

                var args = new object[]
                {
                    entityName, tableName, schemaName, keycol, selectedColumns, nexturl, runs, threads, apiVersion, clientId, clientSecret, databaseName, serverName, tenantId, url, userId
                };

                var result = BatchExtractMultiselect((string)args[0], (string)args[1], (string)args[2], (string)args[3], (string)args[4], (string)args[5], (int)args[6], (int)args[7], (string)args[8], (string)args[9], (string)args[10], (string)args[11], (string)args[12], (string)args[13], (string)args[14], (string)args[15]);

                log.LogInformation(result);

                if (result.StartsWith("FAIL"))
                {
                    log.LogError($"An error occurred: {result}");
                    return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {result}");
                }

                log.LogInformation("FunctionRun Completed");
                return req.CreateResponse(HttpStatusCode.OK, result);
            }
            catch (Exception e)
            {
                log.LogError($"An error occurred: {e.Message}");
                return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {e.Message}");
            }
        }

        // [FunctionName("batch_update_multiselect_func")]
        // public static async Task<HttpResponseMessage> FuncBatchUpdateMultiselect(
        //     [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "batch_update_multiselect")] HttpRequestMessage req,
        //     ILogger log)
        // {
        //     try
        //     {
        //         var reqBody = await req.Content.ReadAsAsync<JObject>();

        //         if (reqBody == null || !reqBody.HasValues)
        //         {
        //             return req.CreateResponse(HttpStatusCode.BadRequest, "Empty body passed");
        //         }

        //         var entityName = reqBody["entity_name"]?.ToString();
        //         if (entityName == "NoJson")
        //         {
        //             return req.CreateResponse(HttpStatusCode.OK, "No Mutiselect Columns to Update");
        //         }

        //         var grpno = int.Parse(reqBody["GRPNO"]?.ToString() ?? "0");
        //         var tableName = reqBody["table_name"]?.ToString();
        //         var keycol = reqBody["keycol"]?.ToString();
        //         var schemaName = reqBody["schema_name"]?.ToString();
        //         var selectsql = reqBody["selectsql"]?.ToString();
        //         var threads = int.Parse(reqBody["threads"]?.ToString() ?? "0");
        //         var batchsize = int.Parse(reqBody["batchsize"]?.ToString() ?? "0");
        //         var jobId = reqBody["Job_ID"]?.ToString();
        //         var jocExeLogId = reqBody["JocExeLogId"]?.ToString();
        //         var apiVersion = reqBody["api_version"]?.ToString();
        //         var clientId = reqBody["client_id"]?.ToString();
        //         var clientSecret = reqBody["client_secret"]?.ToString();
        //         var databaseName = reqBody["database_name"]?.ToString();
        //         var serverName = reqBody["server_name"]?.ToString();
        //         var tenantId = reqBody["tenant_id"]?.ToString();
        //         var url = reqBody["url"]?.ToString();
        //         var userId = reqBody["UserId"]?.ToString();
        //         var environment = reqBody["Environment"]?.ToString();

        //         var args = new object[]
        //         {
        //             entityName, grpno, tableName, schemaName, keycol, selectsql, threads, batchsize, jobId, jocExeLogId, apiVersion, clientId, clientSecret, databaseName, serverName, tenantId, url, userId
        //         };

        //         var result = BatchMultiselectUpdate((string)args[0], (int)args[1], (string)args[2], (string)args[3], (string)args[4], (string)args[5], (int)args[6], (int)args[7], (string)args[8], (string)args[9], (string)args[10], (string)args[11], (string)args[12], (string)args[13], (string)args[14], (string)args[15], (string)args[16]);

        //         log.LogInformation(result);

        //         if (result.StartsWith("FAIL"))
        //         {
        //             log.LogError($"An error occurred: {result}");
        //             return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {result}");
        //         }

        //         log.LogInformation("FunctionRun Completed");
        //         return req.CreateResponse(HttpStatusCode.OK, result);
        //     }
        //     catch (Exception e)
        //     {
        //         log.LogError($"An error occurred: {e.Message}");
        //         return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {e.Message}");
        //     }
        // }

        // [FunctionName("batch_add_nton_func")]
        // public static async Task<HttpResponseMessage> BatchFuncAddNtoN(
        //     [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "batch_add_nton")] HttpRequestMessage req,
        //     ILogger log)
        // {
        //     try
        //     {
        //         var reqBody = await req.Content.ReadAsAsync<JObject>();

        //         if (reqBody == null || !reqBody.HasValues)
        //         {
        //             return req.CreateResponse(HttpStatusCode.BadRequest, "Empty body passed");
        //         }

        //         var apiVersion = reqBody["api_version"]?.ToString();
        //         var clientId = reqBody["client_id"]?.ToString();
        //         var clientSecret = reqBody["client_secret"]?.ToString();
        //         var databaseName = reqBody["database_name"]?.ToString();
        //         var environment = reqBody["Environment"]?.ToString();
        //         var serverName = reqBody["server_name"]?.ToString();
        //         var tenantId = reqBody["tenant_id"]?.ToString();
        //         var url = reqBody["url"]?.ToString();
        //         var userId = reqBody["UserId"]?.ToString();
        //         var jobId = reqBody["Job_ID"]?.ToString();
        //         var srcSchName = reqBody["SrcSchName"]?.ToString();
        //         var srcTblName = reqBody["SrcTblName"]?.ToString();
        //         var trgTblName = reqBody["TrgTblName"]?.ToString();
        //         var targetTableNNSchemaName = reqBody["Target_Table_NN_Schema_Name"]?.ToString();
        //         var parentTable1CollectionName = reqBody["Parent_Table1_Collection_Name"]?.ToString();
        //         var parentTable2CollectionName = reqBody["Parent_Table2_Collection_Name"]?.ToString();
        //         var col1Name = reqBody["Col1Name"]?.ToString();
        //         var col2Name = reqBody["Col2Name"]?.ToString();
        //         var jocExeLogId = reqBody["JocExeLogId"]?.ToString();
        //         var threads = int.Parse(reqBody["threads"]?.ToString() ?? "0");
        //         var batchsize = int.Parse(reqBody["batchsize"]?.ToString() ?? "0");
        //         var grpno = int.Parse(reqBody["GrpNo"]?.ToString() ?? "0");

        //         var args = new object[]
        //         {
        //             srcSchName, srcTblName, trgTblName, targetTableNNSchemaName, parentTable1CollectionName, parentTable2CollectionName, col1Name, col2Name, batchsize, threads, grpno, jobId, jocExeLogId, apiVersion, clientId, clientSecret, databaseName, serverName, tenantId, url, userId
        //         };

        //         log.LogInformation(string.Join(", ", args));

        //         var result = BatchAddNtoNEntity((string)args[0], (string)args[1], (string)args[2], (string)args[3], (string)args[4], (string)args[5], (string)args[6], (string)args[7], (int)args[8], (int)args[9], (int)args[10], (string)args[11], (string)args[12], (string)args[13], (string)args[14], (string)args[15], (string)args[16], (string)args[17], (string)args[18], (string)args[19], (string)args[20]);

        //         log.LogInformation(result);

        //         if (result.StartsWith("FAIL"))
        //         {
        //             log.LogError($"An error occurred: {result}");
        //             return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {result}");
        //         }

        //         log.LogInformation("FunctionRun Completed");
        //         return req.CreateResponse(HttpStatusCode.OK, result);
        //     }
        //     catch (Exception e)
        //     {
        //         log.LogError($"An error occurred: {e.Message}");
        //         return req.CreateResponse(HttpStatusCode.InternalServerError, $"An error occurred: {e.Message}");
        //     }
        // }
    }
}
