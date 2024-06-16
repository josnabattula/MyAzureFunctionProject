using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace MyAzureFunctionProject
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    namespace BatchFunctions
    {
        public class BatchFunctions
        {
            private readonly ILogger<BatchFunctions> _logger;

            public BatchFunctions(ILogger<BatchFunctions> logger)
            {
                _logger = logger;
            }

            private static string GetDebugInfo()
            {
                var frame = new System.Diagnostics.StackFrame(1, true);
                return $"Debug from line number {frame.GetFileLineNumber()}";
            }

            private async Task<SqlConnection> GetEngineAsync(string serverName, string databaseName, string userId)
            {
                var connectionStringBuilder = new SqlConnectionStringBuilder
                {
                    DataSource = serverName,
                    InitialCatalog = databaseName,
                    UserID = userId,
                    Authentication = SqlAuthenticationMethod.ActiveDirectoryMsi,
                    Encrypt = true,
                    TrustServerCertificate = true,
                    ConnectTimeout = 300
                };

                string connectionString = connectionStringBuilder.ConnectionString;
                _logger.LogInformation(connectionString);

                try
                {
                    var engine = new SqlConnection(connectionString);
                    await engine.OpenAsync();
                    _logger.LogInformation("Engine connection successful");
                    return engine;
                }
                catch (SqlException ex)
                {
                    throw new Exception($"FAIL: Failed to make connection to SQL Server: {ex.Message}", ex);
                }
            }

            private async Task<string> RefreshTokenAsync(string url, string clientId, string clientSecret, string tenantId)
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

                    var response = await client.SendAsync(request);
                    response.EnsureSuccessStatusCode();

                    var tokenResponse = await response.Content.ReadAsStringAsync();
                    var token = JObject.Parse(tokenResponse)["access_token"].ToString();
                    return token;
                }
            }

            public async Task<string> BatchMultiselectUpdateAsync(
                string entityName, int grpno, string tableName, string schemaName, string keycol, string selectsql, int threads, int batchsize,
                string jobId, string jocExeLogId, string apiVersion, string clientId, string clientSecret, string databaseName,
                string serverName, string tenantId, string url, string userId)
            {
                string debugInfo = GetDebugInfo();
                _logger.LogInformation($"Starting batch update operation. Debug info: {debugInfo}");

                int errorCount = 0;

                try
                {
                    string batchGuid = Guid.NewGuid().ToString();
                    using (var engineConn = await GetEngineAsync(serverName, databaseName, userId))
                    {
                        var metadata = new MetaData();
                        metadata.Bind = engineConn;
                        var updateLogTable = new Table("MultiselectColErrorLog", metadata, "ETL_Framework", engineConn);
                        var deleteStatement = Queryable.Delete(updateLogTable)
                            .Where(row => row.Field<string>("Job_ID") == jobId && row.Field<string>("JocExeLogId") == jocExeLogId && row.Field<int>("ETL_Group_Number") == grpno);

                        using (var connection = await engineConn.OpenAsync())
                        {
                            await connection.ExecuteAsync(deleteStatement);
                        }

                        var df = await engineConn.ReadSqlAsync(selectsql);

                        if (df.Rows.Count > 0)
                        {
                            var token = await RefreshTokenAsync(url, clientId, clientSecret, tenantId);

                            var updateHeaders = new Dictionary<string, string>
                        {
                            { "Authorization", $"Bearer {token}" },
                            { "Content-Type", "application/json" },
                            { "OData-MaxVersion", "4.0" },
                            { "OData-Version", "4.0" },
                            { "Accept", "application/json" }
                        };

                            int totalRecordsUpdatedCounter = 0;
                            var batchUpdates = new List<Task<(HttpResponseMessage Response, string KeyColVal, string StatusCodeDescription)>>();

                            foreach (var row in df.Rows)
                            {
                                var idDict = new Dictionary<string, object> { { keycol, row[keycol] } };
                                var otherDict = row.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                                otherDict.Remove(keycol);

                                var updatePayload = otherDict;
                                string updateUrl = $"{url}/api/data/v{apiVersion}/{entityName}({idDict[keycol]})";
                                batchUpdates.Add(BatchUpdateEntityWithExceptionHandlingAsync(updateUrl, updatePayload, updateHeaders, idDict[keycol].ToString()));
                                totalRecordsUpdatedCounter++;

                                if (batchUpdates.Count >= batchsize || row.RowIndex == df.Rows.Count - 1)
                                {
                                    await Task.WhenAll(batchUpdates);

                                    foreach (var task in batchUpdates)
                                    {
                                        var (response, keyColValue, responseMsg) = await task;
                                        _logger.LogInformation(responseMsg);

                                        if (response == null || (!new[] { 200, 201, 202, 204 }.Contains((int)response.StatusCode) &&
                                            !new[] { "", "No Content", " Created", "OK", "Accepted" }.Contains((await response.Content.ReadAsStringAsync()).Trim())))
                                        {
                                            using (var conn = await engineConn.OpenAsync())
                                            {
                                                var insertStatement = Queryable.Insert(updateLogTable, new
                                                {
                                                    entity_name = entityName,
                                                    keycol_name = keycol,
                                                    keycol_val = keyColValue,
                                                    response_code = response?.StatusCode.ToString(),
                                                    response_text = await response?.Content.ReadAsStringAsync(),
                                                    response_result = responseMsg,
                                                    Job_ID = jobId,
                                                    JocExeLogId = jocExeLogId,
                                                    BatchGuid = batchGuid,
                                                    ETL_Group_Number = grpno
                                                });

                                                await conn.ExecuteAsync(insertStatement);
                                                await conn.CommitAsync();
                                            }
                                        }
                                    }

                                    batchUpdates.Clear();
                                }
                            }

                            using (var connection = await engineConn.OpenAsync())
                            {
                                var errorResult = await connection.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM ETL_Framework.MultiselectColErrorLog WHERE BatchGuid = '{batchGuid}'");
                                errorCount = errorResult;
                            }

                            if (errorCount > 0)
                            {
                                errorCount = 0;
                                return $"FAIL : {errorCount} Records failed to insert for group {grpno}.";
                            }
                            else
                            {
                                return $"Successfully updated {totalRecordsUpdatedCounter} out of {df.Rows.Count} records.";
                            }
                        }
                        else
                        {
                            return $"No records found to update ({df.Rows.Count} records).";
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Batch update operation failed: {ex.Message}. Debug info: {debugInfo}");
                    return $"FAIL : Batch update operation failed. {ex.Message}. Debug info: {debugInfo}";
                }
            }

            private async Task<(HttpResponseMessage Response, string KeyColVal, string StatusCodeDescription)> BatchUpdateEntityWithExceptionHandlingAsync(string updateUrl, object updatePayload, Dictionary<string, string> updateHeaders, string keyColVal)
            {
                try
                {
                    return await BatchUpdateEntityAsync(updateUrl, updatePayload, updateHeaders, keyColVal);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"An error occurred while processing batch update: {ex.Message}. Update URL: {updateUrl}, KeyColVal: {keyColVal}");
                    return (null, null, $"FAIL: An error occurred while processing batch update: {ex.Message}");
                }
            }

            private async Task<(HttpResponseMessage Response, string KeyColVal, string StatusCodeDescription)> BatchUpdateEntityAsync(string updateUrl, object updatePayload, Dictionary<string, string> updateHeaders, string keyColVal)
            {
                _logger.LogInformation($"Updating entity at {updateUrl}");

                try
                {
                    using (var client = new HttpClient())
                    {
                        foreach (var header in updateHeaders)
                        {
                            client.DefaultRequestHeaders.Add(header.Key, header.Value);
                        }

                        var jsonPayload = JsonConvert.SerializeObject(updatePayload);
                        var response = await client.PatchAsync(updateUrl, new StringContent(jsonPayload, Encoding.UTF8, "application/json"));
                        _logger.LogInformation(response.ToString());

                        var statusCodeDescription = response.StatusCode.ToString();
                        return (response, keyColVal, statusCodeDescription);
                    }
                }
                catch (Exception ex)
                {
                    return (null, keyColVal, $"FAIL: Update failed with error message {ex.Message}");
                }
            }
        }
    }

}
