﻿using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dota_api_crawler
{
    public class Program
    {
        private const int CrawlWaitTime = 1000;
        private const string DatabaseFileName = "database/database.sqlite";

        // Steam API key
        private static readonly string ApiKey = Environment.GetEnvironmentVariable("STEAM_API_KEY");

        private readonly HttpClient _client = new HttpClient();

        public static async Task Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine($"Missing arguments. {AppDomain.CurrentDomain.FriendlyName} startId [endId]");
                return;
            }

            ulong endId = 1;

            if (!ulong.TryParse(args[0], out var startId))
            {
                Console.Error.WriteLine("Argument startId has to be an ulong.");
                return;
            }

            if (args.Length >= 2)
            {
                if (!ulong.TryParse(args[1], out endId))
                {
                    Console.Error.WriteLine("Argument endId has to be an ulong.");
                    return;
                }
            }

            await new Program().Start(startId, endId);
        }

        private async Task Start(ulong startId, ulong endId)
        {
            SQLitePCL.Batteries.Init();

            var connectionParameters = new SqliteConnectionStringBuilder
            {
                DataSource = DatabaseFileName,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };

            // TODO: Check existence of matches table directly when System.Data.Sqlite supports .NET Core

            using (var conn = new SqliteConnection(connectionParameters.ToString()))
            {
                var openSqlite = conn.OpenAsync();

                if (!File.Exists(DatabaseFileName))
                {
                    using (var transaction = conn.BeginTransaction())
                    {
                        var createCommand = conn.CreateCommand();
                        createCommand.Transaction = transaction;
                        createCommand.CommandText = File.ReadAllText("schema.sql");

                        createCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                }

                _client.DefaultRequestHeaders.Accept.Clear();
                _client.DefaultRequestHeaders.Accept.Add(
                    new MediaTypeWithQualityHeaderValue("application/json"));
                _client.DefaultRequestHeaders.Add("User-Agent", "Dota API Crawler 0.1");

                await openSqlite;

                for (var matchId = startId; matchId > endId; matchId--)
                {
                    Console.Write($"Getting match {matchId}: ");

                    HttpResponseMessage response;
                    try
                    {
                        response = await _client.GetAsync(
                            $"https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1" +
                            $"?key={ApiKey}&match_id={matchId}");
                    }
                    catch
                    {
                        // Wait if HTTP is not okay and retry
                        await Task.Delay(TimeSpan.FromSeconds(30));

                        // Increment matchId to re-run current step
                        matchId++;
                        
                        continue;
                    }

                    Console.WriteLine($"(HTTP status {response.StatusCode})");

                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        // Wait if HTTP is not okay and retry
                        await Task.Delay(TimeSpan.FromSeconds(30));

                        // Increment matchId to re-run current step
                        matchId++;

                        continue;
                    }

                    var readBody = response.Content.ReadAsStringAsync();

                    using (var transaction = conn.BeginTransaction())
                    {
                        var insertCommand = conn.CreateCommand();
                        insertCommand.Transaction = transaction;
                        insertCommand.CommandText = "INSERT INTO matches " +
                                                    "(matchId, httpStatusCode, errorMessage, rawJson, private, notFound) " +
                                                    "VALUES " +
                                                    "($matchId, $httpStatusCode, $errorMessage, $rawJson, $private, $notFound)";
                        insertCommand.Parameters.AddWithValue("$matchId", matchId);
                        insertCommand.Parameters.AddWithValue("$httpStatusCode", response.StatusCode);

                        var json = await readBody;

                        string errorMessage;
                        try
                        {
                            errorMessage = JObject.Parse(json).SelectToken("$.result.error")?.Value<string>();
                        }
                        catch (JsonReaderException)
                        {
                            errorMessage = "Invalid JSON";
                        }

                        var inlinedJson = json.Replace("\n", "");
                        
                        if (errorMessage == null)
                        {
                            insertCommand.Parameters.AddWithValue("$errorMessage", DBNull.Value);
                            insertCommand.Parameters.AddWithValue("$rawJson", inlinedJson);
                            insertCommand.Parameters.AddWithValue("$private", false);
                            insertCommand.Parameters.AddWithValue("$notFound", false);
                        }
                        else
                        {
                            insertCommand.Parameters.AddWithValue("$rawJson", inlinedJson);

                            switch (errorMessage)
                            {
                                case "Match ID not found":
                                    insertCommand.Parameters.AddWithValue("$errorMessage", DBNull.Value);
                                    insertCommand.Parameters.AddWithValue("$private", false);
                                    insertCommand.Parameters.AddWithValue("$notFound", true);
                                    break;
                                case "Practice matches are not available via GetMatchDetails":
                                    insertCommand.Parameters.AddWithValue("$errorMessage", DBNull.Value);
                                    insertCommand.Parameters.AddWithValue("$private", true);
                                    insertCommand.Parameters.AddWithValue("$notFound", false);
                                    break;
                                default:
                                    insertCommand.Parameters.AddWithValue("$errorMessage", errorMessage);
                                    insertCommand.Parameters.AddWithValue("$private", false);
                                    insertCommand.Parameters.AddWithValue("$notFound", false);
                                    break;
                            }
                        }
                        
                        insertCommand.ExecuteNonQuery();

                        try
                        {
                            transaction.Commit();
                        }
                        catch (SqliteException e)
                        {
                            if (e.SqliteErrorCode == 19)
                            {
                                // Contraint failed (currently only UNIQUE on matchId) => save to ignore for now
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }

                    await Task.Delay(CrawlWaitTime);
                }
            }
        }
    }
}