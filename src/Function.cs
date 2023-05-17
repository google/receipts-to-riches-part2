/*
Copyright 2023 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    https://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using CloudNative.CloudEvents;

using Google.Api;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
    
using Google.Apis.Storage.v1.Data;
using Google.Cloud.DocumentAI.V1Beta3;
using Google.Cloud.Functions.Framework;
using Google.Cloud.Logging.V2;
using Google.Cloud.Logging.Type;
using Google.Cloud.Storage.V1;
using Google.Events.Protobuf.Cloud.Storage.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Newtonsoft.Json.Linq;

using Grpc.Core;

using Newtonsoft.Json;

namespace HelloGcs;

public class Function : ICloudEventFunction<StorageObjectData>
{
    private readonly StorageClient _storageClient;
    private readonly DocumentProcessorServiceClient _documentAI;
    public static string projectId = "<--your project ID -->";
    public static string gcp_Region = "<-- GCP Region. Example us -->";
    public static string documentAI_processor = "<-- Document AI processor ID-->";
    public static string logId = "<-- Custom Log ID. For example, CF_SupermarketClassifier -->";
    public static string destinationBucketName1 = "<--Storage bucket to where SuperMarketX invoices are saved -->";
    public static string destinationBucketName2 = "<--Storage bucket to where SuperMarketX invoices are saved -->"; 

    public Function()
    {
      _storageClient = StorageClient.Create();
      _documentAI = DocumentProcessorServiceClient.Create();
    }

    public async Task HandleAsync(CloudEvent cloudEvent, StorageObjectData data, CancellationToken cancellationToken)
    {
        var bucketName = data.Bucket;
        var objectName = data.Name;
        var uri = new Uri($"gs://{bucketName}/{objectName}");

        try{
            // Call the DocumentAI classifier endpoint
            var responseDocument = await CallDocumentAiEndPointAsync(uri, cancellationToken);

            // Classify receipts into separate storage buckets based on predicted endpoint.
            await ClassifySuperMakertInvoices(responseDocument, bucketName, objectName);

        }catch(Exception e)
        {
            Console.WriteLine($"Error in classifying receipts. Here are error details {e.ToString()}");
            LogError(e);
        }
        
        return;
    }

    /* Classify receipts into separate storage buckets based on predicted endpoint*/
    private async Task ClassifySuperMakertInvoices(Document document, string sourceBucketName, string sourceObjectName)
    {
        // Find Entity with hightest confidence score
        float highestConfidence = 0;
        Document.Types.Entity entityWithHighestConfidence = null;

        foreach (var entity in document.Entities)
        {
            if (entity.Confidence > highestConfidence)
            {
                entityWithHighestConfidence = entity;
                highestConfidence = entity.Confidence;
            }
        }

        if (entityWithHighestConfidence != null)
        {
            var classification = entityWithHighestConfidence.Type;
            Console.WriteLine($"The entity with the highest confidence is {classification}.");

            // Construct the source and destination object names
            var sourceObject = _storageClient.GetObject(sourceBucketName, sourceObjectName);

            string guid = Guid.NewGuid().ToString();
            var destObjectName = guid + "-" + sourceObjectName;

            // Copy the source file to the appropriate destination buckets
            switch (classification)
            {
                case "grocerybill_1": //This should be same as "Labels" in Document AI classifier
                    _storageClient.CopyObject(sourceBucketName, sourceObjectName, destinationBucketName1, destObjectName);
                    Console.WriteLine($"Copied {sourceBucketName}/{sourceObjectName} to {destinationBucketName1}/{destObjectName}.");
                    break;
                case "grocerybill_2": //This should be same as "Labels" in Document AI classifier
                    _storageClient.CopyObject(sourceBucketName, sourceObjectName, destinationBucketName2, destObjectName);
                    Console.WriteLine($"Copied {sourceBucketName}/{sourceObjectName} to {destinationBucketName2}/{destObjectName}.");
                    break;
                // Add more cases for additional classification labels
                default:
                    Console.WriteLine($"Unknown classification label: {classification}");
                    break;
            }
        }
        else
        {
            Console.WriteLine("No entities found.");
        }
    }
    
    /* Calling Document AI API */
    private async Task<Document> CallDocumentAiEndPointAsync(Uri uri, CancellationToken cancellationToken)
    {
        var bucketName = uri.Host;
        var objectName = uri.AbsolutePath.TrimStart('/');
        string contentType = GetContentType(objectName);
        Console.WriteLine(objectName);

        if(contentType == string.Empty)
        {
            LogError(new Exception($"Invalid file type. Cannot process. ObjectName is : {objectName}")); 
        }

        var obj = await _storageClient.GetObjectAsync(bucketName, objectName);
        using var memoryStream = new MemoryStream();
        _storageClient.DownloadObject(obj, memoryStream);
        memoryStream.Position = 0;

        var rawDocument = new RawDocument
        {
            Content = ByteString.FromStream(memoryStream),
            MimeType = contentType
        };

        var processorName = new ProcessorName(projectId, gcp_Region, documentAI_processor);

        var request = new ProcessRequest
        {
            RawDocument = rawDocument,
            Name = processorName.ToString()
        };

        var response = await _documentAI.ProcessDocumentAsync(request);
        return response.Document;
    }

    /* Custom log entry */
    public static void LogError(Exception ex)
    {
        // Initialize the Logging client
        LoggingServiceV2Client loggingClient = LoggingServiceV2Client.Create();

        // Prepare new log entry.
        LogEntry logEntry = new LogEntry();
        LogName logName = new LogName(projectId, logId);
        logEntry.LogNameAsLogName = logName;
        logEntry.Severity = LogSeverity.Error;
        logEntry.TextPayload = ex.ToString();

        // Write the log entry to Google Cloud Logging
        MonitoredResource resource = new MonitoredResource { Type = "global" };
        try
        {
            // Create dictionary object to add custom labels to the log entry.
            IDictionary<string, string> entryLabels = new Dictionary<string, string>();

            // Add log entry to collection for writing. Multiple log entries can be added.
            IEnumerable<LogEntry> logEntries = new LogEntry[] { logEntry };

            // Write new log entry.
            loggingClient.WriteLogEntries(logName, resource, entryLabels, logEntries);
            //loggingClient.WriteLogEntries(resource, new[] { logEntry });
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.Unauthenticated)
        {
            Console.WriteLine("Failed to authenticate: " + e);
        }
        catch (Exception e)
        {
            Console.WriteLine("Failed to write log: " + e);
        }
    }

    /* Helper function */
    public static string GetContentType(string fileName)
    {
        // Get the file extension
        string extension = Path.GetExtension(fileName).ToLowerInvariant();
        switch (extension)
        {
            case ".pdf":
                return "application/pdf";
            case ".jpg":
            case ".jpeg":
            case ".png":
                return "image/" + extension.Substring(1);
            default:
                return string.Empty;
        }
    }
}
