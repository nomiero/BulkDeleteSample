package com.microsoft.cosmosdb.bulkdelete.sample;


import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import java.util.concurrent.ExecutionException;

public class Main {
    private static String HOST = "https://<endpoint>.documents.azure.com:443/";
    private static String KEY  = "<key>";
    private static String DATABASE_ID = "<DB_ID>";
    private static String COLLECTION_ID = "<COLLECTION_ID>";
    private static String QUERY = "<QUERY>";
    private static int throughputToUse = 50000;
    public static void main( String[] args ) throws ExecutionException, InterruptedException, DocumentClientException {
        new Main().run();
    }

    private void run() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST, KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Eventual);
        DocumentCollection collection = client.readCollection(String.format("dbs/%s/colls/%s", DATABASE_ID, COLLECTION_ID), null).getResource();
        DocumentBulkExecutor.Builder bulkExecutorBuilder = DocumentBulkExecutor.builder().
                from(client, DATABASE_ID, COLLECTION_ID, collection.getPartitionKey(),
                        throughputToUse);

        // Create the Bulk Executor instance
        try {
            DocumentBulkExecutor executor = bulkExecutorBuilder.build();

            // Call deleteAll with the query
            BulkDeleteResponse deleteResponse = executor.deleteAll(
                    QUERY,
                    null);

            System.out.println(String.format("Deleted %d documents", deleteResponse.getNumberOfDocumentsDeleted()));
        } catch (DocumentClientException ex) {
            System.out.println("Caught DocumentClientException when executing deleteAll with status code: " + ex.getStatusCode() + " and message: " + ex.getMessage());
        } catch (Exception ex) {
            System.out.println("Caught Exception when initializing the BulkExecutor. Exception message was: " + ex.getMessage());
        }
    }
}
