package com.example.kafkaexample.ksqlConfig;

import io.confluent.ksql.api.client.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class ExampleAppKsql {

    private Client client;

    public static String KSQLDB_SERVER_HOST = "localhost";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;

    public ExampleAppKsql() {
       ClientOptions clientOptions = ClientOptions.create()
               .setHost(KSQLDB_SERVER_HOST)
               .setPort(KSQLDB_SERVER_HOST_PORT);
       this.client = Client.create(clientOptions);
    }

    /**
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<ResponseDTO> configKsql() throws ExecutionException, InterruptedException {
        // Send requests with the client by following the other examples
        client.streamQuery("SELECT * FROM RIDERLOCATIONS;")
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());

                    RowSubscriber rowSubscriber = new RowSubscriber();
                    streamedQueryResult.subscribe(rowSubscriber);

                    for (int i = 0; i < 10; i ++) {
                        Row row = streamedQueryResult.poll();
                        if (row != null) {
                            System.out.println("Received a row !");
                            System.out.println("Row: "  + row.values());
                        } else {
                            System.out.println("Query has ended");
                        }
                    }

                }).exceptionally(ex -> {
                    System.out.println("Request failed: " + ex);
                    return null;
                });

        //Receive query results in a single batch (executeQuery())
        String pullQuery = "SELECT * FROM RIDERLOCATIONS;";
        BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);

        List<Row> resultRows = batchedQueryResult.get();
        System.out.println("Received result. Numb rows: " + resultRows.size());
        List<ResponseDTO> responseDTOS = new ArrayList<>();
        for (Row row : resultRows) {
            System.out.println("Row: " + row.values());
            ResponseDTO responseDTO = new ResponseDTO();
            responseDTO.setProfileId(row.getString("PROFILEID"));

            responseDTO.setLatitude(String.valueOf(row.getInteger("LATITUDE")));
            responseDTO.setLongItude(String.valueOf(row.getInteger("LONGITUDE")));

            responseDTOS.add(responseDTO);
        }

        // Terminate any open connections and close the client
        client.close();
        return responseDTOS;
    }

    /**
     * @param requestDTO
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void insertQuery(RequestDTO requestDTO) throws ExecutionException, InterruptedException {
        KsqlObject ksqlObject = new KsqlObject()
                .put("PROFILEID", requestDTO.getProfileId())
                .put("LATITUDE", requestDTO.getLatitude())
                .put("LONGITUDE", requestDTO.getLongiTude());

        client.insertInto("RIDERLOCATIONS", ksqlObject).get();
    }

    /**
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public String clientQueryId() throws ExecutionException, InterruptedException {
        ClientOptions clientOptions = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT);
        Client client = Client.create(clientOptions);

        //Receive query results use streamQuery
        String pullQuery = "SELECT * FROM RIDERLOCATIONS LIMIT 3;";
        StreamedQueryResult streamedQueryResult = client.streamQuery(pullQuery).get();

        String queryId = streamedQueryResult.queryID();
        client.terminatePushQuery(queryId).get();

//        Receive query result use executeQuery
//        BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);
//        String queryIdExecuteQuery = batchedQueryResult.queryID().get();
//        client.terminatePushQuery(queryId).get();

        // Terminate any open connections and close the client
        client.close();
        return queryId;
    }

    public void createStream(String queryString) {

    }
}
