package com.google.cloud.bigtable;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;

public class Main {
    public static void main(String[] args) throws Exception {
        BigtableDataClient client = BigtableDataClient.create("igorbernstein-dev", "benchmarks");

        ServerStream<Row> stream = client.readRows(Query.create("usertable_10_100"));
        for (Row row : stream) {
            System.out.println(row);
        }

        client.close();
    }
}
