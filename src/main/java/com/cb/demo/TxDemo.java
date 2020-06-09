package com.cb.demo;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * ###### How to run this demo:
 * 1) Change Couchbase's BUCKET/CLUSTER_ADDRESS/USERNAME/PASSWORD
 * 2) right click on this class and in the context menu click "Run"
 *
 * ###### Use Case:
 *
 *  In this use case we want to keep track of all _interactions_ with a _conference_ in order to build a conference tracking system,
 *  Whenever an email is sent to a client we will execute the following:
 *
 *  1) Increase the total of interactions whith the target conference into the "conference"
 *  2) Update the lastInteraction
 *  2) Add an item to the interaction document with the type of the interaction and its content.
 *
 */
public class TxDemo {

    private static final String BUCKET = "demo";
    private static final String CLUSTER_ADDRESS = "localhost";
    private static final String USERNAME = "Administrator";
    private static final String PASSWORD = "password";

    private static String conferenceId = "ndcOslo2020";
    private static String interactionId = "ndcOslo2020::interactions";
    private static final Logger logger = LoggerFactory.getLogger(TxDemo.class);

    public static void main(String[] args) {

        //1) Connect to the cluster
        Cluster cluster = Cluster.connect(CLUSTER_ADDRESS, USERNAME, PASSWORD);
        Bucket bucket = cluster.bucket(BUCKET);
        Collection col = bucket.defaultCollection();

        //2) Create Event for the account
        createInitialDocuments(bucket);

        // 3) Create the transaction config :
        // Durability: NONE/MAJORITY/PERSIST_TO_MAJORITY/MAJORITY_AND_PERSIST_ON_MASTER
        // TIMEOUT: Max TTL of the transaction
        // OBS: As I'm running in a single node the Durability is set to None
        Transactions transactions = Transactions.create(cluster, TransactionConfigBuilder.create()
                // NONE is the only level that will work with a *single* node, there is no replication involved
                .durabilityLevel(TransactionDurabilityLevel.NONE)

                // wait until each write is available in-memory on majority of replicas                
                //.durabilityLevel(TransactionDurabilityLevel.MAJORITY)

                // Wait until each write is both available in-memory and persisted to disk on a majority
                // of configured replicas before continuing.
                // .durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY)

                // wait until each write is available in-memory on a majority of configured replicas
                // and also persisted to disk on their active nodes, before continuing.
                //.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)

                .build());

        System.out.println("Press ENTER to continue...");
        System.console().readLine();

        try {


            transactions.run((ctx) -> {

                //Getting all documents involved in the transactions
                //There is no virtual limit on number of documents per transaction
                TransactionGetResult conferenceTx = ctx.get(col, conferenceId);
                TransactionGetResult interactionsTx = ctx.get(col, interactionId);

                JsonObject conference = conferenceTx.contentAsObject();
                JsonObject interactions = interactionsTx.contentAsObject();

                //updating 'customer' document
                long interactionDate = new Date().getTime();
                conference.put("followups", conference.getInt("followups") == null? 1: conference.getInt("followups")+1);
                conference.put("lastInteraction", interactionDate);

                // updating 'events' document
                interactions.getArray("events").add(
                    JsonObject.create()
                            .put("type", "CFP")
                            .put("intDate", interactionDate)
                            .put("description", "Submitted to the CFP")
                );
                // interactions.getArray("events").add(
                //     JsonObject.create()
                //             .put("type", "PRESENTATION")
                //             .put("intDate", interactionDate)
                //             .put("description", "Delivered ACID presentation")
                // );
                // interactions.getArray("events").add(
                //     JsonObject.create()
                //             .put("type", "SLACK")
                //             .put("intDate", interactionDate)
                //             .put("description", "Answered questions in Slack")
                // );                                

                //replace both documents
                ctx.replace(conferenceTx, conference);
                ctx.replace(interactionsTx, interactions);

                //uncomment this line to force a rollback
                //throw new IllegalStateException("Emulating a rollback");

                //optional
                //ctx.commit();
            });
        } catch (TransactionFailed e) {
            e.printStackTrace();
            for (LogDefer err : e.result().log().logs()) {
                System.err.println(err.toString());
            }
        }

    }

    // create a conference document
    // and an (empty) interactions document ahead of time
    private static void createInitialDocuments(Bucket bucket) {
        try {
            bucket.defaultCollection().get(interactionId);
            bucket.defaultCollection().get(conferenceId);
            logger.info("documents already exist");
        } catch(Exception e) {
            logger.info("Creating account events...");
            JsonObject interactions = JsonObject.create()
                    .put("type", "interactions")
                    .put("conferenceId", conferenceId)
                    .put( "events", JsonObject.ja());
            bucket.defaultCollection().upsert(interactionId, interactions);

            JsonObject conference = JsonObject.create()
                .put("type", "conference")
                .put("name", "NDC Oslo 2020")
                .put("location", "Norway");
            bucket.defaultCollection().upsert(conferenceId, conference);
        }
    }

}
