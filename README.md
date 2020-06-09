# Transactions Demo (Java)

## Goal

The goal of this demo is to show the basics of client-side transactions available in the Couchbase Java SDK 3.x and Couchbase Server 6.5. It will show a basic transaction that affects two documents. It can also show a rollback.

There are two documents: a conference (e.g. "ndcOslo2020") and a conference's interactions (e.g. "ndcOslo2020::interactions")

In this transaction, a new followup event will be added to the interactions. The conference will also be updated to increase the number of interactions as well as the date of the last interaction.

## Initial setup

1. Modify the constants in `TxDemo.java`:
   1. BUCKET - name of the bucket you've imported CRM data into
   2. CLUSTER_ADDRESS - address of a node in a Couchbase cluster
   3. USERNAME/PASSWORD - Couchbase cluster credentials

## Demo steps

1. Open project in VSCode or IntelliJ or whatever

2. Right click on TxDemo class and select "Run" (press ENTER when prompted)

3. Observe the program. Look at the documents. The `lastInteraction` field in acc1 should match the `intDate` in the interactions document in the events array.

4. Change the `message` (or uncomment one of the other interactions.getArray blocks) and run the program again. Repeat from step 2.

5. Change the `message` again, and uncomment `IllegalStateException` to cause a rollback. Run the program again.

6. Observe that the conference and interactions are in a consistent state.

## Bonus edge case demo

0. Uncomment any exceptions that are thrown.

1. Set a breakpoint at `ctx.replace(interactionsTx, interactions);`

2. Run the program, and when the breakpoint is hit, stop the process.

3. Observe the xattrs of the conference document in the Couchbase UI

4. Wait 60 seconds when prompted to press ENTER. Observe the cleanup processes running.

5. Press ENTER and let the transaction finish.

6. Observe that the conference and interactions are in a consistent state.
