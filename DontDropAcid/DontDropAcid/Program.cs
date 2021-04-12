using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;

namespace DontDropAcid
{
    class Program
    {
        private static ICluster _cluster;
        private static IBucket _bucket;
        private static ICouchbaseCollection _coll;
        private static IScope _scope;

        static async Task Main(string[] args)
        {
            // SETUP: connect to Couchbase
            _cluster = await Cluster.ConnectAsync(
                "couchbase://localhost",
                "Administrator",
                "password");
            _bucket = await _cluster.BucketAsync("matt");
            _scope = await _bucket.ScopeAsync("myScope");
            _coll = await _scope.CollectionAsync("myCollection");

            // SETUP: create a 'conference' document and a 'conference activities' document
            await SetupInitialDocuments();

            // STEP 1: create transactions object
            var transactions = Transactions.Create(_cluster, 
                TransactionConfigBuilder.Create()
                    .DurabilityLevel(DurabilityLevel.MajorityAndPersistToActive)    // since I have 1 node, replication must be 0, or this will throw exception
                    .Build());

            Console.WriteLine("Press ENTER to continue");
            Console.ReadLine();

            // STEP 2: transaction operations
            await transactions.RunAsync(async (ctx) =>
            {
                var now = DateTime.Now;

                // FIRST: get the two document I want to change
                var confDoc = await ctx.GetAsync(_coll, "dataLove2021");
                var actsDoc = await ctx.GetAsync(_coll, "dataLove2021::activities");
                var conf = confDoc.ContentAs<Conference>();
                var acts = actsDoc.ContentAs<ConferenceActivities>();

                // SECOND: add an event to the "activities" document
                acts.Events.Add(new ConferenceEvent
                {
                    Type = "CFP",
                    DtActivity = now,
                    Desc = "Submitted to the CFP"
                });
                // acts.Events.Add(new ConferenceEvent
                // {
                //     Type = "PRESENTATION",
                //     DtActivity = now,
                //     Desc = "Delivered ACID presentation"
                // });
                // acts.Events.Add(new ConferenceEvent
                // {
                //     Type = "SPATIAL",
                //     DtActivity = now,
                //     Desc = "Answered questions in Spatial Chat"
                // });

                // THIRD: change the "conference" document
                conf.Followups = (conf.Followups ?? 0) + 1;
                conf.LastActivity = now;

                // FOURTH: write the changes
                await ctx.ReplaceAsync(confDoc, conf);

                // OPTIONAL STEP: fail right in the middle of the transaction making two writes
                // var fail = true;
                // if(fail) throw new Exception("Something went wrong!");
                
                await ctx.ReplaceAsync(actsDoc, acts);

                // FIFTH: commit (implied)
            });

            await _cluster.DisposeAsync();
        }

        private static async Task SetupInitialDocuments()
        {
            var confExists = await _coll.ExistsAsync("dataLove2021");
            if (confExists.Exists)
                return;
            var confActivitiesExists = await _coll.ExistsAsync("dataLove2021::activities");
            if (confActivitiesExists.Exists)
                return;

            await _coll.InsertAsync("dataLove2021", new Conference
            {
                Name = "Data Love 2021",
                Location = "https://datalove.konfy.care/"
            });
            await _coll.InsertAsync("dataLove2021::activities", new ConferenceActivities
            {
                ConferenceId = "dataLove2021",
                Events = new List<ConferenceEvent>()
            });
        }
    }

    public class Conference
    {
        public string Name { get; set; }
        public string Location { get; set; }
        public string Type => "conference";
        public int? Followups { get; set; }
        public DateTime? LastActivity { get; set; }

        public bool ShouldSerializeFollowups() => Followups.HasValue;
        public bool ShouldSerializeLastActivity() => LastActivity.HasValue;
    }

    public class ConferenceActivities
    {
        public string ConferenceId { get; set; }
        public string Type => "activities";
        public List<ConferenceEvent> Events { get; set; }
    }

    public class ConferenceEvent
    {
        public string Type { get; set; }
        public DateTime DtActivity { get; set; }
        public string Desc { get; set; }
    }
}
