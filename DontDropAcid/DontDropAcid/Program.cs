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

        static async Task Main(string[] args)
        {
            // connect to Couchbase
            _cluster = await Cluster.ConnectAsync(
                "couchbase://localhost",
                "Administrator",
                "password");
            _bucket = await _cluster.BucketAsync("matt");
            _coll = _bucket.DefaultCollection();


            // create a 'conference' document and a 'conference activities' document
            await SetupInitialDocuments();

            var transactions = Transactions.Create(_cluster, 
                TransactionConfigBuilder.Create()
                    .DurabilityLevel(DurabilityLevel.None)
                    .Build());

            await transactions.RunAsync(async (ctx) =>
            {
                var confDoc = await ctx.GetAsync(_coll, "manningK8s");
                var actsDoc = await ctx.GetAsync(_coll, "manningK8s::activities");
                var conf = confDoc.ContentAs<Conference>();
                var acts = actsDoc.ContentAs<ConferenceActivities>();

                var now = DateTime.Now;

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
                //     Type = "SLACK",
                //     DtActivity = now,
                //     Desc = "Answered questions in Slack"
                // });

                conf.Followups = (conf.Followups ?? 0) + 1;
                conf.LastActivity = now;

                await ctx.ReplaceAsync(confDoc, conf);
                await ctx.ReplaceAsync(actsDoc, acts);

                //throw new Exception("Something went wrong!");

            });

            await _cluster.DisposeAsync();
        }

        private static async Task SetupInitialDocuments()
        {
            var confExists = await _coll.ExistsAsync("manningK8s");
            if (confExists.Exists)
                return;
            var confActivitiesExists = await _coll.ExistsAsync("manningK8s::activities");
            if (confActivitiesExists.Exists)
                return;

            await _coll.InsertAsync("manningK8s", new Conference
            {
                Name = "Manning K8S Day",
                Location = "twitch.tv/manningpublications"
            });
            await _coll.InsertAsync("manningK8s::activities", new ConferenceActivities
            {
                ConferenceId = "manningK8s",
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
