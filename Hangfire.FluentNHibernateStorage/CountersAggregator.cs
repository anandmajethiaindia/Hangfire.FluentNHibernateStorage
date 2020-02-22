using System;
using System.Linq;
using System.Threading;
using Hangfire.FluentNHibernateStorage.Entities;
using Hangfire.Logging;
using Hangfire.Server;
using NHibernate;

namespace Hangfire.FluentNHibernateStorage
{
#pragma warning disable 618
    public class CountersAggregator : IBackgroundProcess, IServerComponent
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly FluentNHibernateJobStorage _storage;

        public CountersAggregator(FluentNHibernateJobStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }


        public void Execute(CancellationToken cancellationToken)
        {
            var token = cancellationToken;
            Logger.Info("Aggregating records in 'Counter' table...");

            long removedCount = 0;

            do
            {
                _storage.UseSession(session =>
                {
                    using (var transaction = session.BeginTransaction())
                    {
                        //get counter items, grouped by key
                        var groupedItems = session.Query<_Counter>().GroupBy(counter => counter.Key)
                            .Select(i =>
                                new
                                {
                                    i.Key,
                                    Value = i.Sum(counter => counter.Value),
                                    ExpireAt = i.Max(counter => counter.ExpireAt),
                                    Count = i.Count()
                                }).Take(NumberOfRecordsInSinglePass)
                            .ToList();

                        if (groupedItems.Any())
                        {
                            var query = session.CreateQuery(SqlUtil.UpdateAggregateCounterStatement);

                            //TODO: Consider using upsert approach
                            foreach (var item in groupedItems)
                            {
                                Logger.DebugFormat("Processing aggregate for counter item {0}", item.Key);
                                query.SetParameter(SqlUtil.ValueParameter2Name, item.ExpireAt, NHibernateUtil.DateTime);

                                if (query.SetString(SqlUtil.IdParameterName, item.Key)
                                        .SetParameter(SqlUtil.ValueParameterName, item.Value)
                                        .ExecuteUpdate() == 0)
                                    session.Insert(new _AggregatedCounter
                                    {
                                        Key = item.Key,
                                        Value = item.Value,
                                        ExpireAt = item.ExpireAt
                                    });
                            }

                            session.CreateQuery(SqlUtil.DeleteCounterStatement)
                                .SetParameterList(SqlUtil.IdParameterName, groupedItems.Select(i => i.Key).ToList())
                                .ExecuteUpdate();
                            removedCount = groupedItems.Sum(i => i.Count);
                        }
                        else
                        {
                            removedCount = 0;
                        }

                        transaction.Commit();
                    }
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    token.WaitHandle.WaitOne(DelayBetweenPasses);
                    token.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            token.WaitHandle.WaitOne(_storage.Options.CountersAggregateInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
#pragma warning restore 618
    }
}