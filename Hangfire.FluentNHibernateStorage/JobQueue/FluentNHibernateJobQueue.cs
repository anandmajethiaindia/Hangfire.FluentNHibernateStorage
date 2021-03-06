﻿using System;
using System.Data;
using System.Linq;
using System.Threading;
using Hangfire.FluentNHibernateStorage.Entities;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.FluentNHibernateStorage.JobQueue
{
    public class FluentNHibernateJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.For<FluentNHibernateJobQueue>();

        private readonly FluentNHibernateJobStorage _storage;

        public FluentNHibernateJobQueue(FluentNHibernateJobStorage storage)
        {
            Logger.Debug("Job queue initialized");
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");
            Logger.Debug("Attempting to dequeue");

            var timeoutSeconds = _storage.Options.InvisibilityTimeout.Negate().TotalSeconds;

            while (!cancellationToken.IsCancellationRequested)
            {
                var fluentNHibernateDistributedLock = new FluentNHibernateDistributedLock(_storage, "JobQueue",
                        _storage.Options.JobQueueDistributedLockTimeout)
                    .Acquire();
                if (fluentNHibernateDistributedLock == null)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        return null;
                    }
                    cancellationToken.WaitHandle.WaitOne(_storage.Options.QueuePollInterval);
                }
                else
                    using (fluentNHibernateDistributedLock)
                    {
                        var fluentNHibernateFetchedJob = SqlUtil.WrapForTransaction(() =>
                        {
                            var token = Guid.NewGuid().ToString();


                            using (var session = _storage.GetStatelessSession())
                            {
                                using (var transaction =
                                    session.BeginTransaction(IsolationLevel.Serializable))
                                {
                                    var jobQueueFetchedAt = _storage.UtcNow;

                                    var cutoff = jobQueueFetchedAt.AddSeconds(timeoutSeconds);
                                    if (Logger.IsDebugEnabled())
                                        Logger.Debug(string.Format("Getting jobs where {0}=null or {0}<{1}",
                                            nameof(_JobQueue.FetchedAt), cutoff));

                                    var jobQueue = session.Query<_JobQueue>()
                                        .FirstOrDefault(i =>
                                            (i.FetchedAt == null
                                             || i.FetchedAt < cutoff) && queues.Contains(i.Queue));
                                    if (jobQueue != null)
                                    {
                                        jobQueue.FetchToken = token;
                                        jobQueue.FetchedAt = jobQueueFetchedAt;
                                        session.Update(jobQueue);
                                        transaction.Commit();


                                        Logger.DebugFormat("Dequeued job id {0} from queue {1}",
                                            jobQueue.Job.Id,
                                            jobQueue.Queue);
                                        var fetchedJob = new FetchedJob
                                        {
                                            Id = jobQueue.Id,
                                            JobId = jobQueue.Job.Id,
                                            Queue = jobQueue.Queue
                                        };
                                        return new FluentNHibernateFetchedJob(_storage, fetchedJob);
                                    }
                                }
                            }

                            return null;
                        });
                        if (fluentNHibernateFetchedJob != null)
                            return fluentNHibernateFetchedJob;
                    }
            }

            return null;
        }

        public void Enqueue(StatelessSessionWrapper session, string queue, string jobId)
        {
            var converter = StringToInt32Converter.Convert(jobId);
            if (!converter.Valid)
                return;

            session.Insert(new _JobQueue
            {
                Job = session.Query<_Job>().SingleOrDefault(i => i.Id == converter.Value),
                Queue = queue
            });
            
            Logger.DebugFormat("Enqueued JobId={0} Queue={1}", jobId, queue);
        }
    }
}