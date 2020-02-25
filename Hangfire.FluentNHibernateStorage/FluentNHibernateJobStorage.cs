using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using FluentNHibernate.Cfg;
using FluentNHibernate.Cfg.Db;
using Hangfire.Annotations;
using Hangfire.FluentNHibernateStorage.Entities;
using Hangfire.FluentNHibernateStorage.JobQueue;
using Hangfire.FluentNHibernateStorage.Maps;
using Hangfire.FluentNHibernateStorage.Monitoring;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using NHibernate;
using Snork.FluentNHibernateTools;
using IsolationLevel = System.Data.IsolationLevel;

namespace Hangfire.FluentNHibernateStorage
{
    public class FluentNHibernateJobStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(FluentNHibernateJobStorage));

        private readonly CountersAggregator _countersAggregator;


        private readonly ExpirationManager _expirationManager;


        private readonly ISessionFactory _sessionFactory;

        public FluentNHibernateJobStorage(ProviderTypeEnum providerType, string nameOrConnectionString,
            FluentNHibernateStorageOptions options = null) : this(
            SessionFactoryBuilder.GetFromAssemblyOf<_CounterMap>(providerType, nameOrConnectionString, options))
        {
        }

        public FluentNHibernateJobStorage(IPersistenceConfigurer persistenceConfigurer,
            FluentNHibernateStorageOptions options = null) : this(SessionFactoryBuilder.GetFromAssemblyOf<_CounterMap>(
            persistenceConfigurer, options))
        {
        }


        public FluentNHibernateJobStorage(SessionFactoryInfo info)
        {
            ProviderType = info.ProviderType;
            _sessionFactory = info.SessionFactory;

            var tmp = info.Options as FluentNHibernateStorageOptions;
            Options = tmp ?? new FluentNHibernateStorageOptions();

            InitializeQueueProviders();
            _expirationManager = new ExpirationManager(this);
            _countersAggregator = new CountersAggregator(this);


            //escalate session factory issues early
            try
            {
                EnsureDualHasOneRow();
            }
            catch (FluentConfigurationException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }

        public FluentNHibernateStorageOptions Options { get; }


        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public ProviderTypeEnum ProviderType { get; } = ProviderTypeEnum.None;

        public DateTime UtcNow
        {
            get
            {
                using (var session = _sessionFactory.OpenSession())
                {
                    return session.GetUtcNow(ProviderType);
                }
            }
        }

        public void Dispose()
        {
        }

        private void EnsureDualHasOneRow()
        {
            try
            {
                using (var session = GetSession())
                {
                    using (var transaction = session.BeginTransaction(IsolationLevel.Serializable))
                    {
                        var count = session.Query<_Dual>().Count();
                        switch (count)
                        {
                            case 1:
                                return;
                            case 0:
                                session.Insert(new _Dual {Id = 1});
                                session.Flush();
                                break;
                            default:
                                session.DeleteByInt32Id<_Dual>(
                                    session.Query<_Dual>().Skip(1).Select(i => i.Id).ToList());
                                break;
                        }

                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.WarnException("Issue with dual table", ex);
                throw;
            }
        }

        private void InitializeQueueProviders()
        {
            QueueProviders =
                new PersistentJobQueueProviderCollection(
                    new FluentNHibernateJobQueueProvider(this));
        }

#pragma warning disable 618

        public override IEnumerable<IServerComponent> GetComponents()

        {
            return new List<IServerComponent> {_expirationManager, _countersAggregator};
        }

#pragma warning restore 618

        public List<IBackgroundProcess> GetBackgroundProcesses()
        {
            return new List<IBackgroundProcess> {_expirationManager, _countersAggregator};
        }


        public override void WriteOptionsToLog(ILog logger)
        {
            if (logger.IsInfoEnabled())
                logger.InfoFormat("Using the following options for job storage: {0}",
                    XmlConvert.SerializeObject(Options));
        }


        public override IMonitoringApi GetMonitoringApi()
        {
            return new FluentNHibernateMonitoringApi(this);
        }

        public override IStorageConnection GetConnection()
        {
            return new FluentNHibernateJobStorageConnection(this);
        }


        public IEnumerable ExecuteHqlQuery(string hql)
        {
            using (var session = GetSession())
            {
                return session.CreateQuery(hql).List();
            }
        }

        internal T UseTransaction<T>([InstantHandle] Func<SessionWrapper, T> func)
        {
            using (var transaction = CreateTransaction())
            {
                var result = UseSession(func);
                transaction.Complete();
                return result;
            }
        }

        internal void UseTransaction([InstantHandle] Action<SessionWrapper> action)
        {
            UseTransaction(session =>
            {
                action(session);
                return true;
            });
        }

        private TransactionScope CreateTransaction()
        {
            return new TransactionScope(TransactionScopeOption.Required,
                new TransactionOptions
                {
                    IsolationLevel = Options.TransactionIsolationLevel,
                    Timeout = Options.TransactionTimeout
                });
        }

        public void ResetAll()
        {
            using (var session = GetSession())
            {
                session.DeleteAll<_List>();
                session.DeleteAll<_Hash>();
                session.DeleteAll<_Set>();
                session.DeleteAll<_Server>();
                session.DeleteAll<_JobQueue>();
                session.DeleteAll<_JobParameter>();
                session.DeleteAll<_JobState>();
                session.DeleteAll<_Job>();
                session.DeleteAll<_Counter>();
                session.DeleteAll<_AggregatedCounter>();
                session.DeleteAll<_DistributedLock>();
                session.Flush();
            }
        }

        public void UseSession([InstantHandle] Action<SessionWrapper> action)
        {
            using (var session = GetSession())
            {
                action(session);
            }
        }

        public T UseSession<T>([InstantHandle] Func<SessionWrapper, T> func)
        {
            using (var session = GetSession())
            {
                var result = func(session);
                return result;
            }
        }


        public SessionWrapper GetSession()
        {
            return new SessionWrapper(_sessionFactory.OpenSession(), this);
        }
    }
}