﻿using System;
using System.Linq;
using System.Transactions;
using Hangfire.FluentNHibernateStorage.JobQueue;
using MySql.Data.MySqlClient;
using Xunit;

namespace Hangfire.FluentNHibernateStorage.Tests.JobQueue
{
    public class FluentNHibernateJobQueueMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly MySqlConnection _connection;
        private readonly string _queue = "default";
        private readonly FluentNHibernateStorage _storage;
        private readonly FluentNHibernateJobQueueMonitoringApi _sut;

        public FluentNHibernateJobQueueMonitoringApiTests()
        {
            _connection = new MySqlConnection(ConnectionUtils.GetConnectionString());
            _connection.Open();

            _storage = new FluentNHibernateStorage(_connection);

            _sut = new FluentNHibernateJobQueueMonitoringApi(_storage);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact]
        [CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedAndFetchedCount_ReturnsEqueuedCount_WhenExists()
        {
            EnqueuedAndFetchedCountDto result = null;

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into JobQueue (JobId, Queue) " +
                    "values (1, @queue);", new {queue = _queue});

                result = _sut.GetEnqueuedAndFetchedCount(_queue);

                connection.Execute("delete from JobQueue");
            });

            Assert.Equal(1, result.EnqueuedCount);
        }


        [Fact]
        [CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            var result = _sut.GetEnqueuedJobIds(_queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact]
        [CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            int[] result = null;
            _storage.UseConnection(connection =>
            {
                for (var i = 1; i <= 10; i++)
                {
                    connection.Execute(
                        "insert into JobQueue (JobId, Queue) " +
                        "values (@jobId, @queue);", new {jobId = i, queue = _queue});
                }

                result = Enumerable.ToArray<int>(_sut.GetEnqueuedJobIds(_queue, 3, 2));

                connection.Execute("delete from JobQueue");
            });

            Assert.Equal(2, result.Length);
            Assert.Equal(4, result[0]);
            Assert.Equal(5, result[1]);
        }
    }
}