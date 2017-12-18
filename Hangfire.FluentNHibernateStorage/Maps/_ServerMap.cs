using FluentNHibernate.Mapping;
using Hangfire.FluentNHibernateStorage.Entities;

namespace Hangfire.FluentNHibernateStorage.Maps
{
    internal class _ServerMap : ClassMap<_Server>
    {
        public _ServerMap()
        {
            Table("`Hangfire_Server`");
            Id(i => i.Id).Length(100).GeneratedBy.Assigned().Column(Constants.IdColumnName);
            Map(i => i.Data).Length(Constants.VarcharMaxLength).Not.Nullable().Column(Constants.DataColumnName);
            Map(i => i.LastHeartbeat).Not.Nullable().Column("`LastHeartBeat`");
        }
    }
}