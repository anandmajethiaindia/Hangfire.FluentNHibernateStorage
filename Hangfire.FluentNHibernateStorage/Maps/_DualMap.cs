using FluentNHibernate.Mapping;
using Hangfire.FluentNHibernateStorage.Entities;

namespace Hangfire.FluentNHibernateStorage.Maps
{
    public class _DualMap : ClassMap<_Dual>
    {
        public _DualMap()
        {
            Table("Dual");
            Id(i => i.Id).Column(Constants.ColumnNames.Id.WrapObjectName()).GeneratedBy.Assigned();
        }
    }
}