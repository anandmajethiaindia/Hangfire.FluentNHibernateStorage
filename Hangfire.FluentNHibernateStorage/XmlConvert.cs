using System;
using System.IO;
using System.Xml.Serialization;
using Hangfire.Logging;

namespace Hangfire.FluentNHibernateStorage
{
    internal static class XmlConvert
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        public static string SerializeObject<T>(T input)
        {
            try
            {
                var xmlSerializer = new XmlSerializer(typeof(T));
                using (var stringWriter = new StringWriter())
                {
                    xmlSerializer.Serialize(stringWriter, input);
                    return stringWriter.ToString();
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorException("Can't serialize", ex);
                return null;
            }
        }
    }
}