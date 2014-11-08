namespace ServiceControl.Infrastructure.RavenDB.Expiration
{
    using System;
    using System.ComponentModel.Composition;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using CompositeViews.Messages;
    using Lucene.Net.Documents;
    using Raven.Abstractions;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Logging;
    using Raven.Database;
    using Raven.Database.Impl;
    using Raven.Database.Plugins;
    using ServiceBus.Management.Infrastructure.Settings;

    [InheritedExport(typeof(IStartupTask))]
    [ExportMetadata("Bundle", "customDocumentExpiration")]
    public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(ExpiredDocumentsCleaner));
        private Timer timer;
        DocumentDatabase Database { get; set; }
        string indexName;
        int deleteFrequencyInSeconds;

        public void Execute(DocumentDatabase database)
        {
            Database = database;
            indexName = new MessagesViewIndex().IndexName;

            deleteFrequencyInSeconds = Settings.ExpirationProcessTimerInSeconds;
            if (deleteFrequencyInSeconds == 0)
            {
                return;
            }

            logger.Info("Initialized expired document cleaner, will check for expired documents every {0} seconds", deleteFrequencyInSeconds);
            timer = new Timer(TimerCallback, null, TimeSpan.FromSeconds(deleteFrequencyInSeconds), Timeout.InfiniteTimeSpan);
        }

        void TimerCallback(object state)
        {
            try
            {
                var currentTime = SystemTime.UtcNow;
                var currentExpiryThresholdTime = currentTime.AddHours(-Settings.HoursToKeepMessagesBeforeExpiring);
                logger.Debug("Trying to delete expired documents (with threshold {0})", currentExpiryThresholdTime.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture));
                var indexQuery = new IndexQuery
                {
                    Cutoff = currentTime,
                    Query = string.Format("((Status:3 OR Status:4) AND (ProcessedAt:[ * TO {0} ]))",  currentExpiryThresholdTime.ToString("O").Replace(":", @"\:").Replace("-", @"\-"))
                };

                var bulkOps = new DatabaseBulkOperations(Database, null, CancellationToken.None, null);
                var sw = new Stopwatch();
                sw.Start();

                var result = bulkOps.DeleteByIndex(indexName, indexQuery, true);
                sw.Stop();

                logger.Debug("Deleting {0} documents took {1} ms.", result.Length, sw.ElapsedMilliseconds);
            }
            catch (OperationCanceledException)
            {
                //Ignore
            }
            catch (Exception e)
            {
                logger.ErrorException("Error when trying to find expired documents", e);
            }
            finally
            {
                timer.Change(TimeSpan.FromSeconds(deleteFrequencyInSeconds), Timeout.InfiniteTimeSpan);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            if (timer != null)
            {
                using (var waitHandle = new ManualResetEvent(false))
                {
                    timer.Dispose(waitHandle);

                    waitHandle.WaitOne();
                }
            }
        }
    }
}
