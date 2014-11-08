namespace ServiceControl.Infrastructure.RavenDB.Expiration
{
    using System;
    using System.ComponentModel.Composition;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using CompositeViews.Messages;
    using FluentDate;
    using Raven.Abstractions;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Logging;
    using Raven.Database;
    using Raven.Database.Impl;
    using Raven.Database.Plugins;
    using ServiceBus.Management.Infrastructure.Settings;


   /// <summary>
   /// This cleaner can be be heavy on the system resources and uses stale indexes 
   /// because of this running it to frequently will place undue stress on the system
   /// v1.3 and earlier defaulted to running this every minute
   /// This version defaults to 3 hours and logs a warning if interval is set lower than 1 hour
   /// </summary>


    [InheritedExport(typeof(IStartupTask))]
    [ExportMetadata("Bundle", "customDocumentExpiration")]
    public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(ExpiredDocumentsCleaner));
        private Timer timer;
        DocumentDatabase Database { get; set; }
        string indexName;
        TimeSpan deleteFrequency;

        public void Execute(DocumentDatabase database)
        {
            Database = database;
            indexName = new MessagesViewIndex().IndexName;

            deleteFrequency = TimeSpan.FromSeconds(Settings.ExpirationProcessTimerInSeconds);
            if (deleteFrequency < 1.Seconds())
            {
                return;
            }

            if (deleteFrequency < 1.Hours())
            {
                logger.Warn("The expired document cleaner is set to run more than once an hour - this is not recommended (Default value: {0} (3 Hours)", 3.Hours().TotalSeconds);
            }
            logger.Info("Initialized expired document cleaner, will check for expired documents every {0} seconds", deleteFrequency.Seconds);
            timer = new Timer(TimerCallback, null, deleteFrequency, Timeout.InfiniteTimeSpan);
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
                    // This lucene query can fail silently if dates are not in ISO-8601 format - then escape slashes and quotes to pass it in
                    Query = string.Format("((Status:3 OR Status:4) AND (ProcessedAt:[ * TO {0} ]))", currentExpiryThresholdTime.ToString("O").Replace(":", @"\:").Replace("-", @"\-"))
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
                timer.Change(deleteFrequency, Timeout.InfiniteTimeSpan);
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
