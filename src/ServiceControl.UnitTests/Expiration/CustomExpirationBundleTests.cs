﻿namespace ServiceControl.UnitTests.Expiration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Infrastructure.RavenDB;
    using MessageAuditing;
    using MessageFailures;
    using NUnit.Framework;
    using Raven.Client;
    using ServiceBus.Management.Infrastructure.Settings;
    using ServiceControl.CompositeViews.Messages;

    [TestFixture]
    public class CustomExpirationBundleTests : TestWithRavenDB
    {
        [Test]
        public void Processed_messages_are_being_expired()
        {
            var processedMessage = new ProcessedMessage
            {
                Id = "1",
                ProcessedAt = DateTime.UtcNow.AddHours(-(Settings.HoursToKeepMessagesBeforeExpiring * 3)),
            };

            var processedMessage2 = new ProcessedMessage
            {
                Id = "2",
                ProcessedAt = DateTime.UtcNow.AddHours(-(Settings.HoursToKeepMessagesBeforeExpiring * 2)),                
            };
            processedMessage2.MessageMetadata["IsSystemMessage"] = true;

            using (var session = documentStore.OpenSession())
            {
                session.Store(processedMessage);
                session.Store(processedMessage2);
                session.SaveChanges();
            }

            WaitForIndexing(documentStore);
            Thread.Sleep(Settings.ExpirationProcessTimerInSeconds * 1000 * 2);

            using (var session = documentStore.OpenSession())
            {
                var msg = session.Load<ProcessedMessage>(processedMessage.Id);
                Assert.Null(msg);

                msg = session.Load<ProcessedMessage>(processedMessage2.Id);
                Assert.Null(msg);
            }
        }

        [Test]
        public void Many_processed_messages_are_being_expired()
        {
            new MessagesViewIndex().Execute(documentStore);

            var processedMessage = new ProcessedMessage
            {
                Id = Guid.NewGuid().ToString(),
                ProcessedAt = DateTime.UtcNow.AddMinutes(-DateTime.UtcNow.Millisecond%30).AddDays(-(Settings.HoursToKeepMessagesBeforeExpiring*3)),
            };
            var processedMessage2 = new ProcessedMessage
            {
                Id = "2",
                ProcessedAt = DateTime.UtcNow,
            };
            
            using (var session = documentStore.OpenSession())
            {
                for (var i = 0; i < 2049; i++)
                {
                    processedMessage = new ProcessedMessage
                    {
                        Id = Guid.NewGuid().ToString(),
                        ProcessedAt = DateTime.UtcNow.AddMinutes(-DateTime.UtcNow.Millisecond%30).AddDays(-(Settings.HoursToKeepMessagesBeforeExpiring*3)),
                    };

                    session.Store(processedMessage);
                }

                session.Store(processedMessage2);
                session.SaveChanges();
            }

            WaitForIndexing(documentStore);
            Thread.Sleep(Settings.ExpirationProcessTimerInSeconds * 1000 * 4);

            using (var session = documentStore.OpenSession())
            {
                var results = session.Query<ProcessedMessage, MessagesViewIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .ToArray();
                Assert.AreEqual(1, results.Length);

                var msg = session.Load<ProcessedMessage>(processedMessage.Id);
                Assert.Null(msg, "Message with datestamp {0} and ID {1} was found", processedMessage.ProcessedAt, processedMessage.Id);

                msg = session.Load<ProcessedMessage>(processedMessage2.Id);
                Assert.NotNull(msg);
            }
        }

        [Test]
        public void Only_processed_messages_are_being_expired()
        {
            var processedMessage = new ProcessedMessage
            {
                Id = "1",
                ProcessedAt = DateTime.UtcNow.AddHours(-(Settings.HoursToKeepMessagesBeforeExpiring * 3)),
            };

            var processedMessage2 = new ProcessedMessage
            {
                Id = "2",
                ProcessedAt = DateTime.UtcNow,
            };
            processedMessage2.MessageMetadata["IsSystemMessage"] = true;

            using (var session = documentStore.OpenSession())
            {
                session.Store(processedMessage);
                session.Store(processedMessage2);
                session.SaveChanges();
            }

            WaitForIndexing(documentStore);
            Thread.Sleep(Settings.ExpirationProcessTimerInSeconds * 1000 * 2);

            using (var session = documentStore.OpenSession())
            {
                var msg = session.Load<ProcessedMessage>(processedMessage.Id);
                Assert.Null(msg);

                msg = session.Load<ProcessedMessage>(processedMessage2.Id);
                Assert.NotNull(msg);
            }
        }

        [Test]
        public void Recent_processed_messages_are_not_being_expired()
        {
            var processedMessage = new ProcessedMessage
            {
                Id = "1",
                ProcessedAt = DateTime.UtcNow,
            };

            using (var session = documentStore.OpenSession())
            {
                session.Store(processedMessage);
                session.SaveChanges();
            }

            WaitForIndexing(documentStore);
            Thread.Sleep(Settings.ExpirationProcessTimerInSeconds * 1000 * 2);

            using (var session = documentStore.OpenSession())
            {
                var msg = session.Load<ProcessedMessage>(processedMessage.Id);
                Assert.NotNull(msg);
            }
        }

        [Test]
        public void Errors_are_not_being_expired()
        {
            var failedMsg = new FailedMessage
                            {
                                Id = "1",
                                ProcessingAttempts = new List<FailedMessage.ProcessingAttempt>
                                                     {
                                                         new FailedMessage.ProcessingAttempt
                                                         {
                                                             AttemptedAt = DateTime.UtcNow.AddHours(-(Settings.HoursToKeepMessagesBeforeExpiring * 3))
                                                         },
                                                         new FailedMessage.ProcessingAttempt
                                                         {
                                                             AttemptedAt = DateTime.UtcNow.AddHours(-(Settings.HoursToKeepMessagesBeforeExpiring * 2)),
                                                         }
                                                     },
                                Status = FailedMessageStatus.Unresolved,
                            };

            using (var session = documentStore.OpenSession())
            {
                session.Store(failedMsg);
                session.SaveChanges();
            }

            WaitForIndexing(documentStore);
            Thread.Sleep(Settings.ExpirationProcessTimerInSeconds * 1000 * 2);

            using (var session = documentStore.OpenSession())
            {
                var msg = session.Load<FailedMessage>(failedMsg.Id);
                Assert.NotNull(msg);
            }
        }


        [SetUp]
        public void SetUp()
        {
            documentStore = InMemoryStoreBuilder.GetInMemoryStore(withExpiration: true);

            var customIndex = new MessagesViewIndex();
            customIndex.Execute(documentStore);

            var transformer = new MessagesViewTransformer();

            transformer.Execute(documentStore);
        }

        [TearDown]
        public void TearDown()
        {
            documentStore.Dispose();
        }

        IDocumentStore documentStore;
    }
}
