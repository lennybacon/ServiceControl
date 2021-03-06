namespace ServiceControl.ExternalIntegrations
{
    using NServiceBus;

    public class ExternalIntegrationsInitializer : INeedInitialization
    {
        public void Init()
        {
            Configure.Component<MessageFailedPublisher>(DependencyLifecycle.SingleInstance);
            Configure.Component<HeartbeatStoppedPublisher>(DependencyLifecycle.SingleInstance);
            Configure.Component<HeartbeatRestoredPublisher>(DependencyLifecycle.SingleInstance);
        }
    }
}