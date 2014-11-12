namespace Particular.ServiceControl.Commands
{
    using System;
    using System.Threading;
    using Hosting;

    internal class RunCommand : AbstractCommand
    {
        public override void Execute(HostArguments args)
        {
            if (!Environment.UserInteractive)
            {
                using (var service = new Host{ServiceName = args.ServiceName})
                {
                    service.Run();
                }

                return;
            }

            using (var service = new Host{ ServiceName = args.ServiceName} )
            {
                using (var waitHandle = new ManualResetEvent(false))
                {
                    var serviceClosure = service;
                    var waitHandleClosure = waitHandle;
                    service.OnStopping = () =>
                    {
                        serviceClosure.OnStopping = () => { };
                        if (!waitHandleClosure.SafeWaitHandle.IsClosed)
                        {
                            waitHandleClosure.Set();
                        }
                    };

                    service.Run();

                    Console.CancelKeyPress += (sender, e) =>
                    {
                      serviceClosure.OnStopping = () => { };
                        e.Cancel = true;
                        if (!waitHandleClosure.SafeWaitHandle.IsClosed)
                        {
                            waitHandleClosure.Set();
                        }
                    };

                    Console.WriteLine("Press Ctrl+C to exit");
                    if (!waitHandleClosure.SafeWaitHandle.IsClosed)
                    {
                        waitHandleClosure.Set();
                    }
                }
            }
        }
    }
}