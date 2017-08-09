using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.Net;

namespace PushSharp.Core
{
    public class ServiceBroker<TNotification> : IServiceBroker<TNotification> where TNotification : INotification
    {
        static ServiceBroker ()
        {
            ServicePointManager.DefaultConnectionLimit = 100;
            ServicePointManager.Expect100Continue = false;
        }

        public ServiceBroker (IServiceConnectionFactory<TNotification> connectionFactory)
        {
            ServiceConnectionFactory = connectionFactory;

            lockWorkers = new object ();
            workers = new List<ServiceWorker<TNotification>> ();
            running = false;

            notifications = new ConcurrentQueue<TNotification> ();
            ScaleSize = 1;
            //AutoScale = true;
            //AutoScaleMaxSize = 20;
        }

        public event NotificationSuccessDelegate<TNotification> OnNotificationSucceeded;
        public event NotificationFailureDelegate<TNotification> OnNotificationFailed;

        //public bool AutoScale { get; set; }
        //public int AutoScaleMaxSize { get; set; }
        public int ScaleSize { get; private set; }

        public IServiceConnectionFactory<TNotification> ServiceConnectionFactory { get; set; }

        ConcurrentQueue<TNotification> notifications;
        List<ServiceWorker<TNotification>> workers;
        object lockWorkers;
        bool running;

        public virtual void QueueNotification (TNotification notification)
        {
            notifications.Enqueue (notification);
        }

        public TNotification TakeNotification ()
        {
            TNotification tn = default(TNotification);
            if(notifications.TryDequeue(out tn) && tn != null)
            {
                return tn;
            }

            return default(TNotification);
        }

        public bool IsCompleted {
            get { return notifications.Count <= 0; }
        }

        public void Start ()
        {
            if (running)
                return;

            running = true;
            ChangeScale (ScaleSize);
        }

        public void Stop (bool immediately = false)
        {
            if (!running)
                return;

            running = false;

            //notifications.CompleteAdding ();

            lock (lockWorkers) {
                // Kill all workers right away
                //if (immediately)
                    workers.ForEach (sw => sw.Cancel ());
					
                var all = (from sw in workers
                                       select sw.WorkerTask).ToArray ();

                Log.Info ("Stopping: Waiting on Tasks");

                Task.WaitAll (all);

                Log.Info ("Stopping: Done Waiting on Tasks");

                workers.Clear ();
            }
        }

        public void ChangeScale (int newScaleSize)
        {
            if (newScaleSize <= 0)
                throw new ArgumentOutOfRangeException ("newScaleSize", "Must be Greater than Zero");

            ScaleSize = newScaleSize;

            if (!running)
                return;

            lock (lockWorkers) {

                // Scale down
                while (workers.Count > ScaleSize) {
                    workers [0].Cancel ();
                    workers.RemoveAt (0);
                }

                // Scale up
                while (workers.Count < ScaleSize) {
                    var worker = new ServiceWorker<TNotification> (this, ServiceConnectionFactory.Create ());
                    workers.Add (worker);
                    worker.Start ();
                }

                Log.Debug ("Scaled Changed to: " + workers.Count);
            }
        }

        public void RaiseNotificationSucceeded (TNotification notification)
        {
            var evt = OnNotificationSucceeded;
            if (evt != null)
                evt (notification);
        }

        public void RaiseNotificationFailed (TNotification notification, AggregateException exception)
        {
            var evt = OnNotificationFailed;
            if (evt != null)
                evt (notification, exception);
        }
    }

    class ServiceWorker<TNotification> where TNotification : INotification
    {
        public ServiceWorker (IServiceBroker<TNotification> broker, IServiceConnection<TNotification> connection)
        {
            Broker = broker;
            Connection = connection;

            CancelTokenSource = new CancellationTokenSource ();
        }

        public IServiceBroker<TNotification> Broker { get; private set; }

        public IServiceConnection<TNotification> Connection { get; private set; }

        public CancellationTokenSource CancelTokenSource { get; private set; }

        public Task WorkerTask { get; private set; }

        private bool TaskStarted = false;
        private bool ShouldCancel = false;

        public void Start ()
        {
            WorkerTask = Task.Factory.StartNew (async delegate {
                TaskStarted = true;
                if(ShouldCancel)
                {
                    CancelTokenSource.Cancel();
                }

                while (!CancelTokenSource.IsCancellationRequested || !Broker.IsCompleted) {

                    try {
                       
                        var toSend = new List<Task> ();
                        while(!Broker.IsCompleted)
                        {
                            var n = Broker.TakeNotification();
                            if (n == null)
                                break;

                            var t = Connection.Send(n);
                            // Keep the continuation
                            var cont = t.ContinueWith(ct => {
                                var cn = n;
                                var ex = t.Exception;

                                if (ex == null)
                                    Broker.RaiseNotificationSucceeded(cn);
                                else
                                    Broker.RaiseNotificationFailed(cn, ex);
                            });

                            // Let's wait for the continuation not the task itself
                            toSend.Add(cont);
                        }

                        if (toSend.Count <= 0)
                        {
                            await Task.Delay(100);
                            continue;
                        }
                                               
                        try {
                            Log.Info ("Waiting on all tasks {0}", toSend.Count ());
                            await Task.WhenAll (toSend).ConfigureAwait (false);
                            Log.Info ("All Tasks Finished");
                        } catch (Exception ex) {
                            Log.Error ("Waiting on all tasks Failed: {0}", ex);

                        }
                        Log.Info ("Passed WhenAll");

                    } catch (Exception ex) {
                        Log.Error ("Broker.Take: {0}", ex);
                    }
                }

                if (CancelTokenSource.IsCancellationRequested)
                    Log.Info ("Cancellation was requested");
                if (Broker.IsCompleted)
                    Log.Info ("Broker IsCompleted");

                Log.Debug ("Broker Task Ended");
            }, CancelTokenSource.Token, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap ();
				
            WorkerTask.ContinueWith (t => {
                var ex = t.Exception;
                if (ex != null)
                    Log.Error ("ServiceWorker.WorkerTask Error: {0}", ex);
            }, TaskContinuationOptions.OnlyOnFaulted);              
        }

        public void Cancel ()
        {
            if (TaskStarted)
                CancelTokenSource.Cancel();
            else
                ShouldCancel = true;
        }
    }
}

