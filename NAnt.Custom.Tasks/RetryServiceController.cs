using System;
using System.ComponentModel;
using System.Globalization;
using System.ServiceProcess;

using NAnt.Core;
using NAnt.Core.Attributes;
using NAnt.Core.Util;
using System.Runtime.InteropServices;
using System.Threading;
using System.Diagnostics;

namespace NAnt.Custom.Tasks
{

    [TaskName("retryservicecontroller")]
    public class RetryServiceControllerTasks : Task
    {
        [DllImport("advapi32")]
        static extern bool QueryServiceStatusEx(IntPtr hService, int InfoLevel, ref SERVICE_STATUS_PROCESS lpBuffer, int cbBufSize, out int pcbBytesNeeded);

        const int SC_STATUS_PROCESS_INFO = 0;
        const int SERVICE_WIN32_OWN_PROCESS = 0x00000010;
        const int SERVICE_RUNS_IN_SYSTEM_PROCESS = 0x00000001;

        [StructLayout(LayoutKind.Sequential)]
        struct SERVICE_STATUS_PROCESS
        {
          public int dwServiceType;
          public int dwCurrentState;
          public int dwControlsAccepted;
          public int dwWin32ExitCode;
          public int dwServiceSpecificExitCode;
          public int dwCheckPoint;
          public int dwWaitHint;
          public int dwProcessId;
          public int dwServiceFlags;
        }

        class ProcessWaitForExitData
        {
          public Process Process;
          public volatile bool HasExited;
          public object Sync = new object();
        }

        static void ProcessWaitForExitThreadProc(object state)
        {
              ProcessWaitForExitData threadData = (ProcessWaitForExitData)state;
              try
              {
                  threadData.Process.WaitForExit();
              }
              catch {}
              finally
              {
                  lock (threadData.Sync)
                  {
                      threadData.HasExited = true;
                      Monitor.PulseAll(threadData.Sync);
                  }
              }
        }

        public void StopServiceAndWaitForExit(ServiceController serviceController)
        {
            SERVICE_STATUS_PROCESS ssp = new SERVICE_STATUS_PROCESS();
            int ignored;

            // Obtain information about the service, and specifically its hosting process,
            // from the Service Control Manager.
            if (!QueryServiceStatusEx(serviceController.ServiceHandle.DangerousGetHandle(), SC_STATUS_PROCESS_INFO, ref ssp, Marshal.SizeOf(ssp), out ignored))
                throw new ArgumentException("Couldn't obtain service process information.");

            // A few quick sanity checks that what the caller wants is *possible*.
            if (ssp.dwServiceType != SERVICE_WIN32_OWN_PROCESS)
                throw new ArgumentException("Can't wait for the service's hosting process to exit because there may be multiple services in the process (dwServiceType is not SERVICE_WIN32_OWN_PROCESS");

            if ((ssp.dwServiceFlags & SERVICE_RUNS_IN_SYSTEM_PROCESS) != 0)
                throw new ArgumentException("Can't wait for the service's hosting process to exit because the hosting process is a critical system process that will not exit (SERVICE_RUNS_IN_SYSTEM_PROCESS flag set)");

            if (ssp.dwProcessId == 0)
                throw new ArgumentException("Can't wait for the service's hosting process to exit because the process ID is not known.");

            // Note: It is possible for the next line to throw an ArgumentException if the
            // Service Control Manager's information is out-of-date (e.g. due to the process
            // having *just* been terminated in Task Manager) and the process does not really
            // exist. This is a race condition. The exception is the desirable result in this
            // case.
            using (Process process = Process.GetProcessById(ssp.dwProcessId, MachineName))
            {
                // EDIT: There is no need for waiting in a separate thread, because MSDN says "The handles are valid until closed, even after the process or thread they represent has been terminated." ( http://msdn.microsoft.com/en-us/library/windows/desktop/ms684868%28v=vs.85%29.aspx ),
                //so to keep things in the same thread, the process HANDLE should be opened from the process id before the service is stopped, 
                //and the Wait should be done after that.

                //Response to EDIT: What you report is true, but the problem is that the handle isn't actually opened by Process.GetProcessById
                //It's only opened within the .WaitForExit method, which won't return until the wait is complete. Thus, if we try the wait on the current thread,
                //we can't actually do anything until it's done, and if we defer the check until after the process has completed, it won't be possible to obtain a handle to it any more.

                // The actual wait, using process.WaitForExit, opens a handle with the SYNCHRONIZE
                // permission only and closes the handle before returning. As long as that handle
                // is open, the process can be monitored for termination, but if the process exits
                // before the handle is opened, it is no longer possible to open a handle to the
                // original process and, worse, though it exists only as a technicality, there is
                // a race condition in that another process could pop up with the same process ID.
                // As such, we definitely want the handle to be opened before we ask the service
                // to close, but since the handle's lifetime is only that of the call to WaitForExit
                // and while WaitForExit is blocking the thread we can't make calls into the SCM,
                // it would appear to be necessary to perform the wait on a separate thread.

                ProcessWaitForExitData threadData = new ProcessWaitForExitData();
                threadData.Process = process;

                Thread processWaitForExitThread = new Thread(ProcessWaitForExitThreadProc);

                processWaitForExitThread.IsBackground = Thread.CurrentThread.IsBackground;
                processWaitForExitThread.Start(threadData);


                // Instead of waiting until the *service* is in the "stopped" state, here we
                // wait for its hosting process to go away. Of course, it's really that other
                // thread waiting for the process to go away, and then we wait for the thread
                // to go away.

                int retryCount = 0;
                while (retryCount < _retryCount)
                {
                    try
                    {
                        // Now we ask the service to exit.
                        serviceController.Stop();
                    }
                    catch (Win32Exception ex)
                    {
                        Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Failed to stop service. Service {0}, machine {1}, exception: {2}", ServiceName, MachineName), ex.Message);
                    }
                    lock (threadData.Sync)
                    {
                        if (!threadData.HasExited)
                        {
                            Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Waiting for service stopping. Service {0}, machine {1}", ServiceName, MachineName));
                            if (!Monitor.Wait(threadData.Sync, TimeSpan.FromMilliseconds(Timeout)))
                            {
                                retryCount++;
                                if (retryCount >= _retryCount)
                                {
                                    throw new Exception(string.Format(CultureInfo.InvariantCulture, "Can't stop service {0}, machine {1}.", ServiceName, MachineName));
                                }
                                else
                                {
                                    Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Retrying to stop service due to timeout ({0} of {1}). Service {2}, machine {3}", retryCount, _retryCount + 1, ServiceName, MachineName));
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
        
        #region Public Instance Properties

        public enum ActionType {
            Start,
            Stop,
            Restart,
            Pause,
            Continue
        }

        public RetryServiceControllerTasks()
        {
            _exceptionThrower = (string resourceId, Exception ex) => 
            {
                string errorMessageTemplate = "Undefined error fo service {0} on computer '{1}'";
                switch (resourceId)
                {
                    case "NA3007":
                        errorMessageTemplate = "Cannot start service {0} on computer '{1}'.";
                        break;
                    case "NA3008":
                        errorMessageTemplate = "Cannot stop service {0} on computer '{1}'.";
                        break;
                    case "NA3009":
                        errorMessageTemplate = "Cannot pause service {0} on computer '{1}'.";
                        break;
                    case "NA3010":
                        errorMessageTemplate = "Cannot pause service {0} on computer '{1}' as its not currently started.";
                        break;
                    case "NA3011":
                        errorMessageTemplate = "Cannot pause service {0} on computer '{1}' as it does not support the pause and continue mechanism.";
                        break;
                    case "NA3012":
                        errorMessageTemplate = "Cannot continue service {0} on computer '{1}'.";
                        break;
                    case "NA3013":
                        errorMessageTemplate = "Cannot continue service {0} on computer '{1}' as its not currently started.";
                        break;
                    case "NA3014":
                        errorMessageTemplate = "Cannot continue service {0} on computer '{1}' as it does not support the pause and continue mechanism.";
                        break;
                    case "NA3015":
                        errorMessageTemplate = "Cannot continue service {0} on computer '{1}' as its not currently running or paused.";
                        break;
                    default:
                        break;
                }
                if (null == ex)
                {
                    throw new BuildException(string.Format(CultureInfo.InvariantCulture, errorMessageTemplate, ServiceName, MachineName), Location);
                }
                else
                {
                    throw new BuildException(string.Format(CultureInfo.InvariantCulture, errorMessageTemplate, ServiceName, MachineName), Location, ex);
                }
            };
        }

        /// <summary>
        /// The name of the service that should be controlled.
        /// </summary>
        [TaskAttribute("service", Required=true)]
        [StringValidator(AllowEmpty=false)]
        public string ServiceName {
            get { return _serviceName; }
            set { _serviceName = StringUtils.ConvertEmptyToNull(value); }
        }

        /// <summary>
        /// The name of the computer on which the service resides. The default
        /// is the local computer.
        /// </summary>
        [TaskAttribute("machine")]
        public string MachineName {
            get { return (_machineName == null) ? "." : _machineName; }
            set { _machineName = StringUtils.ConvertEmptyToNull(value); }
        }

        /// <summary>
        /// The action that should be performed on the service.
        /// </summary>
        [TaskAttribute("action", Required=true)]
        public ActionType Action {
            get { return _action; }
            set { _action = value; }
        }

        /// <summary>
        /// The time, in milliseconds, the task will wait for the service to
        /// reach the desired status. The default is 5000 milliseconds.
        /// </summary>
        [TaskAttribute("timeout", Required=false)]
        public double Timeout {
            get { return _timeout; }
            set { _timeout = value; }
        }

        /// <summary>
        /// The amount of retries that will be executed to perform the action
        /// </summary>
        [TaskAttribute("retryCount", Required = false)]
        public int RetryCount
        {
            get { return _retryCount; }
            set { _retryCount = value; }
        }

        #endregion Public Instance Properties

        #region Override implementation of Task

        /// <summary>
        /// Performs actions on the service in order to reach the desired status.
        /// </summary>
        protected override void ExecuteTask() {
            // get handle to service
            using (ServiceController serviceController = new ServiceController(ServiceName, MachineName))
            {
                // determine desired status
                ServiceControllerStatus desiredStatus = DetermineDesiredStatus();

                try {
                    // determine current status, this is also verifies if the service 
                    // is available
                    ServiceControllerStatus currentStatus = serviceController.Status;
                } catch (Exception ex) {
                    throw new BuildException(ex.Message, Location, ex.InnerException);
                }

                // we only need to take action if the service status differs from 
                // the desired status or if the service should be restarted
                if (serviceController.Status != desiredStatus || Action == ActionType.Restart) {
                    switch (Action) {
                        case ActionType.Start:
                            StartService(serviceController);
                            break;
                        case ActionType.Pause:
                            PauseService(serviceController);
                            break;
                        case ActionType.Continue:
                            ContinueService(serviceController);
                            break;
                        case ActionType.Stop:
                            StopService(serviceController);
                            break;
                        case ActionType.Restart:
                            RestartService(serviceController);
                            break;
                    }

                    // refresh current service status
                    serviceController.Refresh();
                }
            }
        }

        #endregion Override implementation of Task

        #region Private Instance Methods 

        /// <summary>
        /// Determines the desired status of the service based on the action
        /// that should be performed on it.
        /// </summary>
        /// <returns>
        /// The <see cref="ServiceControllerStatus" /> that should be reached
        /// in order for the <see cref="Action" /> to be considered successful.
        /// </returns>
        private ServiceControllerStatus DetermineDesiredStatus() {
            switch (Action) {
                case ActionType.Stop:
                    return ServiceControllerStatus.Stopped;
                case ActionType.Pause:
                    return ServiceControllerStatus.Paused;
                default:
                    return ServiceControllerStatus.Running;
            }
        }

        private bool IsPendingStatus(ServiceControllerStatus status)
        {
            return (ServiceControllerStatus.ContinuePending == status) ||
                (ServiceControllerStatus.PausePending == status) ||
                (ServiceControllerStatus.StartPending == status) ||
                (ServiceControllerStatus.StopPending == status);
        }

        private ServiceControllerStatus DeterminePendingFinalStatus(ServiceControllerStatus status)
        {
            switch (status)
            {
                case ServiceControllerStatus.ContinuePending:
                    return ServiceControllerStatus.Running;
                case ServiceControllerStatus.PausePending:
                    return ServiceControllerStatus.Paused;
                case ServiceControllerStatus.StartPending:
                    return ServiceControllerStatus.Running;
                case ServiceControllerStatus.StopPending:
                    return ServiceControllerStatus.Stopped;
                default:
                    return ServiceControllerStatus.Running;
            }
        }

        private void RetryExecutor(Action task, Action<Exception> exceptionThrower)
        {
            for (int i = 0; i < _retryCount; i++)
            {
                try
                {
                    task();
                    break;
                }
                catch (InvalidOperationException ex)
                {
                    //The Service cannot accept control messages at this time.
                    int hResult = Marshal.GetHRForException(ex);
                    if (//(ex.NativeErrorCode == 1061) && 
                        ((uint)hResult == 0x80070425) && (i < _retryCount))
                    {
                        Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} reports that cannot accept messages. Waiting.", ServiceName, MachineName));
                        Thread.Sleep(TimeSpan.FromMilliseconds(Timeout));
                        continue;
                    }
                    throw ex;
                }
                catch (System.ServiceProcess.TimeoutException ex)
                {
                    if (i == _retryCount - 1)
                    {
                        exceptionThrower(ex);
                    }
                    else
                    {
                        Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "{0} Retrying the last task due to timeout ({1} of {2}). Service {3}, machine {4}",
                            DateTime.Now.ToString("s", CultureInfo.InvariantCulture), i, _retryCount, ServiceName, MachineName));
                    }
                }
            }
        }

        private void CheckAndAwaitPendingStatus(ServiceController serviceController)
        {
            ServiceControllerStatus currentStatus = serviceController.Status;
            if (IsPendingStatus(currentStatus))
            {
                ServiceControllerStatus pendingFinalStatus = DeterminePendingFinalStatus(currentStatus);
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Waiting for transitioning from {0} to {1}. Service {2}, machine {3}", currentStatus, pendingFinalStatus, ServiceName, MachineName));
                RetryExecutor(() =>
                {
                    serviceController.WaitForStatus(pendingFinalStatus, TimeSpan.FromMilliseconds(Timeout));
                },
                (Exception ex) =>
                {
                    throw new BuildException(string.Format(CultureInfo.InvariantCulture, "Cannot transition to the final state {0} from the pending state {1}. Service {2}, machine {3}", pendingFinalStatus, currentStatus, ServiceName, MachineName), Location, ex);
                });
            }
        }

        /// <summary>
        /// Starts the service identified by <see cref="ServiceName" /> and
        /// <see cref="MachineName" />.
        /// </summary>
        /// <param name="serviceController"><see cref="ServiceController" /> instance for controlling the service identified by <see cref="ServiceName" /> and <see cref="MachineName" />.</param>
        private void StartService(ServiceController serviceController)
        {
            try
            {
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Starting service. Service {0}, machine {1}, status {2}", ServiceName, MachineName, serviceController.Status));
                RetryExecutor(() =>
                {
                    CheckAndAwaitPendingStatus(serviceController);
                    if (ServiceControllerStatus.Running != serviceController.Status)
                    {
                        if (serviceController.Status == ServiceControllerStatus.Paused)
                        {
                            serviceController.Continue();
                            serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(Timeout));
                        }
                        else
                        {
                            serviceController.Start();
                            serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(Timeout));
                        }
                    }
                }, (Exception ex) =>
                {
                    _exceptionThrower("NA3007", ex);
                });
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} has been started.", ServiceName, MachineName));
            }
            catch (Exception ex)
            {
                Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Starting service {0}, machine {1}, exception {2}, type {3}, HResult {4}",
                    ServiceName, MachineName, ex.Message, ex.GetType().ToString(), Marshal.GetHRForException(ex)));
                _exceptionThrower("NA3007", ex);
            }
        }

        /// <summary>
        /// Stops the service identified by <see cref="ServiceName" /> and
        /// <see cref="MachineName" />.
        /// </summary>
        /// <param name="serviceController"><see cref="ServiceController" /> instance for controlling the service identified by <see cref="ServiceName" /> and <see cref="MachineName" />.</param>
        private void StopService(ServiceController serviceController) 
        {
            try 
            {
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Stopping service. Service {0}, machine {1}, status {2}", ServiceName, MachineName, serviceController.Status));
                RetryExecutor(() =>
                {
                    CheckAndAwaitPendingStatus(serviceController);
                    if (ServiceControllerStatus.Stopped != serviceController.Status)
                    {
                        if (!serviceController.CanStop)
                        {
                            Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} reports that cannot be stopped. Waiting.", ServiceName, MachineName));
                            Thread.Sleep(TimeSpan.FromMilliseconds(Timeout));
                        }
                        else
                        {
                            serviceController.Stop();
                            serviceController.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromMilliseconds(Timeout));
                        }
                    }
                }, (Exception ex) =>
                {
                    _exceptionThrower("NA3008", ex);
                });
/*
                    try
                    {
                        StopServiceAndWaitForExit(serviceController);
                    }
                    catch (System.ArgumentException ex)
                    {
                        //
                    }
*/
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} has been stopped.", ServiceName, MachineName));
            }
            catch (BuildException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Stopping service {0}, machine {1}, exception {2}, type {3}, HResult {4}",
                    ServiceName, MachineName, ex.Message, ex.GetType().ToString(), Marshal.GetHRForException(ex)));
                _exceptionThrower("NA3008", ex);
            }
        }

        /// <summary>
        /// Restarts the service identified by <see cref="ServiceName" /> and
        /// <see cref="MachineName" />.
        /// </summary>
        /// <param name="serviceController"><see cref="ServiceController" /> instance for controlling the service identified by <see cref="ServiceName" /> and <see cref="MachineName" />.</param>
        private void RestartService(ServiceController serviceController) {
            // only stop service if its not already stopped
            if (serviceController.Status != ServiceControllerStatus.Stopped)
            {
                StopService(serviceController);
            }
            // start the service
            StartService(serviceController);
        }

        /// <summary>
        /// Pauses the service identified by <see cref="ServiceName" /> and
        /// <see cref="MachineName" />.
        /// </summary>
        /// <param name="serviceController"><see cref="ServiceController" /> instance for controlling the service identified by <see cref="ServiceName" /> and <see cref="MachineName" />.</param>
        private void PauseService(ServiceController serviceController)
        {
            try
            {
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Suspending service. Service {0}, machine {1}", ServiceName, MachineName));
                RetryExecutor(() =>
                {
                    CheckAndAwaitPendingStatus(serviceController);
                    ServiceControllerStatus currentStatus = serviceController.Status;
                    if (currentStatus != ServiceControllerStatus.Paused)
                    {
                        if (serviceController.Status == ServiceControllerStatus.Running)
                        {
                            if (serviceController.CanPauseAndContinue)
                            {
                                // Original and strange NAnt code
                                if (serviceController.Status != ServiceControllerStatus.Running)
                                {
                                    _exceptionThrower("NA3010", null);
                                }
                                else
                                {
                                    serviceController.Pause();
                                }
                            }
                            else
                            {
                                _exceptionThrower("NA3011", null);
                            }
                        }
                        else
                        {
                            _exceptionThrower("NA3010", null);
                        }
                        serviceController.WaitForStatus(ServiceControllerStatus.Paused, TimeSpan.FromMilliseconds(Timeout));
                    }
                }, (Exception ex) => {
                    _exceptionThrower("NA3009", ex);
                });
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} has been suspended.", ServiceName, MachineName));
            }
            catch (BuildException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                _exceptionThrower("NA3009", ex);
            }
        }

        /// <summary>
        /// Continues the service identified by <see cref="ServiceName" /> and
        /// <see cref="MachineName" />.
        /// </summary>
        /// <param name="serviceController"><see cref="ServiceController" /> instance for controlling the service identified by <see cref="ServiceName" /> and <see cref="MachineName" />.</param>
        private void ContinueService(ServiceController serviceController)
        {
            try
            {
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Resuming service. Service {0}, machine {1}", ServiceName, MachineName));
                RetryExecutor(() =>
                {
                    CheckAndAwaitPendingStatus(serviceController);
                    ServiceControllerStatus currentStatus = serviceController.Status;
                    if (currentStatus != ServiceControllerStatus.Running)
                    {
                        if (serviceController.Status == ServiceControllerStatus.Paused)
                        {
                            if (serviceController.CanPauseAndContinue)
                            {
                                if (serviceController.Status == ServiceControllerStatus.Paused)
                                {
                                    serviceController.Continue();
                                }
                                // Original and strange NAnt code
                                else if (serviceController.Status != ServiceControllerStatus.Running)
                                {
                                    _exceptionThrower("NA3013", null);
                                }
                                else
                                {
                                }
                            }
                            else
                            {
                                _exceptionThrower("NA3014", null);
                            }
                        }
                        else
                        {
                            _exceptionThrower("NA3015", null);
                        }
                        serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(Timeout));
                    }
                }, (Exception ex) =>
                {
                    _exceptionThrower("NA3012", ex);
                });
                Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Service {0}, machine {1} has been resumed.", ServiceName, MachineName));
            }
            catch (BuildException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                _exceptionThrower("NA3012", ex);
            }
        }

        #endregion Private Instance Methods 

        #region Private Instance Fields

        /// <summary>
        /// Holds the name of the service that should be controlled.
        /// </summary>
        private string _serviceName;

        /// <summary>
        /// Holds the name of the computer on which the service resides.
        /// </summary>
        private string _machineName;

        /// <summary>
        /// Holds the action that should be performed on the service.
        /// </summary>
        private ActionType _action;

        /// <summary>
        /// Holds the time, in milliseconds, the task will wait for a service
        /// to reach the desired status.
        /// </summary>
        private double _timeout = 5000;

        private int _retryCount = 1;

        private Action<string, Exception> _exceptionThrower;

        #endregion Private Instance Fields
    }
} 
