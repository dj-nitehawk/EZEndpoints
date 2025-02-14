using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using FastEndpoints.Messaging.Jobs;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FastEndpoints;

abstract class JobQueueBase
{
    //key: tCommand
    //val: job queue for the command type
    //values get created when the DI container resolves each job queue type and the ctor is run.
    //see ctor in JobQueue<TCommand, TStorageRecord, TStorageProvider>
    protected static readonly ConcurrentDictionary<Type, JobQueueBase> JobQueues = new();

    protected abstract IJobStorageRecord CreateJob(ICommandBase command, DateTime? executeAfter, DateTime? expireOn);

    protected abstract void TriggerJob();

    protected abstract Task<Guid> StoreJobAsync(ICommandBase command, DateTime? executeAfter, DateTime? expireOn, CancellationToken ct);

    protected abstract Task CancelJobAsync(Guid trackingId, CancellationToken ct);

    protected abstract Task<TResult?> GetJobResultAsync<TResult>(Guid trackingId, CancellationToken ct);

    protected abstract Task StoreJobResultAsync<TResult>(Guid trackingId, TResult result, CancellationToken ct);

    internal abstract void SetLimits(int concurrencyLimit, TimeSpan executionTimeLimit, TimeSpan semWaitLimit);

    internal static TStorageRecord CreateJob<TStorageRecord>(ICommandBase command, DateTime? executeAfter, DateTime? expireOn)
        where TStorageRecord : class, IJobStorageRecord, new()
    {
        var tCommand = command.GetType();

        return !JobQueues.TryGetValue(tCommand, out var queue)
                   ? throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]")
                   : (TStorageRecord)queue.CreateJob(command, executeAfter, expireOn);
    }

    internal static void TriggerJobExecution(ICommandBase command)
    {
        var tCommand = command.GetType();

        if (!JobQueues.TryGetValue(tCommand, out var queue))
            throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]");

        queue.TriggerJob();
    }

    internal static Task<Guid> AddToQueueAsync(ICommandBase command, DateTime? executeAfter, DateTime? expireOn, CancellationToken ct)
    {
        var tCommand = command.GetType();

        return !JobQueues.TryGetValue(tCommand, out var queue)
                   ? throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]")
                   : queue.StoreJobAsync(command, executeAfter, expireOn, ct);
    }

    internal static Task CancelJobAsync<TCommand>(Guid trackingId, CancellationToken ct) where TCommand : ICommandBase
    {
        var tCommand = typeof(TCommand);

        return !JobQueues.TryGetValue(tCommand, out var queue)
                   ? throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]")
                   : queue.CancelJobAsync(trackingId, ct);
    }

    internal static Task<TResult?> GetJobResultAsync<TCommand, TResult>(Guid trackingId, CancellationToken ct) where TCommand : ICommandBase
    {
        var tCommand = typeof(TCommand);
        var tResult = tCommand.GetInterface(typeof(ICommand<>).Name)?.GetGenericArguments()[0];

        if (tResult == Types.VoidResult)
            throw new InvalidOperationException($"Job results are not supported with commands that don't return a result! Offending command: [{tCommand.FullName}]");

        return !JobQueues.TryGetValue(tCommand, out var queue)
                   ? throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]")
                   : queue.GetJobResultAsync<TResult>(trackingId, ct);
    }

    internal static Task StoreJobResultAsync<TCommand, TResult>(Guid trackingId, TResult result, CancellationToken ct)
        where TCommand : ICommandBase
        where TResult : IJobResult
    {
        var tCommand = typeof(TCommand);
        var tResult = tCommand.GetInterface(typeof(ICommand<>).Name)?.GetGenericArguments()[0];

        if (tResult == Types.VoidResult)
            throw new InvalidOperationException($"Job results are not supported with commands that don't return a result! Offending command: [{tCommand.FullName}]");

        return
            !JobQueues.TryGetValue(tCommand, out var queue)
                ? throw new InvalidOperationException($"A job queue has not been registered for [{tCommand.FullName}]")
                : queue.StoreJobResultAsync(trackingId, result, ct);
    }
}

// instantiated by DI as singleton
[SuppressMessage("Reliability", "CA2016:Forward the \'CancellationToken\' parameter to methods"),
 SuppressMessage("ReSharper", "MethodSupportsCancellation"),
 SuppressMessage("ReSharper", "SuspiciousTypeConversion.Global")]
sealed class JobQueue<TCommand, TResult, TStorageRecord, TStorageProvider> : JobQueueBase
    where TCommand : class, ICommandBase
    where TStorageRecord : class, IJobStorageRecord, new()
    where TStorageProvider : class, IJobStorageProvider<TStorageRecord>
{
    static readonly Type _tCommand = typeof(TCommand);
    static readonly string _tCommandName = _tCommand.FullName!;

    //public due to: https://github.com/FastEndpoints/FastEndpoints/issues/468
    public static readonly string QueueID = _tCommandName.ToHash();

    readonly MemoryCache _cancellations = new(new MemoryCacheOptions());
    readonly ParallelOptions _parallelOptions = new() { MaxDegreeOfParallelism = Environment.ProcessorCount };
    readonly CancellationToken _appCancellation;
    readonly TStorageProvider _storage;
    readonly IJobResultProvider? _resultStorage;
    readonly SemaphoreSlim _sem = new(0);
    readonly ILogger _log;
    TimeSpan _executionTimeLimit;
    TimeSpan _cancellationExpiry;
    TimeSpan _semWaitLimit;
    bool _isInUse;

    public JobQueue(TStorageProvider storageProvider,
                    IHostApplicationLifetime appLife,
                    ILogger<JobQueue<TCommand, TResult, TStorageRecord, TStorageProvider>> logger)
    {
        JobQueues[_tCommand] = this;
        _storage = storageProvider;
        _resultStorage = storageProvider as IJobResultProvider;
        _appCancellation = appLife.ApplicationStopping;
        _parallelOptions.CancellationToken = _appCancellation;
        _log = logger;
        JobStorage<TStorageRecord, TStorageProvider>.Provider = _storage;
        JobStorage<TStorageRecord, TStorageProvider>.AppCancellation = _appCancellation;
    }

    internal override void SetLimits(int concurrencyLimit, TimeSpan executionTimeLimit, TimeSpan semWaitLimit)
    {
        _parallelOptions.MaxDegreeOfParallelism = concurrencyLimit;
        _executionTimeLimit = executionTimeLimit;
        _cancellationExpiry = executionTimeLimit + TimeSpan.FromHours(1);
        _semWaitLimit = semWaitLimit;
        _ = CommandExecutorTask();
    }

    protected override TStorageRecord CreateJob(ICommandBase command, DateTime? executeAfter, DateTime? expireOn)
    {
        if (executeAfter?.Kind is not null and not DateTimeKind.Utc ||
            expireOn?.Kind is not null and not DateTimeKind.Utc)
            throw new ArgumentException($"Only UTC dates are accepted for '{nameof(executeAfter)}' & '{nameof(expireOn)}' parameters!");

        var job = new TStorageRecord
        {
            TrackingID = Guid.NewGuid(),
            QueueID = QueueID,
            ExecuteAfter = executeAfter ?? DateTime.UtcNow,
            ExpireOn = expireOn ?? DateTime.UtcNow.AddHours(4)
        };

        if (command is IHasTrackingID cti)
            cti.TrackingID = job.TrackingID;

        if (command is IHasCommandType cct)
            cct.CommandType = _tCommandName;

        job.SetCommand((TCommand)command);

        return job;
    }

    protected override void TriggerJob()
    {
        _isInUse = true;
        _sem.Release();
    }

    protected override async Task<Guid> StoreJobAsync(ICommandBase command, DateTime? executeAfter, DateTime? expireOn, CancellationToken ct)
    {
        var job = CreateJob(command, executeAfter, expireOn);
        await _storage.StoreJobAsync(job, ct);
        TriggerJob();

        return job.TrackingID;
    }

    protected override async Task CancelJobAsync(Guid trackingId, CancellationToken ct)
    {
        _ = AttemptCancellationTask(trackingId, _cancellations, _cancellationExpiry);
        await _storage.CancelJobAsync(trackingId, ct);

        static async Task AttemptCancellationTask(Guid trackingId, MemoryCache cancellations, TimeSpan cancelExpiry)
        {
            var cts = cancellations.GetOrCreate<CancellationTokenSource?>(
                trackingId,
                cacheEntry =>
                {
                    cacheEntry.SlidingExpiration = cancelExpiry;

                    return null;
                });

            //this is probably unnecessary. but could come in handy in case there's some delays (or race condition) in
            //marking the job as complete via the storage provider and the job gets picked up for execution in the meantime.
            for (var i = 0; i < 10; i++)
            {
                if (cts is not null) //job execution has started and cts is available
                {
                    if (!cts.IsCancellationRequested)
                        cts.Cancel(false);

                    return; //we're done!
                }
                await Task.Delay(1000);
            }
        }
    }

    protected override Task<TRes?> GetJobResultAsync<TRes>(Guid trackingId, CancellationToken ct) where TRes : default
    {
        if (_storage is not IJobResultProvider s)
            throw new NotSupportedException($"Please implement the  interface '{nameof(IJobResultProvider)}' on the job storage provider!");

        var tRes = typeof(TRes);
        var tResult = typeof(TResult);
        var cmdName = typeof(TCommand).FullName;

        if (tRes != tResult)
        {
            throw new InvalidOperationException(
                $"The correct result type for the command [{cmdName}] should be: [{tResult.FullName}]! You specified: [{tRes.FullName}]!");
        }

        return s.GetJobResultAsync<TRes>(trackingId, ct);
    }

    protected override Task StoreJobResultAsync<TRes>(Guid trackingId, TRes result, CancellationToken ct) where TRes : default
    {
        if (_storage is not IJobResultProvider s)
            throw new NotSupportedException($"Please implement the  interface '{nameof(IJobResultProvider)}' on the job storage provider!");

        var tRes = typeof(TRes);
        var tResult = typeof(TResult);
        var cmdName = typeof(TCommand).FullName;

        if (tRes != tResult)
        {
            throw new InvalidOperationException(
                $"The correct result type for the command [{cmdName}] should be: [{tResult.FullName}]! You specified: [{tRes.FullName}]!");
        }

        return s.StoreJobResultAsync(trackingId, result, ct);
    }

    async Task CommandExecutorTask()
    {
        while (!_appCancellation.IsCancellationRequested)
        {
            IEnumerable<TStorageRecord> records;

            try
            {
                records = await _storage.GetNextBatchAsync(
                              new()
                              {
                                  Limit = _parallelOptions.MaxDegreeOfParallelism,
                                  QueueID = QueueID,
                                  CancellationToken = _appCancellation,
                                  Match = r => r.QueueID == QueueID &&
                                               r.IsComplete == false &&
                                               DateTime.UtcNow >= r.ExecuteAfter &&
                                               DateTime.UtcNow <= r.ExpireOn
                              });
            }
            catch (Exception x)
            {
                _log.StorageRetrieveError(QueueID, _tCommandName, x.Message);
                await Task.Delay(5000);

                continue;
            }

            if (!records.Any())
            {
                // If _isInUse is false, it signifies that no job has been queued yet.
                // Therefore, there is no necessity for further iterations within the loop until the semaphore is released upon queuing the first job.
                //
                // If _isInUse is true, it indicates that we must await the queuing of the next job or the passage of delay duration (default: 1 minute), whichever comes first.
                // We must periodically reevaluate the storage status to ascertain if the user has rescheduled any previous jobs while no new jobs are being queued.
                // Failure to conduct this periodic check could result in rescheduled jobs only executing upon the arrival of new jobs, potentially leading to expired job executions.
                await (_isInUse
                           ? Task.WhenAny(_sem.WaitAsync(_appCancellation), Task.Delay(_semWaitLimit))
                           : _sem.WaitAsync(_appCancellation));
            }
            else
                await Parallel.ForEachAsync(records, _parallelOptions, ExecuteCommand);
        }

        async ValueTask ExecuteCommand(TStorageRecord record, CancellationToken _)
        {
            using var cts = _cancellations.GetOrCreate<CancellationTokenSource?>(
                record.TrackingID,
                e =>
                {
                    e.AbsoluteExpirationRelativeToNow = _cancellationExpiry;
                    var s = CancellationTokenSource.CreateLinkedTokenSource(_appCancellation);
                    s.CancelAfter(_executionTimeLimit);

                    return s;
                });

            if (cts is null) //don't execute this job because cancellation has been requested already
                return;      //the cts will be null if the entry was created by a call to CancelJobAsync() before the job was picked up for execution

            //if cts is not null, proceed with job execution as cancellation has not been requested yet.

            try
            {
                var cmd = record.GetCommand<TCommand>();
                record.Command = cmd; //needed in case user does whole record (non-partial) updates via storage provider.

                switch (cmd)
                {
                    case ICommand c:
                        await c.ExecuteAsync(cts.Token);

                        break;
                    case ICommand<TResult> cr:
                        ((IJobResultStorage)record).SetResult(await cr.ExecuteAsync(cts.Token));

                        break;
                }

                _cancellations.Remove(record.TrackingID); //remove entry on completion. cancellations are not possible/valid after this point.
            }
            catch (Exception x)
            {
                _cancellations.Remove(record.TrackingID); //remove entry on execution error to allow obtaining a new cts on retry/reentry
                _log.CommandExecutionCritical(_tCommandName, x.Message);

                while (true)
                {
                    try
                    {
                        if (_resultStorage is not null)
                            (record as IJobResultStorage)?.SetResult(await GetJobResultAsync<TResult>(record.TrackingID, _appCancellation));

                        await _storage.OnHandlerExecutionFailureAsync(record, x, _appCancellation);

                        break;
                    }
                    catch (Exception xx)
                    {
                        _log.StorageOnExecutionFailureError(QueueID, _tCommandName, xx.Message);

                        if (_appCancellation.IsCancellationRequested)
                            break;

                        await Task.Delay(5000);
                    }
                }

                return; //abort execution here
            }

            while (true)
            {
                try
                {
                    record.IsComplete = true;
                    await _storage.MarkJobAsCompleteAsync(record, _appCancellation);

                    break;
                }
                catch (Exception x)
                {
                    _log.StorageMarkAsCompleteError(QueueID, _tCommandName, x.Message);

                    if (_appCancellation.IsCancellationRequested)
                        break;

                    await Task.Delay(5000);
                }
            }

            while (_resultStorage is not null)
            {
                try
                {
                    await _resultStorage.StoreJobResultAsync(
                        record.TrackingID,
                        ((IJobResultStorage)record).GetResult<TResult>(),
                        _appCancellation);

                    break;
                }
                catch (Exception x)
                {
                    _log.StorageStoreJobResultError(QueueID, _tCommandName, x.Message);

                    if (_appCancellation.IsCancellationRequested)
                        break;

                    await Task.Delay(5000);
                }
            }
        }
    }
}