namespace Com.MinorD.Tools
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    public class FolderComparer
    {
        private const string DateTimeFormatTemplate = "yyyy-MM-ddTHH:mm:ss.fff";
        private readonly string sourceFolder;
        private readonly string targetFolder;
        private readonly string fileSearchPattern;
        private readonly string fromFile;
        private readonly string logFolder;
        private readonly IFolderComparerConfig config;
        
        public FolderComparer(string sourceFolder, string targetFolder, string fileSearchPattern, string fromFile, string logFolder, IFolderComparerConfig config)
        {
            this.sourceFolder = sourceFolder;
            this.targetFolder = targetFolder;
            this.fileSearchPattern = fileSearchPattern;
            this.fromFile = fromFile;
            this.logFolder = logFolder;
            this.config = config;
        }

        public void CompareFolder(CancellationTokenSource cancellationTokenSource)
        {
            this.CompareFolderAsync(cancellationTokenSource).Wait();
        }

        public async Task CompareFolderAsync(CancellationTokenSource cancellationTokenSource)
        {            
            var fileNameKey = DateTime.UtcNow.ToString("yyyy_MM_ddTHH_mm_ss_fff");
            using (var processLogger = new Logger(Path.Combine(this.logFolder, $"{fileNameKey}_P.log"), this.config))
            {
                using (var compareLogger = new Logger(Path.Combine(this.logFolder, $"{fileNameKey}_C.log"), this.config))
                {
                    Stopwatch totalTimeStopwatch = new Stopwatch();
                    totalTimeStopwatch.Start();
                    long totalBytesCompared = 0;
                    TimeSpan lastCheckTimespan = totalTimeStopwatch.Elapsed;
                    object syncRoot = new object();
                    await WriteProgressAsync(processLogger, $"Start comparing at folder \"{this.sourceFolder}\" and \"{this.targetFolder}\".");
                    
                    try
                    {
                        using (SemaphoreSlim semaphoreSlim = new SemaphoreSlim(this.config.MaxConcurrency, this.config.MaxConcurrency + 1))
                        {
                            using (ManualResetEventSlim manualResetEvent = new ManualResetEventSlim(false))
                            {
                                bool foundFromFile = false;
                                foreach (var sourceFileTmp in Directory.EnumerateFiles(this.sourceFolder, this.fileSearchPattern, SearchOption.AllDirectories))
                                {
                                    if (cancellationTokenSource.Token.IsCancellationRequested)
                                    {
                                        break;
                                    }

                                    if(!foundFromFile && (string.IsNullOrEmpty(this.fromFile) || string.Equals(sourceFileTmp, this.fromFile, StringComparison.OrdinalIgnoreCase)))
                                    {
                                        foundFromFile = true;
                                    }

                                    if(!foundFromFile)
                                    {
                                        continue;
                                    }

                                    await semaphoreSlim.WaitAsync();
                                    var func = new Func<Task>(async () =>
                                    {
                                        var sourceFile = sourceFileTmp;
                                        string targetFile = string.Empty;
                                        Stopwatch sw = new Stopwatch();
                                        sw.Start();
                                        long totalBytesComparedPerFile = 0;
                                        try
                                        {
                                            if (cancellationTokenSource.Token.IsCancellationRequested)
                                            {
                                                return;
                                            }

                                            await WriteProgressAsync(processLogger, $"Start comparing file\"{sourceFile}:{targetFile}\".");
                                            var reletiveFilePath = GetRelativeFilePath(sourceFile, this.sourceFolder);
                                            targetFile = Path.Combine(this.targetFolder, reletiveFilePath);

                                            if (!File.Exists(targetFile))
                                            {
                                                await compareLogger.WriteLineAsync(string.Format($"-::{sourceFile}:{targetFile}"));
                                                return;
                                            }

                                            var sourceFileInfo = new FileInfo(sourceFile);
                                            var targetFileInfo = new FileInfo(targetFile);
                                            if (sourceFileInfo.Length != targetFileInfo.Length)
                                            {
                                                await WriteProgressAsync(processLogger, $"File length {sourceFileInfo.Length} of \"{sourceFile}\" is different from file lenght {targetFileInfo.Length} of {targetFile}.");
                                                await compareLogger.WriteLineAsync($"!::{sourceFile}:{targetFile}");
                                                return;
                                            }

                                            var sourceBuffer = new byte[this.config.BufferSize];
                                            var targetBuffer = new byte[this.config.BufferSize];
                                            try
                                            {
                                                bool same = true;
                                                FileStream sourceStream = null;
                                                try
                                                {
                                                    sourceStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read, FileShare.Read);
                                                }
                                                catch(Exception ex)
                                                {
                                                    await WriteProgressAsync(processLogger, $"Error opening source file\"{sourceFile}\". Exception: {ex.ToString()}.");                                                    
                                                }

                                                FileStream targetStream = null;

                                                try
                                                {
                                                    targetStream = new FileStream(targetFile, FileMode.Open, FileAccess.Read, FileShare.Read);
                                                }
                                                catch (Exception ex)
                                                {
                                                    await WriteProgressAsync(processLogger, $"Error opening target file\"{targetFile}\". Exception: {ex.ToString()}.");
                                                }

                                                using (sourceStream)
                                                using (targetStream)
                                                {
                                                    if (sourceStream == null || targetStream == null)
                                                    {
                                                        await compareLogger.WriteLineAsync($"*::{sourceFile}:{targetFile}");
                                                        return;
                                                    }


                                                    int sourceRead = 0;
                                                    do
                                                    {
                                                        sourceRead = await sourceStream.ReadAsync(sourceBuffer, 0, sourceBuffer.Length);
                                                        totalBytesComparedPerFile += sourceRead;
                                                        if (sourceRead > 0)
                                                        {
                                                            var targetRead = await (targetStream.ReadAsync(targetBuffer, 0, sourceRead));
                                                            if (sourceRead != targetRead)
                                                            {
                                                                await compareLogger.WriteLineAsync($"!::{sourceFile}:{targetFile}");
                                                                same = false;
                                                                break;
                                                            }

                                                            bool different = false;

                                                            for (int i = 0; i < sourceRead; i++)
                                                            {
                                                                if (sourceBuffer[i] != targetBuffer[i])
                                                                {
                                                                    different = true;
                                                                    break;
                                                                }
                                                            }

                                                            if (different)
                                                            {
                                                                await compareLogger.WriteLineAsync($"!::{sourceFile}:{targetFile}");
                                                                same = false;
                                                                break;
                                                            }
                                                        }

                                                        TryWriteProgress(totalTimeStopwatch, ref lastCheckTimespan, processLogger);
                                                        if (cancellationTokenSource.Token.IsCancellationRequested)
                                                        {
                                                            return;
                                                        }

                                                    } while (sourceRead > 0);
                                                }
                                                

                                                if (same)
                                                {
                                                    await compareLogger.WriteLineAsync($"=::{sourceFile}:{targetFile}");
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                await WriteProgressAsync(processLogger, $"Error comparing file\"{sourceFile}:{targetFile}\". Exception: {ex.ToString()}.");
                                                await compareLogger.WriteLineAsync($"*::{sourceFile}:{targetFile}");
                                            }
                                        }
                                        finally
                                        {
                                            sw.Stop();
                                            await WriteProgressAsync(processLogger, $"Finish comparing file\"{sourceFile}:{targetFile}\". Total bytes compared: {totalBytesComparedPerFile}. Time elasped {sw.Elapsed.TotalMilliseconds}.");
                                            Interlocked.Add(ref totalBytesCompared, totalBytesComparedPerFile);
                                            this.TryReleaseSemaphore(semaphoreSlim, manualResetEvent);
                                        }
                                    });

                                    ThreadPool.QueueUserWorkItem((state) =>
                                    {
                                        func();
                                    });
                                }

                                TryReleaseSemaphore(semaphoreSlim, manualResetEvent);
                                manualResetEvent.Wait();
                            }
                        }

                        foreach (var targetFile in Directory.EnumerateFiles(this.targetFolder, this.fileSearchPattern, SearchOption.AllDirectories))
                        {
                            if (cancellationTokenSource.Token.IsCancellationRequested)
                            {
                                break;
                            }

                            var reletiveFilePath = GetRelativeFilePath(targetFile, this.targetFolder);
                            var sourceFile = Path.Combine(this.sourceFolder, reletiveFilePath);

                            try
                            {
                                if (!File.Exists(sourceFile))
                                {
                                    await WriteProgressAsync(processLogger, $"Start comparing file\"{sourceFile}:{targetFile}\".");
                                    await compareLogger.WriteLineAsync($"+::{targetFile}");
                                    await WriteProgressAsync(processLogger, $"End comparing file\"{sourceFile}:{targetFile}\".");
                                }
                            }
                            catch (Exception ex)
                            {
                                await WriteProgressAsync(processLogger, $"Error comparing file\"{sourceFile}:{targetFile}\". Exception: {ex.ToString()}.");
                                await compareLogger.WriteLineAsync($"*::{sourceFile}:{targetFile}");
                            }

                            TryWriteProgress(totalTimeStopwatch, ref lastCheckTimespan, processLogger);
                        }                        
                    }
                    catch(Exception ex)
                    {
                        await WriteProgressAsync(processLogger, $"Error comparing at folder \"{this.sourceFolder}\" and \"{this.targetFolder}\". Excption: {ex.ToString()}.");                        
                        throw;
                    }
                    finally
                    {
                        totalTimeStopwatch.Stop();
                        await WriteProgressAsync(processLogger, $"Finish comparing at folder \"{this.sourceFolder}\" and \"{this.targetFolder}\". Total bytes compared: {totalBytesCompared}. Time elasped {totalTimeStopwatch.Elapsed.TotalMilliseconds}.");                        
                    }
                }
            }
        }

        private void TryReleaseSemaphore(SemaphoreSlim semaphoreSlim, ManualResetEventSlim manualResetEvent)
        {
            if(semaphoreSlim.Release() == this.config.MaxConcurrency )
            {
                manualResetEvent.Set();
            }
        }

        private static void TryWriteProgress(Stopwatch totalTimeStopwatch, ref TimeSpan lastCheckTimespan, Logger processLogger)
        {
            if ((totalTimeStopwatch.Elapsed - lastCheckTimespan).TotalMilliseconds > 1000)
            {
                lock (processLogger)
                {
                    if ((totalTimeStopwatch.Elapsed - lastCheckTimespan).TotalMilliseconds > 1000)
                    {
                        lastCheckTimespan = totalTimeStopwatch.Elapsed;
                        Console.Write(".");                        
                    }
                }
            }
        }
        
        private static async Task WriteProgressAsync(Logger processLogger, string logEntry)
        {
            logEntry = $"{DateTime.UtcNow.ToString(DateTimeFormatTemplate)}: {logEntry ?? string.Empty}";
            Console.WriteLine(logEntry);            

            await processLogger.WriteLineAsync(logEntry);
        }

        private static string GetRelativeFilePath(string file, string parentFolder)
        {
            file = file.Substring(parentFolder.Length);
            if(file.StartsWith("\\"))
            {
                file = file.Substring(1);
            }

            return file;
        }
    }
}
