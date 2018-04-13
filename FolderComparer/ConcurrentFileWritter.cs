namespace Com.MinorD.Tools
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public sealed class ConcurrentFileWritter : IDisposable
    {
        private readonly object syncRoot = new object();
        private readonly string id;
        private readonly string fileName;
        private readonly FileStream fileStream;
        private readonly int bufferSize;
        private readonly int bufferCount;
        private readonly ConcurrentStack<byte[]> bufferRecycleBin;
        private readonly SemaphoreSlim maxBufferCountSemaphore;
        private readonly SemaphoreSlim flushSemaphore;
        private readonly SemaphoreSlim concurrentCallSemaphore;
        private readonly ManualResetEventSlim concurrentCallSemaphoreDisposeEvent;
        private readonly ManualResetEventSlim currentBufferReaderLock;

        private volatile Buffer bufferCurrent;
        private volatile Buffer bufferHead;

        private int disposedState;

        public ConcurrentFileWritter(string id, string fileName, int bufferSize, int bufferCount)
        {
            this.id = id;
            this.fileName = fileName;
            this.bufferSize = bufferSize;
            this.bufferCount = bufferCount >= 2 ? bufferCount : int.MaxValue;
            this.fileStream = new FileStream(this.fileName, FileMode.Create, FileAccess.Write, FileShare.Read);

            this.maxBufferCountSemaphore = new SemaphoreSlim(this.bufferCount - 1, this.bufferCount);
            this.flushSemaphore = new SemaphoreSlim(1, 1);
            this.concurrentCallSemaphore = new SemaphoreSlim(int.MaxValue - 1, int.MaxValue);
            this.concurrentCallSemaphoreDisposeEvent = new ManualResetEventSlim(false);
            this.currentBufferReaderLock = new ManualResetEventSlim(true);

            // We don't have to use Queue, however, we'd like to if we want to keep the log sequence.
            // Or we can configure the buffersize large enough so that it won't fill up very quickly.
            // However, it that's the case, the overhead of locking can queueing when creating new buffer
            // is trival as well.            
            this.bufferHead = this.bufferCurrent = new Buffer(this.bufferSize);
            this.bufferRecycleBin = new ConcurrentStack<byte[]>();
        }

        private bool Disposed
        {
            get
            {
                return Thread.VolatileRead(ref this.disposedState) != 0;
            }
        }

        public void Write(byte[] bytesToWrite)
        {
            this.WriteAsync(bytesToWrite).Wait();
        }

        public async Task WriteAsync(byte[] bytesToWrite)
        {
            if (bytesToWrite == null || bytesToWrite.Length == 0)
            {
                return;
            }

            if (this.Disposed)
            {
                throw new ObjectDisposedException(this.id, "Cannot write to an ConcurrentFileWritter which is disposing or has disposed.");
            }

            await this.concurrentCallSemaphore.WaitAsync();

            try
            {
                if (this.Disposed)
                {
                    throw new ObjectDisposedException(this.id, "Cannot write to an ConcurrentFileWritter which is disposing or has disposed.");
                }

                int writtenBytes = 0;
                do
                {
                    Buffer currentBuffer = null;

                    try
                    {
                        currentBuffer = this.GetCurrentBuffer();
                        var bufferLeft = currentBuffer.BufferLeft;
                        if (bufferLeft > 0)
                        {
                            var bytesToCopy = Math.Min(bufferLeft, bytesToWrite.Length - writtenBytes);
                            var currentBufferIndex = Interlocked.Add(ref currentBuffer.bufferCursor, bytesToCopy);
                            if (currentBufferIndex <= currentBuffer.bytes.Length)
                            {
                                int startIndex = currentBufferIndex - bytesToCopy;
                                Array.Copy(bytesToWrite, writtenBytes, currentBuffer.bytes, startIndex, bytesToCopy);
                                writtenBytes += bytesToCopy;

                                if (currentBufferIndex == currentBuffer.bytes.Length)
                                {
                                    this.currentBufferReaderLock.Reset();
                                    try
                                    {
                                        this.bufferCurrent = await this.CreateNewBufferAndFinishCurrentEntry(currentBuffer, bytesToWrite, writtenBytes);

                                        // set currentBuffer to null to skip the this.ReleaseBuffer in finally statement since in this situation, 
                                        // we've already released the buffer. 
                                        // The reason we cannot leverage this.ReleaseBuffer in finally is we need to release the buffer refCount before 
                                        // creating new buffer. Otherwise we may capped by the buffernumber limitation and have a deadload.
                                        currentBuffer = null;
                                    }
                                    finally
                                    {
                                        this.currentBufferReaderLock.Set();
                                    }

                                    break;
                                }

                                if (writtenBytes == bytesToWrite.Length)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                Interlocked.Add(ref currentBuffer.bufferCursor, -1 * bytesToCopy);
                            }
                        }
                    }
                    finally
                    {
                        if (currentBuffer != null)
                        {
                            await this.ReleaseBuffer(currentBuffer);
                        }
                    }
                } while (true);
            }
            finally
            {
                ReleaseSemaphore(this.concurrentCallSemaphore, this.concurrentCallSemaphoreDisposeEvent, int.MaxValue);
            }
        }

        public void Flush()
        {
            this.FlushAsync().Wait();
        }

        public async Task FlushAsync()
        {
            if (this.Disposed)
            {
                throw new ObjectDisposedException(this.id, "Cannot write to an ConcurrentFileWritter which is disposing or has disposed.");
            }

            try
            {
                await this.flushSemaphore.WaitAsync();
                try
                {
                    await this.fileStream.FlushAsync();
                }
                finally
                {

                }
                this.flushSemaphore.Release();
            }
            catch
            { 
                // Ignore exception incase flushSemaphore or fileStream is disposed.
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!this.Disposed && disposing)
            {
                if (Interlocked.Increment(ref this.disposedState) == 1)
                {
                    try
                    {
                        try
                        {
                            ReleaseSemaphore(this.concurrentCallSemaphore, this.concurrentCallSemaphoreDisposeEvent, int.MaxValue);
                            this.concurrentCallSemaphoreDisposeEvent.Wait();

                            var currentBuffer = this.bufferCurrent;
                            this.ReleaseBuffer(currentBuffer).Wait();
                        }
                        finally
                        {
                            this.fileStream.Dispose();

                            this.maxBufferCountSemaphore.Dispose();
                            this.flushSemaphore.Dispose();
                            this.concurrentCallSemaphore.Dispose();
                            this.concurrentCallSemaphoreDisposeEvent.Dispose();
                            this.currentBufferReaderLock.Dispose();
                        }
                    }
                    catch
                    {
                        // ignore all errors when disposing
                    }
                }
            }
        }

        private static void ReleaseSemaphore(SemaphoreSlim semaphore, ManualResetEventSlim manualResetEvent, int semaphoreMaxValue)
        {
            int semaphoreValue = semaphore.Release();
            if (semaphoreValue == semaphoreMaxValue - 1)
            {
                manualResetEvent.Set();
            }
        }

        private async Task<Buffer> CreateNewBufferAndFinishCurrentEntry(Buffer currentBuffer, byte[] bytesToWrite, int writtenBytes)
        {
            var tail = currentBuffer;

            // Without this method, we still logged every bits. However, since the newly created buffer will immediately used by other 
            // thread, there is no guarantee the left over entry is always on the start of the new buffer. This is broken given UTF8 encoding
            // may need multile bytes to compose one charactor. We cannot split in between.
            // to make the file continue, we firstly flash teh left over buffer in to new buffer. And keep doing that if one new buffer isn't 
            // enough until we flushed the entire entry.
            try
            {
                await this.ReleaseBuffer(currentBuffer);
            }
            finally
            {
                // We need to decrement the refCount twice. so that the previous buffer is released.
                await this.ReleaseBuffer(currentBuffer);
            }

            var newBuffer = await this.CreateNewBuffer();

            lock (this.syncRoot)
            {
                // we chould flushed all the buffers before new buffer created. In this situation, this.bufferHead is null.
                // we need to rebuild the head to point it to the new buffer we created.
                tail.Next = newBuffer;
                tail = tail.Next;
                if (this.bufferHead == null)
                {
                    this.bufferHead = tail;
                }
            }

            do
            {
                var bytesToCopy = Math.Min(bytesToWrite.Length - writtenBytes, newBuffer.BufferLeft);
                Array.Copy(bytesToWrite, writtenBytes, newBuffer.bytes, newBuffer.bufferCursor, bytesToCopy);
                writtenBytes += bytesToCopy;
                newBuffer.bufferCursor += bytesToCopy;
                if (newBuffer.Full)
                {
                    await this.ReleaseBuffer(newBuffer);
                    newBuffer = await this.CreateNewBuffer();
                    lock (this.syncRoot)
                    {
                        // we chould flushed all the buffers before new buffer created. In this situation, this.bufferHead is null.
                        // we need to rebuild the head to point it to the new buffer we created.
                        tail.Next = newBuffer;
                        tail = tail.Next;
                        if (this.bufferHead == null)
                        {
                            this.bufferHead = tail;
                        }
                    }
                }

                if (writtenBytes == bytesToWrite.Length)
                {
                    Thread.MemoryBarrier();
                    return newBuffer;
                }
            } while (true);
        }

        private async Task<Buffer> CreateNewBuffer()
        {
            await this.maxBufferCountSemaphore.WaitAsync();
            var newbuffer = this.CreateNewBufferInstance();
            return newbuffer;
        }

        private Buffer CreateNewBufferInstance()
        {
            byte[] bytes = null;
            if (!this.bufferRecycleBin.TryPop(out bytes))
            {
                bytes = null;
            }

            return new Buffer(this.bufferSize, bytes);
        }

        private Buffer GetCurrentBuffer()
        {
            do
            {
                var currentBuffer = this.bufferCurrent;
                if (!currentBuffer.Full)
                {
                    Interlocked.Increment(ref currentBuffer.refCount);
                    return currentBuffer;
                }

                this.currentBufferReaderLock.Wait();
            } while (true);
        }

        private async Task ReleaseBuffer(Buffer currentBuffer)
        {
            var localRefCount = Interlocked.Decrement(ref currentBuffer.refCount);
            if (localRefCount <= 0)
            {
                await this.FlushBuffer();
            }
        }

        private async Task FlushBuffer()
        {
            await this.flushSemaphore.WaitAsync();

            try
            {
                List<Buffer> bufferToFlush = new List<Buffer>();

                lock (this.syncRoot)
                {
                    var head = this.bufferHead;
                    if (head != null)
                    {
                        while (true)
                        {
                            if (Thread.VolatileRead(ref head.refCount) <= 0)
                            {
                                this.maxBufferCountSemaphore.Release();

                                bufferToFlush.Add(head);

                                head = head.Next;
                                if (head == null)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                break;
                            }
                        }

                        this.bufferHead = head;
                    }
                }

                foreach (var buffer in bufferToFlush)
                {
                    if (buffer.bufferCursor > 0)
                    {
                        try
                        {
                            await this.fileStream.WriteAsync(buffer.bytes, 0, buffer.bufferCursor);
                            await this.fileStream.FlushAsync();
                        }
                        catch
                        {
                            // Ignore write error.
                        }
                    }

                    Thread.MemoryBarrier();
                    if (this.bufferRecycleBin.Count < this.bufferCount - 1)
                    {
                        this.bufferRecycleBin.Push(buffer.bytes);
                    }
                }
            }
            finally
            {
                this.flushSemaphore.Release();
            }
        }

        private class Buffer
        {
            public int refCount = 1;
            public int bufferCursor = 0;
            public readonly byte[] bytes;

            public Buffer(int bufferSize, byte[] bytesIn = null)
            {
                if (bytesIn == null)
                {
                    bytesIn = new byte[bufferSize];
                }

                this.bytes = bytesIn;
            }

            public bool Full
            {
                get
                {
                    return this.BufferLeft <= 0;
                }
            }

            public int BufferLeft
            {
                get
                {
                    return Math.Max(0, this.bytes.Length - Thread.VolatileRead(ref this.bufferCursor));
                }
            }

            public Buffer Next { get; set; }
        }
    }
}
