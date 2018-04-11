namespace Com.MinorD.Tools
{
    using System;

    public interface ILoggerConfig
    {
        int LogBufferSize { get; }

        int LogBufferCount { get; }        
    }

    public interface IFolderComparerConfig : ILoggerConfig
    {
        int MaxConcurrency { get; }

        TimeSpan WaitSpinDelay { get; }

        int BufferSize { get; }
    }
}
