namespace Com.MinorD.Tools
{
    using System;
    using System.Configuration;

    public class FolderComparerConfig : IFolderComparerConfig
    {
        public int MaxConcurrency
        {
            get
            {
                string maxConcurrencyStr = ConfigurationManager.AppSettings["MaxConcurrency"];
                int maxConcurrency;
                if(string.IsNullOrWhiteSpace(maxConcurrencyStr) || !int.TryParse(maxConcurrencyStr, out maxConcurrency) || maxConcurrency <= 0)
                {
                    maxConcurrency = 32;
                }

                return maxConcurrency;
            }
        }

        public TimeSpan WaitSpinDelay
        {
            get
            {
                string waitSpinDelayInMillisecondsStr = ConfigurationManager.AppSettings["WaitSpinDelayInMilliseconds"];
                int waitSpinDelayInMilliseconds;
                if (string.IsNullOrWhiteSpace(waitSpinDelayInMillisecondsStr) || !int.TryParse(waitSpinDelayInMillisecondsStr, out waitSpinDelayInMilliseconds) || waitSpinDelayInMilliseconds < 0)
                {
                    waitSpinDelayInMilliseconds = 0;
                }

                return TimeSpan.FromMilliseconds(waitSpinDelayInMilliseconds);
            }
        }

        public int BufferSize
        {
            get
            {
                string bufferSizeStr = ConfigurationManager.AppSettings["BufferSize"];
                int bufferSize;
                if (string.IsNullOrWhiteSpace(bufferSizeStr) || !int.TryParse(bufferSizeStr, out bufferSize) || bufferSize < 0)
                {
                    bufferSize = 1048576;  // 1M
                }

                return bufferSize;
            }
        }

        public int LogBufferSize
        {
            get
            {
                string logBufferSizeStr = ConfigurationManager.AppSettings["LogBufferSize"];
                int logBufferSize;
                if (string.IsNullOrWhiteSpace(logBufferSizeStr) || !int.TryParse(logBufferSizeStr, out logBufferSize) || logBufferSize <= 0)
                {
                    logBufferSize = 524288;
                }

                return logBufferSize;
            }
        }

        public int LogBufferCount
        {
            get
            {
                string logBufferCountStr = ConfigurationManager.AppSettings["LogBufferCount"];
                int logBufferCount;
                if (string.IsNullOrWhiteSpace(logBufferCountStr) || !int.TryParse(logBufferCountStr, out logBufferCount) || logBufferCount < 2)
                {
                    logBufferCount = 2;
                }

                return logBufferCount;
            }
        }        
    }
}
