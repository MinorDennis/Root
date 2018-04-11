namespace Com.MinorD.Tools
{
    using System;
    using System.Text;
    using System.Threading.Tasks;

    public sealed class Logger : IDisposable
    {
        private readonly ConcurrentFileWritter fileWritter;
        private readonly ILoggerConfig config;

        private bool disposed;

        public Logger(string fileName, ILoggerConfig config)
        {
            this.config = config;
            this.fileWritter = new ConcurrentFileWritter(fileName, fileName, this.config.LogBufferSize, this.config.LogBufferCount);
        }
        
        public void WriteLine(string str)
        {
            this.WriteLineAsync(str).Wait();
        }

        public async Task WriteLineAsync(string str)
        {
            StringBuilder sb = new StringBuilder(str.Length + 2);
            sb.AppendLine(str);

            await this.fileWritter.Write(Encoding.UTF8.GetBytes(sb.ToString()));
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if(!this.disposed && disposing)
            {
                try
                {
                    this.fileWritter.Dispose();
                }
                finally
                {
                    this.disposed = true;    
                }
            }
        }
    }
}
