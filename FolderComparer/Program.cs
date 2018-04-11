namespace Com.MinorD.Tools
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class Program
    {
        private static CancellationTokenSource cancellationTokenSource;
        public static void Main(string[] args)
        {
            string sourceFolder;
            string targetFolder;
            string logFolder;
            string searchPattern;
            bool debug;

            if (!ParseArgs(args, out sourceFolder, out targetFolder, out logFolder, out searchPattern, out debug))
            {
                Console.WriteLine("FolderCompare -s <SourceFolder> -t <TargetFolder> -logFolder <LogFolder> [-searchPattern <SearchPattern>] [-d]");
                return;
            }

            if (!Directory.Exists(sourceFolder))
            {
                throw new InvalidOperationException($"Cannot find source folder {sourceFolder}.");
            }

            if (!Directory.Exists(targetFolder))
            {
                throw new InvalidOperationException($"Cannot find target folder {targetFolder}.");
            }

            if (!Directory.Exists(logFolder))
            {
                throw new InvalidOperationException($"Cannot find target folder {targetFolder}.");
            }

            if(debug)
            {
                Console.WriteLine("Press ENTER to continue.");
                Console.ReadLine();
            }

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;
                if (cancellationTokenSource != null)
                {
                    cancellationTokenSource.Cancel();
                }
            };

            var folderComparer = new FolderComparer(sourceFolder, targetFolder, searchPattern, logFolder, new FolderComparerConfig());

            cancellationTokenSource = new CancellationTokenSource();

            try
            {
                folderComparer.CompareFolder(cancellationTokenSource);
            }
            finally
            {
                cancellationTokenSource = null;
            }
        }        

        private static bool ParseArgs(string[] args, out string sourceFolder, out string targetFolder, out string logFolder, out string searchPattern, out bool debug)
        {
            sourceFolder = string.Empty;
            targetFolder = string.Empty;
            logFolder = null;
            searchPattern = "*.*";
            debug = false;
            for(int i = 0; i< args.Length; i++)
            {
                if(string.Equals("-s", args[i], StringComparison.OrdinalIgnoreCase))
                {
                    if(i == args.Length -1)
                    {
                        return false;
                    }

                    sourceFolder = args[++i];
                }
                else if (string.Equals("-t", args[i], StringComparison.OrdinalIgnoreCase))
                {
                    if (i == args.Length - 1)
                    {
                        return false;
                    }

                    targetFolder = args[++i];
                }
                else if (string.Equals("-logFolder", args[i], StringComparison.OrdinalIgnoreCase))
                {
                    if (i == args.Length - 1)
                    {
                        return false;
                    }

                    logFolder = args[++i];
                }
                else if (string.Equals("-searchPattern", args[i], StringComparison.OrdinalIgnoreCase))
                {
                    if (i == args.Length - 1)
                    {
                        return false;
                    }

                    searchPattern = args[++i];
                }
                else if (string.Equals("-d", args[i], StringComparison.OrdinalIgnoreCase))
                {
                    debug = true;
                }
                else
                {
                    return false;
                }
            }

            sourceFolder = Path.GetFullPath(sourceFolder);
            targetFolder = Path.GetFullPath(targetFolder);

            return true;
        }
    }
}
