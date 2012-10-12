using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using TimeSeriesDBAPI;

namespace ConsoleApplication1
{
	class TimeSeriesDBAPITest
	{
		static void Main(string[] args)
		{
			string dbfilename = string.Empty;

			for(int i = 0; i < args.Length; i++)
			{
				switch(args[i])
				{
					case "-f":
						dbfilename = args[i + 1];
						break;
				}
			}

			if (!string.IsNullOrEmpty(dbfilename))
			{
				TSStreamer tst = new TSStreamer(dbfilename);
				DateTime startdt = new DateTime(2010, 11, 1, 9, 30, 0, 300);
				DateTime enddt = new DateTime(2010, 11, 4, 16, 00, 0);
				int idx = 0;
				TSRecord x = null;
				do
				{
					foreach (TSRecord val in tst.GetData(startdt, enddt))
					{
						idx++;
						x = val;
					}
//						Console.WriteLine(val);
				} while (tst.MoreData == true);
			}
		}
	}
}
