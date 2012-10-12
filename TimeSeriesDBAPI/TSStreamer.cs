using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NYurik.TimeSeriesDb;
using NYurik.TimeSeriesDb.Common;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace TimeSeriesDBAPI
{
	public class TSStreamer
	{
		private static readonly int MaxStreamValues = 1000000;
		private string Filename { get; set; }
		private IEnumerableFeed<ulong, TSRecord> TimeSeriesDB { get; set; }
		public bool MoreData { get; private set; }
		private DateTime LastDt { get; set; }
		private DateTime EndDt { get; set; }

		public TSStreamer(string filename)
		{
			this.Filename = filename;
			this.TimeSeriesDB = null;
			this.LastDt = DateTime.MinValue;
			this.EndDt = DateTime.MinValue;
			this.MoreData = false;
		}

		private TSRecord GetQuote(ulong index)
		{
			TSRecord tsr = null;
			try
			{
				var bf = (IEnumerableFeed<ulong, TSRecord>)BinaryFile.Open(this.Filename, false);
				foreach (TSRecord val in bf.Stream(index, index + 1))
					tsr = val;
			}
			catch (Exception ex)
			{
			}

			return tsr;
		}

		public IEnumerable<TSRecord> GetData(DateTime startdt, DateTime enddt)
		{
			List<uint> marketids = new List<uint>();
			List<TSRecord> timeseries = new List<TSRecord>();

			if (this.MoreData == false)
			{
				this.LastDt = startdt;
				this.EndDt = enddt;
			}
			else
				this.MoreData = false;

			ulong idx = 0;
			this.TimeSeriesDB = (IEnumerableFeed<ulong, TSRecord>)BinaryFile.Open(this.Filename, false);
			foreach (TSRecord val in this.TimeSeriesDB.Stream(new TSDateTime(this.LastDt, 0, 0).Timestamp, new TSDateTime(this.EndDt.AddMilliseconds(1), 0, 0).Timestamp))
			{
				if ((timeseries.Count >= TSStreamer.MaxStreamValues) && (100000 * (val.Idx / 100000) > idx))
				{
					// store lastdt
					this.LastDt = new TSDateTime(val.Idx).Dt;

					// set Moredata to true - indicates there is more data to be streamed
					this.MoreData = true;

					break;
				}

				if (!marketids.Contains(val.GetMarketId()))
				{
					if (!val.IsQuote())
					{
						TSRecord tsr = GetQuote(val.QuoteIdx);
						if(tsr != null)
							timeseries.Add(tsr);
					}
					marketids.Add(val.GetMarketId());
				}
				timeseries.Add(val);
				idx = 100000 * (val.Idx / 100000);
			}

			foreach (TSRecord ts in timeseries)
				yield return ts;
		}
	}
}
