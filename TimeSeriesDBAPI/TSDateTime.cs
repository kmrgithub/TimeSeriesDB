using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesDBAPI
{
	public class TSDateTime : IComparable<TSDateTime>
	{
		public uint MarketId { get; private set; }
		public uint SeqNo { get; private set; }
		public DateTime Dt { get; private set; }
		public ulong Timestamp { get; private set; }

		public TSDateTime(ulong timestamp)
		{
			this.Timestamp = timestamp;
			this.Dt = new DateTime((long)(10000 * (this.Timestamp / 100000)));
			this.SeqNo = (uint)((this.Timestamp - (ulong)(10 * this.Dt.Ticks)) / 1000);
			this.MarketId = (uint)(this.Timestamp - (ulong)(10 * this.Dt.Ticks) - (ulong)(1000 * this.SeqNo));
		}

		public TSDateTime(DateTime dt, uint marketid, uint seqno)
		{
			this.Dt = new DateTime(1000 * (dt.Ticks / 1000));
			this.MarketId = marketid;
			this.SeqNo = seqno;

			this.Timestamp = 10 * ((ulong)dt.Ticks);
			this.Timestamp /= 100000;
			this.Timestamp *= 100000;
			this.Timestamp += (seqno * 1000);
			this.Timestamp += marketid;
		}

		public int CompareTo(TSDateTime x)
		{
			return this.Timestamp.CompareTo(x.Timestamp);
		}

		public override string ToString()
		{
			return string.Format("Timestamp={0} MarketId={1} SeqNo={2} Dt={3}", this.Timestamp, this.MarketId, this.SeqNo, this.Dt.ToString("yyyy-MM-dd HH-mm-ss.fff"));
		}
	}
}
