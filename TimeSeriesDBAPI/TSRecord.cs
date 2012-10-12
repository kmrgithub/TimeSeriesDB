using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NYurik.TimeSeriesDb;
using NYurik.TimeSeriesDb.Common;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace TimeSeriesDBAPI
{
	public class TSRecord
	{
		[Index]
		public ulong Idx = 0;
		//			public string Exch = String.Empty; //{ get; set; }
		public double Bid = 0.0; // { get; set; }
		public double Ask = 0.0; // { get; set; }
		public uint BidSz = 0; // { get; set; }
		public uint AskSz = 0; // { get; set; }
		public ulong QuoteIdx = 0;
		private uint MarketId = 0;

		public TSRecord()
			: this(0, string.Empty, 0.0, 0, 0.0, 0)
		{
		}

		public TSRecord(ulong timestamp, string exch, double bid, uint bidsz, double ask, uint asksz)
		{
			this.Idx = timestamp;
			this.Bid = bid;
			this.BidSz = bidsz;
			this.Ask = ask;
			this.AskSz = asksz;
			this.QuoteIdx = 0;
			this.MarketId = new TSDateTime(this.Idx).MarketId;
			//				this.Exch = exch;
		}

		public TSRecord(ulong timestamp, string exch, double price, uint volume)
			: this(timestamp, exch, price, volume, 0.0, 0)
		{
		}

		public TSRecord(DateTime dt, uint seqno, uint marketid, string exch, double bid, uint bidsz, double ask, uint asksz)
			: this(new TSDateTime(dt, marketid, seqno).Timestamp, exch, bid, bidsz, ask, asksz)
		{
		}

		public TSRecord(DateTime dt, uint marketid, uint seqno, string exch, double price, uint volume)
			: this(dt, marketid, seqno, exch, price, volume, 0.0, 0)
		{
		}

		public bool IsQuote()
		{
			return this.Ask != 0.0 ? true : false;
		}

		public uint GetMarketId()
		{
			return this.MarketId;
		}

		public override string ToString()
		{
			TSDateTime tsdt = new TSDateTime(this.Idx);
			if (this.Ask == 0.0)
				return string.Format("Dt={0} MarketId={1} SeqNo={2} Price={3} Volume={4}", tsdt.Dt.ToString("yyyy-MM-dd HH-mm-ss.fff"), tsdt.MarketId, tsdt.SeqNo, this.Bid, this.BidSz);
			else
				return string.Format("Dt={0} MarketId={1} SeqNo={2} Bid={3} BidSz={4} Ask={5} AskSz={6}", tsdt.Dt.ToString("yyyy-MM-dd HH-mm-ss.fff"), tsdt.MarketId, tsdt.SeqNo, this.Bid, this.BidSz, this.Ask, this.AskSz);
		}
	}
}
