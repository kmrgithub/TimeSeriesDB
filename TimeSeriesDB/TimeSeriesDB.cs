using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

using NYurik.TimeSeriesDb;
using NYurik.TimeSeriesDb.Common;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

using TimeSeriesDBAPI;

namespace TimeSeriesDB
{
	static class TimeSeriesDB
	{
		private static readonly List<string> ExchangeInclusionList = new List<string>() { "C", "J", "K", "N", "P", "Z" };
		private static readonly List<string> ExchangeExclusionList = new List<string>() { "Y" };
		private static readonly TimeSpan WhenTimeIsBefore = new TimeSpan(9, 30, 0);
		private static readonly TimeSpan WhenTimeIsAfter = new TimeSpan(16, 0, 0);
		static Dictionary<string, uint> NameToMarketId = new Dictionary<string, uint>() { { "DOW", 1 }, { "DAN", 2 }, { "CLF", 3 }, { "AIG", 4 }, {"SPY", 5} };

		enum DataType { Quote, Trade };

		class TradeData
		{
			public DateTime Dt { get; set; }
			public double Price { get; set; }
			public uint Volume { get; set; }
			public string Exch { get; set; }
			public string SalesCondition { get; set; }
			public string CorrectionIndicator { get; set; }
			public uint SeqNo { get; set; }
			public string TradeStopIndicator { get; set; }
			public string SourceOfTrade { get; set; }
			public string MDS127TRF { get; set; }
			public string ExcludeRecordFlag { get; set; }
			public double FilteredPrice { get; set; }

			public TradeData(string data)
			{
				this.Dt = DateTime.Now;
				this.Price = 0.0;
				this.Volume = 0;
				this.Exch = string.Empty;
				this.SalesCondition = string.Empty;
				this.CorrectionIndicator = string.Empty;
				this.SeqNo = 0;
				this.TradeStopIndicator = string.Empty;
				this.SourceOfTrade = string.Empty;
				this.MDS127TRF = string.Empty;
				this.ExcludeRecordFlag = string.Empty;
				this.FilteredPrice = 0.0;

				string[] dataelems = data.Split(new char[] { ',' });
				if (dataelems != null && dataelems.Length == 13)
				{
					this.Dt = Convert.ToDateTime(string.Format("{0} {1}", dataelems[0], dataelems[1]));
					if (!string.IsNullOrEmpty(dataelems[2]))
						this.Price = double.Parse(dataelems[2]);
					if (!string.IsNullOrEmpty(dataelems[3]))
						this.Volume = uint.Parse(dataelems[3]);
					this.Exch = dataelems[4];
					this.SalesCondition = dataelems[5];
					this.CorrectionIndicator = dataelems[6];
					if (!string.IsNullOrEmpty(dataelems[7]))
						this.SeqNo = uint.Parse(dataelems[7]);
					this.TradeStopIndicator = dataelems[8];
					this.SourceOfTrade = dataelems[9];
					this.MDS127TRF = dataelems[10];
					this.ExcludeRecordFlag = dataelems[11];
					if(!string.IsNullOrEmpty(dataelems[12]))
						this.FilteredPrice = double.Parse(dataelems[12]);
				}
			}
		}

		class QuoteData
		{
			public DateTime Dt { get; set; }
			public string MMId { get; set; }
			public string QuoteCondition { get; set; }
			public double Bid { get; set; }
			public double Ask { get; set; }
			public uint BidSz { get; set; }
			public uint AskSz { get; set; }
			public uint SeqNo { get; set; }
			public string Exch { get; set; }
			public string BidExch { get; set; }
			public string AskExch { get; set; }
			public uint NationalBBO { get; set; }
			public uint NasdaqBBO { get; set; }
			public string QuoteCancel { get; set; }
			public string QuoteSource { get; set; }

			public QuoteData(string data)
			{
				this.Dt = DateTime.Now;
				this.Exch = string.Empty;
				this.MMId = string.Empty;
				this.QuoteCondition = string.Empty;
				this.Bid = 0.0;
				this.Ask = 0.0;
				this.BidSz = 0;
				this.AskSz = 0;
				this.SeqNo = 0;
				this.BidExch = string.Empty;
				this.AskExch = string.Empty;
				this.NationalBBO = 0;
				this.NasdaqBBO = 0;
				this.QuoteCancel = string.Empty;
				this.QuoteSource = string.Empty;

				string[] dataelems = data.Split(new char[] { ',' });
				if (dataelems != null && dataelems.Length == 16)
				{
					this.Dt = Convert.ToDateTime(string.Format("{0} {1}", dataelems[0], dataelems[1]));
					this.Exch = dataelems[2];
					if (!string.IsNullOrEmpty(dataelems[3]))
						this.Bid = double.Parse(dataelems[3]);
					if (!string.IsNullOrEmpty(dataelems[4]))
						this.Ask = double.Parse(dataelems[4]);
					if (!string.IsNullOrEmpty(dataelems[5]))
						this.BidSz = uint.Parse(dataelems[5]);
					if (!string.IsNullOrEmpty(dataelems[6]))
						this.AskSz = uint.Parse(dataelems[6]);
					this.QuoteCondition = dataelems[7];
					this.MMId = dataelems[8];
					if (!string.IsNullOrEmpty(dataelems[9]))
						this.SeqNo = uint.Parse(dataelems[9]);
					this.BidExch = dataelems[10];
					this.AskExch = dataelems[11];
					if (!string.IsNullOrEmpty(dataelems[12]))
						this.NationalBBO = uint.Parse(dataelems[12]);
					if (!string.IsNullOrEmpty(dataelems[13]))
						this.NasdaqBBO = uint.Parse(dataelems[13]);
					this.QuoteCancel = dataelems[14];
					this.QuoteSource = dataelems[15];
				}
			}
		}

		static int BinarySearchForMatch<T>(this IList<T> list, Func<T, int> comparer)
		{
			int min = 0;
			int max = list.Count - 1;

			while (min <= max)
			{
				int mid = (min + max) / 2;
				int comparison = comparer(list[mid]);
				if (comparison == 0)
				{
					return mid;
				}
				if (comparison < 0)
				{
					min = mid + 1;
				}
				else
				{
					max = mid - 1;
				}
			}
			return min;
		}

		static string MarketName = string.Empty;
		static uint MarketId = 0;

		public static List<T> CreateList<T>(params T[] elements)
		{
			var list = new List<T>(elements);
			list.Clear();
			return list;
		}

		public static IEnumerable<int> ForLoopBlock(int start, int end, int incr)
		{
			for (int i = start; i < end; i += incr)
				yield return i;
		}

		static void Main(string[] args)
		{
			string sourcedirectory = string.Empty;
			string dbdirectory = Directory.GetCurrentDirectory();
			string fileextension = "asc";

			for (int i = 0; i < args.Length; i++)
			{
				switch (args[i])
				{
					case "-s":
						sourcedirectory = args[i + 1];
						break;

					case "-o":
						dbdirectory = args[i + 1];
						break;
				}
			}

			if (string.IsNullOrEmpty(sourcedirectory) || string.IsNullOrEmpty(dbdirectory))
			{
				Console.WriteLine("USAGE: TimeSeriesDB.exe -s sourcedirectory [-o output directory]");
				return;
			}

			Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss.fff"));
			SortedList<string, List<string>> FilesSortedByMarket = new SortedList<string, List<string>>();
			foreach (string file in System.IO.Directory.GetFiles(sourcedirectory, "*." + fileextension, SearchOption.AllDirectories))
			{
				string fname = Path.GetFileNameWithoutExtension(file);
				string[] fnameelems = fname.Split(new char[] { '_' });
				string market = fnameelems[0];
				if (FilesSortedByMarket.ContainsKey(market))
					FilesSortedByMarket[market].Add(file);
				else
					FilesSortedByMarket.Add(market, new List<string>() { file });
			}

			// get quote files by market and process to build
			// stream of best bid-offer
			foreach (string markets in FilesSortedByMarket.Keys)
			{
				#region Process Ticker Files
				if (NameToMarketId.ContainsKey(markets))
				{
					if (MarketName != markets)
					{
						MarketName = markets;
						MarketId = NameToMarketId[MarketName];
						Console.WriteLine("Processing {0}", MarketName);
						Console.WriteLine("\tQuotes");
					}
					List<string> files = FilesSortedByMarket[MarketName];

					// loop over all quotes files for the market and insert each quote into sortedquotedata
					#region Process Ticker Quote Files
					Parallel.ForEach(files.Where(x => x.Contains("_Q")), new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, quotedatafile =>
					{
						Console.WriteLine("\t\t{0}", quotedatafile);

						// list that contains sorted time series of trades and quotes for all markets
						var sortedalltimeseries = CreateList(new { TimeSeriesRecord = (TSRecord)null });

						string tradedatafile = quotedatafile.Replace("X_Q", "X_T");
						if (!File.Exists(tradedatafile))
						{
							Console.WriteLine("\t\t\tTrade data file does not exist: {0}", tradedatafile);
							return;
						}

						Dictionary<DateTime, uint> markettodt = new Dictionary<DateTime, uint>();
						var sortedquotedata = CreateList(new { Key = (TSDateTime)null, Value = (QuoteData)null }); // new[] { new { Key = (TSDateTime)null, Value = (QuoteData)null } }.ToList();
						using (StreamReader sr = new StreamReader(quotedatafile))
						{
							String line = null;
							while ((line = sr.ReadLine()) != null)
							{
								QuoteData qd = new QuoteData(line);
								if (qd.Dt.TimeOfDay >= WhenTimeIsBefore && qd.Dt.TimeOfDay <= WhenTimeIsAfter && qd.Bid != 0.0 && qd.Ask != 0.0 && qd.BidSz != 0 && qd.AskSz != 0 && qd.Bid <= qd.Ask && ExchangeInclusionList.Contains(qd.Exch))
								{
									uint seqno = 1;
									if (markettodt.ContainsKey(qd.Dt))
									{
										seqno = markettodt[qd.Dt];
										markettodt[qd.Dt] = seqno + 1;
									}
									else
										markettodt[qd.Dt] = 1;
									TSDateTime tsdt = new TSDateTime(qd.Dt, MarketId, seqno); //GetSeqNo(qd.Dt, MarketId));
									sortedquotedata.Add(new { Key = new TSDateTime(qd.Dt, MarketId, seqno), Value = qd });
								}
							}
						}
//						List<QuoteData> quotedata = ParseQuoteData(item, sortedquotedata);
						Console.WriteLine("\t\t\tSorting quotes for {0}", quotedatafile);
						sortedquotedata.Sort((p1, p2) => p1.Key.Timestamp.CompareTo(p2.Key.Timestamp));
						markettodt.Clear();

						#region Build Inside Quotes For Ticker
						// we have sorted quotes for a market. now build stream of best bid-offer
						var sortedbiddata = CreateList(new { Exch = string.Empty, Price = 0.0, Size = (uint)0 });
						var sortedaskdata = CreateList(new { Exch = string.Empty, Price = 0.0, Size = (uint)0 });
						var sortedquotes = CreateList(new { Dt = (ulong)0, Exch = string.Empty, Bid = 0.0, BidSz = (uint)0, Ask = 0.0, AskSz = (uint)0 });

						string prevbidexch = string.Empty;
						string prevaskexch = string.Empty;
						double prevbidprice = 0.0;
						double prevaskprice = 0.0;
						uint prevbidsize = 0;
						uint prevasksize = 0;
						DateTime prevdt = DateTime.Now;

						// walk sortedquotedata to compute inside market
						// insert inside market records into sortedquotes
						Console.WriteLine("\t\t\tBuilding inside market for {0}", quotedatafile);
						foreach (var qd in sortedquotedata)
						{
							bool newbiddata = true;
							bool newaskdata = true;

							var mqqs = sortedbiddata.FirstOrDefault(x => x.Exch == qd.Value.BidExch);
							if (mqqs == null)
								sortedbiddata.Add(new { Exch = qd.Value.BidExch, Price = qd.Value.Bid, Size = qd.Value.BidSz });
							else if (mqqs != null && (mqqs.Price != qd.Value.Bid || mqqs.Size != qd.Value.BidSz))
							{
								sortedbiddata.Remove(mqqs);
								sortedbiddata.Add(new { Exch = qd.Value.BidExch, Price = qd.Value.Bid, Size = qd.Value.BidSz });
							}
							else
								newbiddata = false;

							mqqs = sortedaskdata.FirstOrDefault(x => x.Exch == qd.Value.AskExch);
							if (mqqs == null)
								sortedaskdata.Add(new { Exch = qd.Value.AskExch, Price = qd.Value.Ask, Size = qd.Value.AskSz });
							else if (mqqs != null && (mqqs.Price != qd.Value.Ask || mqqs.Size != qd.Value.AskSz))
							{
								sortedaskdata.Remove(mqqs);
								sortedaskdata.Add(new { Exch = qd.Value.AskExch, Price = qd.Value.Ask, Size = qd.Value.AskSz });
							}
							else
								newaskdata = false;

							if (newbiddata)
								sortedbiddata = sortedbiddata.OrderByDescending(x => x.Price).ThenByDescending(y => y.Size).ToList();
							if (newaskdata)
								sortedaskdata = sortedaskdata.OrderBy(x => x.Price).ThenByDescending(y => y.Size).ToList();

							if (
									((prevbidprice != sortedbiddata[0].Price) || (prevbidsize != sortedbiddata[0].Size)
									|| (prevaskprice != sortedaskdata[0].Price) || (prevasksize != sortedaskdata[0].Size)
								//						|| (prevbidexch != sortedbiddata[0].Exch) || (prevaskexch != sortedaskdata[0].Exch)
									)
									&&
									(prevdt != qd.Value.Dt)
								)
							{
								sortedquotes.Add(new { Dt = (ulong)new TSDateTime(qd.Key.Dt, qd.Key.MarketId, 0).Timestamp, Exch = sortedbiddata[0].Exch, Bid = (double)sortedbiddata[0].Price, BidSz = (uint)sortedbiddata[0].Size, Ask = (double)sortedaskdata[0].Price, AskSz = (uint)sortedaskdata[0].Size });
								//						Console.WriteLine(string.Format("{0} {1}:{2}:{3} {4}:{5}:{6}", qd.Value.Dt.Ticks, sortedbiddata[0].Exch, sortedbiddata[0].Price, sortedbiddata[0].Size, sortedaskdata[0].Exch, sortedaskdata[0].Price, sortedaskdata[0].Size));
								prevbidexch = sortedbiddata[0].Exch;
								prevbidprice = sortedbiddata[0].Price;
								prevbidsize = sortedbiddata[0].Size;
								prevaskexch = sortedaskdata[0].Exch;
								prevaskprice = sortedaskdata[0].Price;
								prevasksize = sortedaskdata[0].Size;
								prevdt = qd.Value.Dt;
							}
						}
						Console.WriteLine("\t\t\tSorting inside market for {0}", quotedatafile);
						sortedquotes.Sort((p1, p2) => p1.Dt.CompareTo(p2.Dt));
						sortedbiddata.Clear();
						sortedaskdata.Clear();
						sortedquotedata.Clear();
						#endregion

						#region Process Ticker Trades Files
						var sortedtrades = CreateList(new { Key = (TSDateTime)null, Value = (TradeData)null });
						if (sortedquotes.Count > 0)
						{
							using (StreamReader sr = new StreamReader(tradedatafile))
							{
								String line = null;
								while ((line = sr.ReadLine()) != null)
								{
									try
									{
										TradeData td = new TradeData(line);
										uint seqno = 1;
										if (markettodt.ContainsKey(td.Dt))
										{
											seqno = markettodt[td.Dt];
											markettodt[td.Dt] = seqno + 1;
										}
										else
											markettodt[td.Dt] = 1;
										if (td.Dt.TimeOfDay >= WhenTimeIsBefore && td.Dt.TimeOfDay <= WhenTimeIsAfter && td.Price > 0.0 && td.Volume > 0 && ExchangeInclusionList.Contains(td.Exch))
											sortedtrades.Add(new { Key = new TSDateTime(td.Dt, TimeSeriesDB.MarketId, seqno), Value = td });
									}
									catch
									{
									}
								}
							}
							// need to process all trades files and create a sorted list of trades
							Console.WriteLine("\t\t{0}", tradedatafile);
						}
						#endregion
						Console.WriteLine("\tSorting trades");
						sortedtrades.Sort((p1, p2) => p1.Key.Timestamp.CompareTo(p2.Key.Timestamp));

						#region Build Interleaved Timeseries
						var sortedtimeseries = CreateList(new { TimeSeriesRecord = (TSRecord)null });

						ulong timestamp_msecs = 0;
						ulong timestamp_quote = 0;
						// loop over sortedtrades and create time series record 
						Console.WriteLine("\tBuilding interleaved timeseries");
						foreach (var x in sortedtrades)
						{
							if (10 * ((ulong)x.Key.Dt.Ticks) > timestamp_msecs)
							{
								// find nearest quote by timestamp
								int index = BinarySearchForMatch(sortedquotes, (y) => { return y.Dt.CompareTo(x.Key.Timestamp); });
								int idx = index == 0 ? 0 : index - 1;
								var quote = sortedquotes[idx];
								timestamp_quote = quote.Dt;
								timestamp_msecs = 10 * ((ulong)x.Key.Dt.Ticks);
							}
							sortedtimeseries.Add(new { TimeSeriesRecord = new TSRecord(x.Key.Timestamp, x.Value.Exch, x.Value.Price, x.Value.Volume) { QuoteIdx = timestamp_quote } });
						}
						sortedtrades.Clear();
						foreach (var x in sortedquotes)
							sortedtimeseries.Add(new { TimeSeriesRecord = new TSRecord(x.Dt, x.Exch, x.Bid, x.BidSz, x.Ask, x.AskSz) });
						sortedquotes.Clear();

						Console.WriteLine("\tSorting timeseries");
						sortedtimeseries.Sort((p1, p2) => p1.TimeSeriesRecord.Idx.CompareTo(p2.TimeSeriesRecord.Idx));
						#endregion

						Console.WriteLine("\tAdding {0} timeseries to master timeseries", TimeSeriesDB.MarketName);
						sortedalltimeseries.AddRange(sortedtimeseries);

						Console.WriteLine("\tSorting master timeseries");
						sortedalltimeseries.Sort((p1, p2) => p1.TimeSeriesRecord.Idx.CompareTo(p2.TimeSeriesRecord.Idx));

						#region Write To Timeseries DB
						if (sortedalltimeseries.Count > 0)
						{
							string filename = string.Format(@"{0}\{1}", dbdirectory, Path.GetFileName(quotedatafile).Replace("_Q", "_TSDB").Replace(fileextension, "dts")); //new TSDateTime(sortedalltimeseries[0].TimeSeriesRecord.Idx).Dt.ToString("yyyyMMddHHmmssfff"), new TSDateTime(sortedalltimeseries[sortedalltimeseries.Count - 1].TimeSeriesRecord.Idx).Dt.ToString("yyyyMMddHHmmssfff"));

							if (File.Exists(filename))
								File.Delete(filename);

							using (var file = new BinCompressedSeriesFile<ulong, TSRecord>(filename))
							{
								var root = (ComplexField)file.RootField;
								((ScaledDeltaFloatField)root["Bid"].Field).Multiplier = 1000;
								((ScaledDeltaFloatField)root["Ask"].Field).Multiplier = 1000;

								file.UniqueIndexes = false; // enforces index uniqueness
								file.InitializeNewFile(); // create file and write header

								List<TSRecord> tsrlist = new List<TSRecord>();
								foreach (var tsr in sortedalltimeseries)
									tsrlist.Add(tsr.TimeSeriesRecord);

								ArraySegment<TSRecord> arr = new ArraySegment<TSRecord>(tsrlist.ToArray());
								file.AppendData(new ArraySegment<TSRecord>[] { arr });
							}
						}
						#endregion
					});
					#endregion
				}
				#endregion

				// merge files together
				#region merge per day per tick ts files into a single per tick file
				string mergedfilename = string.Format(@"{0}\{1}", dbdirectory, markets + ".dts"); //new TSDateTime(sortedalltimeseries[0].TimeSeriesRecord.Idx).Dt.ToString("yyyyMMddHHmmssfff"), new TSDateTime(sortedalltimeseries[sortedalltimeseries.Count - 1].TimeSeriesRecord.Idx).Dt.ToString("yyyyMMddHHmmssfff"));
				if (File.Exists(mergedfilename))
					File.Delete(mergedfilename);

				List<string> filestobemerged = Directory.GetFiles(dbdirectory, markets + "*.dts").ToList();
				do
				{
					var pairs = filestobemerged.Where((x, i) => i % 2 == 0).Zip(filestobemerged.Where((x, i) => i % 2 == 1), (second, first) => new[] { first, second }).ToList();
					Parallel.ForEach(pairs, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, pair =>
					{
						string mergedfile = Path.GetTempFileName();
						File.Copy(mergedfile, mergedfile = dbdirectory + "\\" + Path.GetFileName(mergedfile));
						File.Delete(mergedfile);
						string mergefile1 = pair[0];
						string mergefile2 = pair[1];

						List<TSDBEnumerator> sortedtsdbenumerators = new List<TSDBEnumerator>();
						List<TSDBEnumerator> TSDBEnumerators = pair.Select(x => new TSDBEnumerator(new TSStreamer(x), new TSDateTime(DateTime.MinValue, 0, 0).Timestamp, new TSDateTime(DateTime.MaxValue, 999, 99).Timestamp)).ToList(); //.TimeSeriesDB.Stream(new TSDateTime(DateTime.MinValue, 0, 0).Timestamp, new TSDateTime(DateTime.MaxValue, 999, 99).Timestamp).GetEnumerator())).ToList();
						using (var file = new BinCompressedSeriesFile<ulong, TSRecord>(mergedfile))
						{
							List<TSRecord> tsrlist = new List<TSRecord>();

							var root = (ComplexField)file.RootField;
							((ScaledDeltaFloatField)root["Bid"].Field).Multiplier = 1000;
							((ScaledDeltaFloatField)root["Ask"].Field).Multiplier = 1000;

							file.UniqueIndexes = false; // enforces index uniqueness
							file.InitializeNewFile(); // create file and write header

							sortedtsdbenumerators.AddRange(TSDBEnumerators.Select(x => x));
							do
							{
								foreach (var tsdbenumerator in TSDBEnumerators.Where(x => x.GetNext == true && x.TSEnumerator != null))
								{
									tsdbenumerator.GetNext = false;
									if (true == tsdbenumerator.TSEnumerator.MoveNext())
									{
										tsdbenumerator.TSRecord = tsdbenumerator.TSEnumerator.Current;
									}
									else
									{
										tsdbenumerator.Dispose();
										tsdbenumerator.TSEnumerator = null;
										tsdbenumerator.TSRecord = null;
										sortedtsdbenumerators.Remove(tsdbenumerator);
									}
								}
								if (sortedtsdbenumerators.Count > 0)
								{
									sortedtsdbenumerators.Sort((item1, item2) => item1.TSRecord.Idx.CompareTo(item2.TSRecord.Idx));
									sortedtsdbenumerators[0].GetNext = true;
									tsrlist.Add(sortedtsdbenumerators[0].TSRecord);
								}
								if (tsrlist.Count == 10000000)
								{
									ArraySegment<TSRecord> arr = new ArraySegment<TSRecord>(tsrlist.ToArray());
									file.AppendData(new ArraySegment<TSRecord>[] { arr });
									tsrlist.Clear();
								}
							} while (sortedtsdbenumerators.Count > 0);
							if (tsrlist.Count > 0)
							{
								ArraySegment<TSRecord> arr = new ArraySegment<TSRecord>(tsrlist.ToArray());
								file.AppendData(new ArraySegment<TSRecord>[] { arr });
							}
						}
						filestobemerged.Remove(mergefile1);
						if (mergefile1.Contains(".tmp"))
							File.Delete(mergefile1);
						filestobemerged.Remove(mergefile2);
						if (mergefile2.Contains(".tmp"))
							File.Delete(mergefile2);
						filestobemerged.Add(mergedfile);
					});
				} while (filestobemerged.Count > 1);
				if (filestobemerged.Count == 1)
					File.Move(filestobemerged[0], dbdirectory + "\\" + markets + ".dts");
				#endregion
			}

			Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss.fff"));
		}

		private class TSDBEnumerator : IDisposable
		{
			public TSStreamer TSStreamer { get; set; }
			public bool GetNext { get; set; }
			public TSRecord TSRecord { get; set; }
			public IEnumerator<TSRecord> TSEnumerator { get; set; }
			private bool _isDisposed = false;
			public TSDBEnumerator(TSStreamer tssstreamer, ulong minval, ulong maxval)
			{
				this.TSStreamer = tssstreamer;
				this.GetNext = true;
				this.TSRecord = null;
				this.TSEnumerator = this.TSStreamer.TimeSeriesDB.Stream(minval, maxval).GetEnumerator();
			}

			public void Dispose()
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			public void Dispose(bool disposing)
			{
				if (!_isDisposed)
				{
					if (disposing)
						this.TSStreamer.Dispose();
					this.TSStreamer = null;

					_isDisposed = true;
				}
			}

		}
	}
}
