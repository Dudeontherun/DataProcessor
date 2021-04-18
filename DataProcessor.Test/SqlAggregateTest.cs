using DataProcessor.Aggregate;
using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.Input;
using DataProcessor.Interfaces;
using DataProcessor.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataProcessor.Test
{
	[TestClass]
	public class SqlAggregateTest
	{

		[TestMethod]
		public void Put()
		{

			var connBuilder = new SqlConnectionStringBuilder();
			connBuilder.DataSource = System.Net.Dns.GetHostName();
			connBuilder.InitialCatalog = "CsvImportTest";
			connBuilder.UserID = connBuilder.Password = "user";

			string connectionString = connBuilder.ToString();

			CsvInputProcessor input = new CsvInputProcessor(100000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\Fixed-NETSTAFFHR_LD_2020_09_01_LD.csv");

			//TODO: Make Aggregate look at multiple columns.

			AggregateManager manager = new AggregateManager(input, 10000, 1);
			Aggregate.Aggregate routeName = new Aggregate.Aggregate(@"() => ""Peerless""", "RouteName", Guid.NewGuid());
			Aggregate.Aggregate AcctNum = new Aggregate.Aggregate("i => (string)i", "CustomerAccountID", "AcctNum", Guid.NewGuid());
			Aggregate.Aggregate CircuitNum = new Aggregate.Aggregate(@"() => """"", "CircuitNum", Guid.NewGuid());
			
			Aggregate.Aggregate CallUtc = new Aggregate.Aggregate(@"(CallDate, CallTime) => (DateTime.Parse((string)CallDate) + TimeSpan.Parse((String)CallTime))", new string[2] { "CallDate", "CallTime"}, "CallUTC", Guid.NewGuid());
			Aggregate.Aggregate Direction = new Aggregate.Aggregate((@"() => ""OutBound"""), "Direction", Guid.NewGuid());
			Aggregate.Aggregate BillableDurationMSec = new Aggregate.Aggregate("((i) => ((int)(Decimal.Parse((String)i) * 60 * 1000)))", "BillableTime", "BillableDurationMsecs", Guid.NewGuid());
			Aggregate.Aggregate BillableDurationMin = new Aggregate.Aggregate("(i) => (Decimal.Parse((String)i))", "BillableTime", "BillableDurationMin", Guid.NewGuid());
			Aggregate.Aggregate PerMinuteRate = new Aggregate.Aggregate("(i => String.IsNullOrEmpty((string)i) ? Decimal.Parse((String)i) : 0.0m)", "RatePerMinute", "PerMinuteRate", Guid.NewGuid());
			//Takes Rateperminute and billable time
			Aggregate.Aggregate PerMinuteRateCalc = new Aggregate.Aggregate(@"(RatePerMinute, BillableTime) => ((String.IsNullOrEmpty(((string)RatePerMinute)) == false && String.IsNullOrEmpty(((string)BillableTime)) == false && Decimal.Parse((String)BillableTime) != 0.000m) ? Decimal.Parse((String)RatePerMinute) / Decimal.Parse((String)BillableTime) : 0.0m)", new string[2] {"RatePerMinute", "BillableTime" }, "PerMinuteRateCalc", Guid.NewGuid());
			Aggregate.Aggregate BilledAmount = new Aggregate.Aggregate("(i => (String.IsNullOrEmpty((string)i) == false) ? Decimal.Parse((string)i) : 0.0m)", "BillableAmount", "BilledAmount", Guid.NewGuid());
			Aggregate.Aggregate OrigNumber_10 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i)))", "OriginatingPhoneNumber", "OrigNumber_10", Guid.NewGuid());
			Aggregate.Aggregate OrigLRN_10 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i)))", "OriginatingLRN", "OrigLRN_10", Guid.NewGuid());
			Aggregate.Aggregate OrigLRN_6 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 6) ? ((String)i).Substring(0, 6) : ((String)i)))", "OriginatingLRN", "OrigLRN_6", Guid.NewGuid());
			Aggregate.Aggregate OrigCity = new Aggregate.Aggregate("(i => (string)i)", "OriginatingCity", "OrigCity", Guid.NewGuid());
			Aggregate.Aggregate OrigState = new Aggregate.Aggregate("(i => (string)i)", "OriginatingState", "OrigState", Guid.NewGuid());
			Aggregate.Aggregate OrigCountry = new Aggregate.Aggregate("(i => (string)i)", "FromCountryCode", "OrigCountry", Guid.NewGuid());
			Aggregate.Aggregate DialedNumber_10 = new Aggregate.Aggregate("(i => (string)i)", "DialedNumber", "DialedNumber_10", Guid.NewGuid());
			Aggregate.Aggregate TermNumber_10 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i)))", "TerminatingPhoneNumber", "TermNumber_10", Guid.NewGuid());
			Aggregate.Aggregate TermLRN_10 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i)))", "TerminatingPhoneNumber", "TermLRN_10", Guid.NewGuid());
			Aggregate.Aggregate TermLRN_6 = new Aggregate.Aggregate("(i => ((((String)i).Length >= 6) ? ((String)i).Substring(0, 6) : ((String)i)))", "TerminatingPhoneNumber", "TermLRN_6", Guid.NewGuid());
			Aggregate.Aggregate TermCity = new Aggregate.Aggregate("(i => (string)i)", "TerminatingCity", "TermCity", Guid.NewGuid());
			Aggregate.Aggregate TermState = new Aggregate.Aggregate("(i => (string)i)", "TerminatingState", "TermState", Guid.NewGuid());
			Aggregate.Aggregate TermCountry = new Aggregate.Aggregate("(i => (string)i)", "ToCountryCode", "TermCountry", Guid.NewGuid());
			Aggregate.Aggregate Note1 = new Aggregate.Aggregate(@"(() => """")", "Note1", Guid.NewGuid());
			Aggregate.Aggregate Note2 = new Aggregate.Aggregate(@"(() => """")", "Note2", Guid.NewGuid());
			Aggregate.Aggregate Note3 = new Aggregate.Aggregate(@"(() => """")", "Note3", Guid.NewGuid());

			manager.AddAggregate(routeName,
			AcctNum, CircuitNum,
			CallUtc, Direction,
			BillableDurationMSec, BillableDurationMin,
			PerMinuteRate, PerMinuteRateCalc,
			BilledAmount,
			OrigNumber_10, OrigLRN_10, OrigLRN_6,
			OrigCity, OrigState, OrigCountry,
			DialedNumber_10, TermNumber_10, TermLRN_10, TermLRN_6,
			TermCity, TermState, TermCountry,
			Note1, Note2, Note3
			);

			SQLOutput output = new SQLOutput(manager, int.MaxValue, connectionString, "dbo.yes");

			BaseDataProcessor processor = new BaseDataProcessor(input, manager, output);
			processor.Start();

			while (processor.IsFinished() == false)
			{
				Thread.Sleep(1000);
			}
		}
	}
}