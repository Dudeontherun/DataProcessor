using DataProcessor.Aggregate;
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
			throw new NotImplementedException();	

			var connBuilder = new SqlConnectionStringBuilder();
			connBuilder.DataSource = System.Net.Dns.GetHostName();
			connBuilder.InitialCatalog = "CsvImportTest";
			connBuilder.UserID = connBuilder.Password = "user";

			string connectionString = connBuilder.ToString();

			CsvInputProcessor input = new CsvInputProcessor(10000, @"");

			//TODO: Make Aggregate look at multiple columns

			AggregateManager manager = new AggregateManager(input, 10000, 20);
			Aggregate.Aggregate routeName = new Aggregate.Aggregate(i => "Peerless", "RouteName", Guid.NewGuid());
			Aggregate.Aggregate AcctNum = new Aggregate.Aggregate((i => i), "CustomerAccountID", "AcctNum", Guid.NewGuid());
			Aggregate.Aggregate CircuitNum = new Aggregate.Aggregate((i => ""), "CircuitNum", Guid.NewGuid());
			//Takes calldate & call time.
			Aggregate.Aggregate CallUtc = new Aggregate.Aggregate((i => i), "AcctNum", Guid.NewGuid());
			Aggregate.Aggregate Direction = new Aggregate.Aggregate((i => "OutBound"), "Direction", Guid.NewGuid());
			Aggregate.Aggregate BillableDurationMSec = new Aggregate.Aggregate(((i) => ((int)(Decimal.Parse((String)i) * 60 * 1000))), "BillableTime", "BillableDurationMsecs", Guid.NewGuid());
			Aggregate.Aggregate BillableDurationMin = new Aggregate.Aggregate((i) => (Decimal.Parse((String)i)), "BillableTime", "BillableDurationMin", Guid.NewGuid());
			Aggregate.Aggregate PerMinuteRate = new Aggregate.Aggregate((i => String.IsNullOrEmpty((string)i) ? Decimal.Parse((String)i) : 0.0m), "Direction", Guid.NewGuid());
			//Takes Rateperminute and billable time
			Aggregate.Aggregate PerMinuteRateCalc = new Aggregate.Aggregate((i => "OutBound"), "Direction", Guid.NewGuid());
			Aggregate.Aggregate BilledAmount = new Aggregate.Aggregate((i => (String.IsNullOrEmpty((string)i) == false) ? Decimal.Parse((string)i) : 0.0m), "BilledAmount", Guid.NewGuid());
			Aggregate.Aggregate OrigNumber_10 = new Aggregate.Aggregate((i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i))), "OriginatingPhoneNumber", "OrigNumber_10", Guid.NewGuid());
			Aggregate.Aggregate OrigLRN_10 = new Aggregate.Aggregate((i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i))), "OriginatingLRN", "OrigLRN_10", Guid.NewGuid());
			Aggregate.Aggregate OrigLRN_6 = new Aggregate.Aggregate((i => ((((String)i).Length >= 6) ? ((String)i).Substring(0, 6) : ((String)i))), "OriginatingLRN", "OrigLRN_6", Guid.NewGuid());
			Aggregate.Aggregate OrigCity = new Aggregate.Aggregate((i => (string)i), "OriginatingCity", "OrigCity", Guid.NewGuid());
			Aggregate.Aggregate OrigState = new Aggregate.Aggregate((i => (string)i), "OriginatingState", "OrigState", Guid.NewGuid());
			Aggregate.Aggregate OrigCountry = new Aggregate.Aggregate((i => (string)i), "FromCountryCode", "OrigCountry", Guid.NewGuid());
			Aggregate.Aggregate DialedNumber_10 = new Aggregate.Aggregate((i => (string)i), "DialedNumber", "DialedNumber_10", Guid.NewGuid());
			Aggregate.Aggregate TermNumber_10 = new Aggregate.Aggregate((i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i))), "TerminatingPhoneNumber", "TermNumber_10", Guid.NewGuid());
			Aggregate.Aggregate TermLRN_10 = new Aggregate.Aggregate((i => ((((String)i).Length >= 10) ? ((String)i).Substring(0, 10) : ((String)i))), "TerminatingPhoneNumber", "TermLRN_10", Guid.NewGuid());
			Aggregate.Aggregate TermLRN_6 = new Aggregate.Aggregate((i => ((((String)i).Length >= 6) ? ((String)i).Substring(0, 6) : ((String)i))), "TerminatingPhoneNumber", "TermLRN_6", Guid.NewGuid());
			Aggregate.Aggregate TermCity = new Aggregate.Aggregate((i => (string)i), "TerminatingCity", "TermCity", Guid.NewGuid());
			Aggregate.Aggregate TermState = new Aggregate.Aggregate((i => (string)i), "TerminatingState", "TermState", Guid.NewGuid());
			Aggregate.Aggregate TermCountry = new Aggregate.Aggregate((i => (string)i), "ToCountryCode", "TermCountry", Guid.NewGuid());
			Aggregate.Aggregate Note1 = new Aggregate.Aggregate((i => ""), "Note1", Guid.NewGuid());
			Aggregate.Aggregate Note2 = new Aggregate.Aggregate((i => ""), "Note2", Guid.NewGuid());
			Aggregate.Aggregate Note3 = new Aggregate.Aggregate((i => ""), "Note3", Guid.NewGuid());

			manager.AddAggregate(routeName, 
			AcctNum, CircuitNum, 
			CallUtc, Direction, 
			BillableDurationMSec, BillableDurationMin, 
			PerMinuteRate, PerMinuteRateCalc, 
			BilledAmount, 
			OrigNumber_10, OrigLRN_10, OrigLRN_6,
			OrigCity, OrigState, OrigCountry,
			Note1, Note2, Note3
			);

			SQLOutput output = new SQLOutput(input, int.MaxValue, connectionString, "dbo.yes");
			output.Start();

			while (output.IsCompleted() == false)
			{
				Thread.Sleep(1000);
			}
		}
	}
}