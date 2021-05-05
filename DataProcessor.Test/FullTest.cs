using DataProcessor.Interfaces;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using DataProcessor.Input;
using DataProcessor.Aggregate;
using DataProcessor.Output;
using DataProcessor.Base;
using System.Threading;

namespace DataProcessor.Test
{
	[TestClass]
	public class FullTest
	{
		[TestInitialize]
		public void PopulateTestData()
		{
			int numOfRows = 1000000;

			var config = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
			{
				AllowComments = false,
				BufferSize = 10000,
			};

			ConcurrentBuffer buffer = new ConcurrentBuffer(10000);
			Task.Run(() =>
			{
				Enumerable.Range(0, numOfRows).ToList().ForEach(j =>
				{
					string[] header = { "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field00" };
					string[] data = new string[10];
					for (int i = 0; i < data.Length; i++)
					{
						data[i] = Guid.NewGuid().ToString();
					}

					IDataRow row = new DataRow.BaseDataRow(header, data);

					buffer.Add(row);
				});
				buffer.CompleteAdding();
			}
			);

			using (CsvHelper.CsvWriter writer = new CsvHelper.CsvWriter(new StreamWriter(@"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\FULLTEXT.csv", false), config))
			{
				IDataRow record = buffer.Take();

				if (record == null) { throw new Exception("No record found."); }

				WriteHeader(writer, record);
				WriteRecord(writer, record);

				while (!buffer.IsCompleted())
				{
					record = buffer.Take();

					//No more to read
					if (record == null) { break; }

					WriteRecord(writer, record);
				}
				writer.Flush();
			}
		}
		
		[TestMethod]
		public void RunFullTest()
		{
			try
			{
				CsvInputProcessor input = new CsvInputProcessor(20000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\FULLTEXT.csv");
				AggregateManager manager = new AggregateManager(input, 20000, 1);
				Aggregate.Aggregate aggregate = new Aggregate.Aggregate(@"(i =>
				{
					object ret = null;
					if (i is string)
					{
						string a = (string)i;
						ret = a.Substring(0, 1);
					}

					return ret;
				})", "field1", "bubba", Guid.NewGuid());

				Aggregate.Aggregate aggregate1 = new Aggregate.Aggregate(@"(i =>
				{
					object ret = null;
					if (i is string)
					{
						string a = (string)i;
						ret = a.Substring(0, 4);
					}

					return ret;
				})", "field1", "field1", Guid.NewGuid());

				manager.AddAggregate(aggregate);
				manager.AddAggregate(aggregate1);

				CsvOutputProcessor output = new CsvOutputProcessor(manager, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\FULLTEXT231.csv");

				BaseDataProcessor processor = new BaseDataProcessor(input, manager, output);

				processor.Start();

				while (!processor.IsFinished())
				{
					Thread.Sleep(1000);
				}
			}
			catch (Exception e) { Assert.Fail(e.Message); }
			

			//TODO: verify new columns
		}

		private void WriteHeader(CsvHelper.CsvWriter writer, IDataRow record)
		{
			int columnCount = record.GetColumnCount();
			for (int i = 0; i < columnCount; i++)
			{
				string columnName = record.GetColumnName(i);
				writer.WriteField(columnName);
			}

			writer.NextRecord();
		}

		private void WriteRecord(CsvHelper.CsvWriter writer, IDataRow record)
		{
			int columnCount = record.GetColumnCount();
			for (int i = 0; i < columnCount; i++)
			{
				var field = record.GetColumn(i);
				writer.WriteField(field);
			}
			writer.NextRecord();
		}
	}
}