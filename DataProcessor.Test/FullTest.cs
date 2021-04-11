﻿using DataProcessor.Interfaces;
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
		[TestMethod]
		public void RunFullTest()
		{
			int numOfRows = 20000000;

			var config = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
			{
				AllowComments = false,
				BufferSize = 10000,
			};

			ConcurrentBuffer buffer = new ConcurrentBuffer(1000);
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

			using (CsvHelper.CsvWriter writer = new CsvHelper.CsvWriter(new StreamWriter(@"C:\Users\Jesse\source\repos\DataProcessor.Test\FULLTEXT.csv", false), config))
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
			}

			CsvInputProcessor input = new CsvInputProcessor(20000, @"C:\Users\Jesse\source\repos\DataProcessor.Test\FULLTEXT.csv");
			AggregateManager manager = new AggregateManager(input, 20000, 1);
			Aggregate.Aggregate aggregate = new Aggregate.Aggregate(new Func<object, object>(i =>
			{
				object ret = null;	
				if(i is string)
				{
					string a = (string)i;
					ret = a.Substring(0, 1);
				}

				return ret;
			}), "field1", Guid.NewGuid());

			manager.AddAggregate(aggregate);

			CsvOutputProcessor output = new CsvOutputProcessor(manager, @"C:\Users\Jesse\source\repos\DataProcessor.Test\FULLTEXT231.csv");

			BaseDataProcessor processor = new BaseDataProcessor(input, manager, output);

			processor.Start();

			while(!processor.IsFinished())
			{
				Thread.Sleep(1000);
			}

			string x = "";
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