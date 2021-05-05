using DataProcessor.Aggregate;
using DataProcessor.Base;
using DataProcessor.Input;
using DataProcessor.Interfaces;
using DataProcessor.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataProcessor.Test
{
	[TestClass]
	public class InputProcessor
	{
		#region Populate Data
		private int _numOfRows = 1000000;

		[TestInitialize]
		public void PopulateTestData()
		{

			var config = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
			{
				AllowComments = false,
				BufferSize = 10000,
			};

			ConcurrentBuffer buffer = new ConcurrentBuffer(10000);
			Task.Run(() =>
			{
				Enumerable.Range(0, _numOfRows).ToList().ForEach(j =>
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

			using (CsvHelper.CsvWriter writer = new CsvHelper.CsvWriter(new StreamWriter(@"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest.csv", false), config))
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

		#endregion

		[TestMethod]
		public void RunInput()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest.csv");

			processor.ProcessTest();

			int count = processor.GetBufferItems().Count;

			Assert.AreEqual(_numOfRows, count);
		}

		[TestMethod]
		public void RunAggregate()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest.csv");

			processor.ProcessTest();

			AggregateManager manager = new AggregateManager(processor, 64000, 1);

			manager.ProcessTest();

			int count = manager.GetBufferItems().Count;
			Assert.AreEqual(_numOfRows, count);

			count = processor.GetBufferItems().Count;
			Assert.AreEqual(0, count);
		}

		[TestMethod]
		public void RunCsvInputAndOutput()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest.csv");

			processor.ProcessTest();

			AggregateManager manager = new AggregateManager(processor, 64000, 1);

			manager.ProcessTest();

			CsvOutputProcessor output = new CsvOutputProcessor(manager, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest2.csv");

			output.ProcessTest();

			int count = manager.GetBufferItems().Count;
			Assert.AreEqual(0, count);

			count = processor.GetBufferItems().Count;
			Assert.AreEqual(0, count);
		}


		[TestMethod]
		public void RunImprovedInput()
		{
			//CsvInputMultiProcessor InboundProcessor = new CsvInputMultiProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestData.csv", 5);
			CsvInputMultiProcessor inboundProcessor = new CsvInputMultiProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\Fixed-NETSTAFFHR_LD_2020_09_01_LD.csv", 1);

			BufferDepleter output = new BufferDepleter(inboundProcessor, 24);

			var processor = new BaseDataProcessor(inboundProcessor, output);

			processor.Start();

			while (!processor.IsFinished())
			{
				Thread.Sleep(1000);
			}
		}

		[TestMethod]
		public void RunThreadSafeInput()
		{
			//CsvInputMultiProcessor InboundProcessor = new CsvInputMultiProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestData.csv", 5);
			CsvInputThreadSafe inboundProcessor = new CsvInputThreadSafe(1000000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\InputTest.csv", 24);

			var output = new BufferDepleter(inboundProcessor, 24);

			var processor = new BaseDataProcessor(inboundProcessor, output);

			processor.Start();

			while (!processor.IsFinished())
			{
				Thread.Sleep(1000);
			}
		}
	}
}
