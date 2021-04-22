using DataProcessor.Aggregate;
using DataProcessor.Base;
using DataProcessor.Input;
using DataProcessor.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;

namespace DataProcessor.Test
{
	[TestClass]
	public class InputProcessor
	{
		[TestMethod]
		public void RunInput()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestData.csv");

			processor.ProcessTest();

			int count = processor.GetBufferItems().Count;

			Assert.AreEqual(4, count);
		}

		[TestMethod]
		public void RunAggregate()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestData.csv");

			processor.ProcessTest();

			AggregateManager manager = new AggregateManager(processor, 64000, 1);

			manager.ProcesstTest();

			int count = manager.GetBufferItems().Count;
			Assert.AreEqual(4, count);

			count = processor.GetBufferItems().Count;
			Assert.AreEqual(0, count);
		}

		[TestMethod]
		public void RunCsvInput()
		{
			CsvInputProcessor processor = new CsvInputProcessor(64000, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestData.csv");

			processor.ProcessTest();

			AggregateManager manager = new AggregateManager(processor, 64000, 1);

			manager.ProcesstTest();

			CsvOutputProcessor output = new CsvOutputProcessor(manager, @"C:\Users\Jesse\Desktop\VS Projects\DataProcessor\DataProcessor.Test\TestDataaa.csv");

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

			var connBuilder = new System.Data.SqlClient.SqlConnectionStringBuilder();
			connBuilder.DataSource = System.Net.Dns.GetHostName();
			connBuilder.InitialCatalog = "CsvImportTest";
			connBuilder.UserID = connBuilder.Password = "user";

			string connectionString = connBuilder.ToString();
			SQLOutput output = new SQLOutput(inboundProcessor, int.MaxValue, connectionString, "[dbo].Fasty");

			var processor = new BaseDataProcessor(inboundProcessor, output);

			processor.Start();

			while(!processor.IsFinished())
			{
				Thread.Sleep(1000);
			}
			
		}
	}
}
