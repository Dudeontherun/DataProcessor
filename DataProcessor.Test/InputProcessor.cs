using DataProcessor.Aggregate;
using DataProcessor.Input;
using DataProcessor.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
		public void RunOutput()
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
	}
}
