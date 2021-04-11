using DataProcessor.Base;
using DataProcessor.Interfaces;
using System;
using System.IO;

namespace DataProcessor.Output
{
	public sealed class CsvOutputProcessor : BaseProcessor, IOutputProcessor
	{
		private FileInfo _info;
		IProcessor IOutputProcessor.Processor { get => this; }
		bool IOutputProcessor.IsSingleThreaded => true;
		public IAggregateManager Manager { get; private set; }

		private bool _isFinished = false;
		public bool IsFinished() => _isFinished;

		public CsvOutputProcessor(IAggregateManager manager, string filePath) : base(1)
		{
			this._info = new FileInfo(filePath);
			if (!this._info.Exists) { _info.Create().Close(); }

			this.Manager = manager;
		}

		public void ProcessTest()
		{
			this._isRunning = true;
			Process();
			this._isRunning = false;
		}

		void IOutputProcessor.Process() => Process();
		public override void Process()
		{
			var config = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
			{
				AllowComments = false
			};

			using (CsvHelper.CsvWriter writer = new CsvHelper.CsvWriter(new StreamWriter(this._info.FullName), config))
			{
				IDataRow record = this.Manager.Read();

				if (record == null) { throw new Exception("No record found."); }

				WriteHeader(writer, record);
				WriteRecord(writer, record);

				while (this._isRunning)
				{
					record = this.Manager.Read();

					//No more to read
					if (record == null) { break; }

					WriteRecord(writer, record);
				}
			}

			this._isFinished = true;
			this._isRunning = false;
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

		void IOutputProcessor.Start() => base.Start();
		void IOutputProcessor.Stop(bool forceStop) => base.Stop(forceStop);

	}
}