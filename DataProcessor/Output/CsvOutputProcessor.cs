using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.Interfaces;
using System;
using System.IO;

namespace DataProcessor.Output
{
	public sealed class CsvOutputProcessor : BaseProcessor, IOutputProcessor
	{
		private FileInfo _info;
		bool IOutputProcessor.IsSingleThreaded => true;

		private IOutBuffer _inputBuffer;
		IOutBuffer IOutputProcessor.InputBuffer => this._inputBuffer;

		private bool _isFinished = false;

		public bool IsCompleted() => _isFinished;

		public CsvOutputProcessor(IOutBuffer inputBuffer, string filePath) : base(1)
		{
			this._info = new FileInfo(filePath);
			if (!this._info.Exists) { _info.Create().Close(); }

			this._inputBuffer = inputBuffer;
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
				IDataRow record = this._inputBuffer.Take();

				if (record == null) { throw new Exception("No record found."); }

				WriteHeader(writer, record);
				WriteRecord(writer, record);

				while (this._isRunning)
				{
					record = this._inputBuffer.Take();

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
	}
}