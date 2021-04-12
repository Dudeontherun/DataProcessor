using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.DataRow;
using DataProcessor.Interfaces;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

namespace DataProcessor.Input
{
	[Serializable]
	public class CsvInputProcessor : BaseProcessor, IInputProcessor
	{
		[XmlIgnore]
		private readonly ConcurrentBuffer _readBuffer;


		public string FilePath;

		public CsvInputProcessor(int boundedCapacity, string filePath) : base(1)
		{
			this.FilePath = filePath;
			if (!File.Exists(filePath)) { throw new ArgumentException("File doesn't exist"); }

			this._readBuffer = new ConcurrentBuffer(boundedCapacity);
		}

		IProcessor IInputProcessor.Processor => this;

		bool IInputProcessor.IsSingleThreaded => true;

		public List<IDataRow> GetBufferItems()
		{
			var list = this._readBuffer.GetBufferItems();
			return list;
		}

		public void ProcessTest()
		{
			this._isRunning = true;
			Process();
			this._isRunning = false;
		}

		public override void Process()
		{
			if(!File.Exists(this.FilePath)) { throw new Exception("File " + this.FilePath + " does not exist."); }

			bool skipHeader = false;
			string[] headers = new string[0];
			using (TextFieldParser parser = new TextFieldParser(new StreamReader(File.OpenRead(this.FilePath), bufferSize: 8192 * 10, leaveOpen: false)))
			{
				parser.TextFieldType = FieldType.Delimited;
				parser.SetDelimiters(",", "\t");

				while (this._isRunning && this._readBuffer.IsCompleted() == false)
				{
					if (parser.EndOfData) { break; }
					var fields = parser.ReadFields();
					if (skipHeader == false) { headers = fields; skipHeader = true; continue; }

					IDataRow row = new BaseDataRow(headers, fields);
					this._readBuffer.Add(row);
				}
			}

			this._readBuffer.CompleteAdding();
			this._isRunning = false;
		}

		void IInputProcessor.Start() => base.Start();
		void IInputProcessor.Stop(bool forceStop) => base.Stop(forceStop);

		IDataRow IOutBuffer.Take() => this._readBuffer.Take();

		bool IOutBuffer.TryTake(out IDataRow row) => this._readBuffer.TryTake(out row);

		bool IOutBuffer.IsCompleted() => this._readBuffer.IsCompleted();
	}
}