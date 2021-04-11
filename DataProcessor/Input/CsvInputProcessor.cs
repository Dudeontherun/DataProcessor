using DataProcessor.Base;
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
		private bool _isFinished = false;
		public bool IsFinished() => _isFinished;

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

		public IDataRow Read()
		{
			IDataRow row = null;
			try
			{
				if (this._readBuffer.IsCompleted() == false)
				{
					if (this._readBuffer.TryTake(out row) == false && this._readBuffer.IsCompleted() == true)
					{
						if(row == null) { throw new Exception("WHAT HAPPENED???"); }
					}
					else
					{
						if (row == null) { row = this._readBuffer.Take(); }
					}
				}
				else { this._isFinished = true; }
			}
			catch (Exception e) { this._isFinished = true; }
			return row;
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

		List<IDataRow> IInputProcessor.GetBufferItems() => this.GetBufferItems();

		void IInputProcessor.Start() => base.Start();
		void IInputProcessor.Stop(bool forceStop) => base.Stop(forceStop);
	}
}
