using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.DataRow;
using DataProcessor.Interfaces;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace DataProcessor.Input
{
	[Serializable]
	public class CsvInputThreadSafe : BaseProcessor, IInputProcessor
	{
		[XmlIgnore]
		private readonly ConcurrentBuffer _buffer;

		public string FilePath;

		public CsvInputThreadSafe(int boundedCapacity, string filePath, int numOfThreads): base (numOfThreads)
		{
			this.FilePath = filePath;
			if (!File.Exists(filePath)) { throw new ArgumentException("File doesn't exist"); }

			this._buffer = new ConcurrentBuffer(boundedCapacity);
		}

		IProcessor IInputProcessor.Processor => this;

		bool IInputProcessor.IsSingleThreaded => true;

		public List<IDataRow> GetBufferItems()
		{
			var list = this._buffer.GetBufferItems();
			return list;
		}

		private readonly object _getLineLocker = new object();
		private bool _hasInitiated = false;
		private IEnumerator<string> _fileLines;
		private string[] _headers = Array.Empty<string>();
		private char[] _delimiters;

		public override void Process()
		{
			if (this._hasInitiated == false)
			{
				lock (this._getLineLocker)
				{
					if (this._hasInitiated == false)
					{
						if (!File.Exists(this.FilePath)) { throw new Exception("File " + this.FilePath + " does not exist."); }

						var enumerable = File.ReadLines(this.FilePath);
						this._fileLines = enumerable.GetEnumerator();
						this._fileLines.MoveNext();
						//TODO: auto detect delimiters
						_delimiters = new char[] { ',', '\t' };

						//Grab columns first.
						string headerFields = this._fileLines.Current;
						this._fileLines.MoveNext();
						var columns = headerFields.Split(_delimiters);
						if (columns.Length == 0) { throw new Exception("Columns names are bad,  No column names found"); }

						this._headers = columns;

						this._hasInitiated = true;
					}
				}
			}

			string line = String.Empty;
			bool hasNext = false;
			lock(this._getLineLocker)
			{
				if (this._isRunning == false) { return; }
				
				line = this._fileLines.Current;
				hasNext = this._fileLines.MoveNext();
				
				if(hasNext == false) { this._isRunning = false; }
			}

			//Process a row and put it into buffer.
			string[] columnDatas = line.Split(_delimiters);
			IDataRow data = new BaseDataRow(this._headers.ToArray(), columnDatas);
			this._buffer.Add(data);


			if (hasNext == false)
			{
				while (this._buffer.WaitingToAdd > 0)
				{ }

				this._buffer.CompleteAdding();
			}
			
		}

		void IInputProcessor.Start() => base.Start();
		void IInputProcessor.Stop(bool forceStop) => base.Stop(forceStop);

		IDataRow IOutBuffer.Take() => this._buffer.Take();

		bool IOutBuffer.TryTake(out IDataRow row) => this._buffer.TryTake(out row);

		bool IOutBuffer.IsCompleted() => this._buffer.IsCompleted();
	}
}