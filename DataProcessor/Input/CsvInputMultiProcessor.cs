using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.DataRow;
using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataProcessor.Input
{
	[Serializable]
	public class CsvInputMultiProcessor : BaseProcessor, IInputProcessor
	{
		[XmlIgnore]
		private readonly ConcurrentBuffer _readBuffer;

		public string FilePath;

		private int _numOfThread;

		public CsvInputMultiProcessor(int boundedCapacity, string filePath, int numOfThread) : base(1)
		{
			this.FilePath = filePath;
			if (!File.Exists(filePath)) { throw new ArgumentException("File doesn't exist"); }

			this._readBuffer = new ConcurrentBuffer(boundedCapacity);
			this._numOfThread = numOfThread;
		}

		IProcessor IInputProcessor.Processor => this;

		bool IInputProcessor.IsSingleThreaded => false;

		public List<IDataRow> GetBufferItems()
		{
			var list = this._readBuffer.GetBufferItems();
			return list;
		}


		//Used to create access streams
		private object _locker = new object();
		private MemoryMappedFile _memoryFile;
		private FileInfo _fileInfo;
		private int _fileOpen;
		private System.Collections.ObjectModel.ReadOnlyCollection<string> _columnNames;

		public override void Process()
		{
			Parallel.ForEach(File.ReadLines(this.FilePath), new ParallelOptions() { MaxDegreeOfParallelism = this._numOfThread },
			(line, state, index) => {
				if(index == 0) 
				{
					var columns = line.Split(',');
					if (columns.Length == 0) { throw new Exception("Columns names are bad,  No column names found"); }

					this._columnNames = columns.ToList().AsReadOnly();
				}
				else { while (this._columnNames == null || this._columnNames.Count == 0) { int i = 0; } }

				string[] columnDatas = line.Split(',');
				IDataRow data = new BaseDataRow();
				for(int i = 0; i < _columnNames.Count; i++)
				{
					var columnName = this._columnNames[i];
					var columnData = columnDatas[i];

					data.AddColumn(columnName, columnData);
				}

				this._readBuffer.Add(data);
			});

			this._readBuffer.CompleteAdding();
			this._isRunning = false;
		}


		#region commeneted out code

		//So... the architecture I made is kinda suck for this.
//		Stream viewer;
//		long streamSize;
//		long offSet;
//			lock (_locker)
//			{
//				if(this._fileInfo == null) { this._fileInfo = new FileInfo(this.FilePath); }
//				int numThreads = this.GetNumOfThreads();
//		streamSize = this._fileInfo.Length / numThreads;
//				offSet = _fileOpen++ * streamSize;

//				if (this._memoryFile == null)
//				{
//			this._memoryFile = MemoryMappedFile.CreateFromFile(this.FilePath, FileMode.Open);

//			var getColumns = this._memoryFile.CreateViewStream(0, streamSize);
//			using (StreamReader read = new StreamReader(getColumns))
//			{
//				if (read.EndOfStream == false)
//				{
//					string columnLine = read.ReadLine();
//					var columns = columnLine.Split(',');

//					columnNames.AddRange(columns);
//				}
//			}

//			File.
//				}

//		viewer = this._memoryFile.CreateViewStream(offSet, streamSize);
//			}

//			//Go thru
//			using (StreamReader read = new StreamReader(viewer))
//			{
//				if(read.EndOfStream == false && offSet == 0) { read.ReadLine(); }
//while (read.EndOfStream == false)
//{
//	string line = read.ReadLine();

//	var columns = line.Split(',');
//	IDataRow row = new BaseDataRow();
//	for (int i = 0; i < columnNames.Count; i++)
//	{
//		string columnName = columnNames[i];
//		string columnData = columns[i];
//		row.AddColumn(columnName, columnData);
//	}

//	this._readBuffer.Add(row);
//}
//			}
		#endregion


		void IInputProcessor.Start() => base.Start();
		void IInputProcessor.Stop(bool forceStop) => base.Stop(forceStop);

		IDataRow IOutBuffer.Take() => this._readBuffer.Take();

		bool IOutBuffer.TryTake(out IDataRow row) => this._readBuffer.TryTake(out row);

		bool IOutBuffer.IsCompleted() => this._readBuffer.IsCompleted();
	}
}