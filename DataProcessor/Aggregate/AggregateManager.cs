using DataProcessor.Base;
using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;

namespace DataProcessor.Aggregate
{
	[Serializable]
	public class AggregateManager : BaseProcessor, IAggregateManager
	{
		public IInputProcessor InputProcessor { get; private set; }

		private List<IAggregate> _aggregates = new List<IAggregate>();

		[XmlIgnore]
		private bool _isFinished = false;
		
		public bool IsFinished() => _isFinished;

		[XmlIgnore]
		private ConcurrentBuffer _readBuffer;

		public AggregateManager(IInputProcessor inputProcessor, int boundedCapacity, int numOfThreads) : base(numOfThreads)
		{
			this.InputProcessor = inputProcessor;
			this._readBuffer = new ConcurrentBuffer(boundedCapacity);
			this._isFinished = false;
		}

		#region Aggregate Functions
		public void AddAggregate(IAggregate aggregate)
		{
			this._aggregates.Add(aggregate);
		}

		public void AddAggregate(int position, IAggregate aggregate)
		{
			this._aggregates.Insert(position, aggregate);
		}

		public void RemoveAggregate(int position)
		{
			this._aggregates.RemoveAt(position);
		}
		public bool RemoveAggregate(IAggregate aggregate)
		{
			bool ret = this._aggregates.Remove(aggregate);
			return ret;
		}

		public List<IAggregate> GetAggregates()
		{
			var list = this._aggregates.ToList();
			return list;
		}
		#endregion

		public List<IDataRow> GetBufferItems()
		{
			var list = _readBuffer.GetBufferItems();
			return list;
		}

		public IDataRow Read()
		{
			IDataRow row = null;
			try 
			{
				if (this._readBuffer.IsCompleted() == false) { row = this._readBuffer.Take(); }
				else { this._isFinished = true; }
			}
			catch (Exception e) { this._isFinished = true; }

			return row;
		}

		public void ProcesstTest()
		{
			this._isRunning = true;
			Process();
			this._isRunning = false;
		}

		public override void Process()
		{
			while (this._isRunning)
			{
				var record = this.InputProcessor.Read();

				if (record == null) { this._readBuffer.CompleteAdding(); break; }

				var aggregates = GetAggregates();
				for (int i = 0; i < aggregates.Count; i++)
				{
					var aggregate = aggregates[i];
					var value = record.GetColumn(aggregate.ColumnName);

					value = aggregate.ProcessItem(value);

					record.SetColumn(i, value);
				}

				this._readBuffer.Add(record);
			}
		}
	}
}
