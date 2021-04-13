using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.DataRow;
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
		protected IOutBuffer InputBuffer { get; private set; }
		IOutBuffer IAggregateManager.InputBuffer => InputBuffer;

		[XmlIgnore]
		private ConcurrentBuffer _readBuffer;

		private List<IAggregate> _aggregates = new List<IAggregate>();

		[XmlIgnore]
		private bool _isFinished = false;

		public bool IsFinished() => _isFinished;


		public AggregateManager(IOutBuffer inputBuffer, int boundedCapacity, int numOfThreads) : base(numOfThreads)
		{
			this.InputBuffer = inputBuffer;
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
				var record = this.InputBuffer.Take();

				if (record == null) { this._readBuffer.CompleteAdding(); break; }

				var aggregates = GetAggregates();

				//TODO: Change this to copy same object from record.
				IDataRow newRecord = new BaseDataRow();

				for (int i = 0; i < aggregates.Count; i++)
				{
					var aggregate = aggregates[i];
					var value = record.GetColumn(aggregate.OldColumnName);

					var newValue = aggregate.ProcessColumn(value);

					newRecord.AddColumn(aggregate.NewColumnName, newValue);
				}

				this._readBuffer.Add(record);
			}
		}

		IDataRow IOutBuffer.Take() => this._readBuffer.Take();

		bool IOutBuffer.TryTake(out IDataRow row) => this._readBuffer.TryTake(out row);

		bool IOutBuffer.IsCompleted() => this._readBuffer.IsCompleted();
	}
}