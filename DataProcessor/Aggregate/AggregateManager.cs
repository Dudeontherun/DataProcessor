﻿using DataProcessor.Base;
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
		private ConcurrentBuffer _outputBuffer;

		private List<IAggregate> _aggregates = new List<IAggregate>();

		[XmlIgnore]
		private bool _isFinished = false;

		public bool IsFinished() => _isFinished;


		public AggregateManager(IOutBuffer inputBuffer, int boundedCapacity, int numOfThreads) : base(numOfThreads)
		{
			this.InputBuffer = inputBuffer;
			this._outputBuffer = new ConcurrentBuffer(boundedCapacity);
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
			var list = _outputBuffer.GetBufferItems();
			return list;
		}

		public void ProcessTest()
		{
			this._isRunning = true;
			while(this._isRunning)
				Process();
			this._isRunning = false;
		}

		public override void Process()
		{
			var record = this.InputBuffer.Take();

			if (record == null)
			{
				while (this._outputBuffer.WaitingToAdd > 0)
				{ }

				this._outputBuffer.CompleteAdding();
				this._isRunning = false;
				return;
			}

			var aggregates = GetAggregates();

			//TODO: Change this to know which object to use.
			IDataRow newRecord = new BaseDataRow();

			for (int i = 0; i < aggregates.Count; i++)
			{
				var aggregate = aggregates[i];
				aggregate.Init();

				object[] oldValues = new object[aggregate.OldColumnNames.Length];
				for (int j = 0; j < oldValues.Length; j++)
				{
					string oldColumnName = aggregate.OldColumnNames[j];
					var value = record.GetColumn(oldColumnName);
					oldValues[j] = value;
				}

				var newValue = aggregate.ProcessColumn(oldValues);

				newRecord.AddColumn(aggregate.NewColumnName, newValue);
			}

			this._outputBuffer.Add(newRecord);
		}

		IDataRow IOutBuffer.Take() => this._outputBuffer.Take();

		bool IOutBuffer.TryTake(out IDataRow row) => this._outputBuffer.TryTake(out row);

		bool IOutBuffer.IsCompleted() => this._outputBuffer.IsCompleted();

		public void AddAggregate(params IAggregate[] aggregates)
		{
			if(aggregates == null || aggregates.Length <= 0) { return; }

			foreach (var arg in aggregates)
			{
				this._aggregates.Add(arg);
			}
		}
	}
}