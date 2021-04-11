using DataProcessor.Interfaces;
using System;

namespace DataProcessor.Base
{
	public class BaseDataProcessor
	{
		public IInputProcessor Input { get; set; }
		public IAggregateManager AggregateManager { get; set; }
		public IOutputProcessor Output { get; set; }

		public BaseDataProcessor(IInputProcessor input, IAggregateManager aggregateManager, IOutputProcessor output)
		{
			this.Input = input;
			this.AggregateManager = aggregateManager;
			this.Output = output;
		}

		public bool IsFinished()
		{
			return Input.IsFinished() && AggregateManager.IsFinished() && Output.IsFinished();
		}

		public void Start()
		{
			this.Input.Start();
			this.AggregateManager.Start();
			this.Output.Start();
		}

		public void Stop(bool forceStop = false)
		{
			this.Input.Stop(forceStop);
			this.AggregateManager.Stop(forceStop);
			this.Output.Stop(forceStop);
		}
	}
}
