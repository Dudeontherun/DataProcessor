using DataProcessor.Interfaces;
using System;

namespace DataProcessor.Base
{
	public class BaseDataProcessor
	{
		public IInputProcessor Input { get; set; }
		public IAggregateManager AggregateManager { get; set; }
		public IOutputProcessor Output { get; set; }

		public BaseDataProcessor(IInputProcessor input, IOutputProcessor output)
		{
			this.Input = input;
			this.Output = output;
		}

		public BaseDataProcessor(IInputProcessor input, IAggregateManager aggregateManager, IOutputProcessor output)
		{
			this.Input = input;
			this.AggregateManager = aggregateManager;
			this.Output = output;
		}

		public bool IsFinished()
		{
			bool inputCompleted = (Input != null) ? Input.IsCompleted() : true;
			bool aggregateCompleted = (AggregateManager != null) ? AggregateManager.IsCompleted() : true;
			bool outputCompleted = (Output != null) ? Output.IsCompleted() : true;
			return inputCompleted && aggregateCompleted && outputCompleted;
		}

		public void Start()
		{
			if(this.Input != null) { this.Input.Start(); }
			if(this.AggregateManager != null) { this.AggregateManager.Start(); }
			if(this.Output != null) { this.Output.Start(); }
		}

		public void Stop(bool forceStop = false)
		{
			this.Input.Stop(forceStop);
			this.AggregateManager.Stop(forceStop);
			this.Output.Stop(forceStop);
		}
	}
}
