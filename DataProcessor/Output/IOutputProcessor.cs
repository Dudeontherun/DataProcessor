namespace DataProcessor.Interfaces
{
	public interface IOutputProcessor
	{
		public IProcessor Processor { get; }

		public IAggregateManager Manager { get; }

		public bool IsSingleThreaded { get; }

		protected void Process();

		public bool IsFinished();
		public void Start();
		public void Stop(bool forecStop);
	}
}