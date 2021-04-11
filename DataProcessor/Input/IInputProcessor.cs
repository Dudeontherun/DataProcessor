using System.Collections.Generic;

namespace DataProcessor.Interfaces
{
	public interface IInputProcessor
	{
		public IProcessor Processor { get; }

		/// <summary>
		/// Grabs an item from the input processor.
		/// </summary>
		public IDataRow Read();

		public bool IsFinished();
		public List<IDataRow> GetBufferItems();

		public bool IsSingleThreaded { get; }

		public void Start();
		public void Stop(bool forceStop);
	}
}
