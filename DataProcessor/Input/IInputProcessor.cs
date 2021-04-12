using DataProcessor.Buffer;
using System.Collections.Generic;

namespace DataProcessor.Interfaces
{
	public interface IInputProcessor: IOutBuffer
	{
		public IProcessor Processor { get; }

		public bool IsSingleThreaded { get; }

		public void Start();
		public void Stop(bool forceStop);
	}
}
