using System;

namespace DataProcessor.Interfaces
{
	public interface IProcessor
	{
		public Guid Id { get; set; }
		/// <summary>
		/// Set amount of threads dedicated to input processor.
		/// You can use this method while the processor is running.
		/// </summary>
		public void SetThreads(int numOfThreads);
		public int GetNumOfThreads();

		/// <summary>
		/// Start threads to start grabbing records and place them into a buffer.
		/// </summary>
		public void Start();

		/// <summary>
		/// Stop the processor.
		/// </summary>
		/// <param name="forceStop">Force abort threads from processing.</param>
		public void Stop(bool forceStop);
	}
}
