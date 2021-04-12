﻿using DataProcessor.Buffer;

namespace DataProcessor.Interfaces
{
	public interface IOutputProcessor: IProcessor
	{
		protected IOutBuffer InputBuffer { get; }

		public bool IsSingleThreaded { get; }

		protected void Process();

		public bool IsCompleted();
	}
}