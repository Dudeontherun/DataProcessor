using DataProcessor.Base;
using DataProcessor.Buffer;
using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataProcessor.Output
{
	public sealed class BufferDepleter : BaseProcessor, IOutputProcessor
	{
		private IOutBuffer _buffer;
		IOutBuffer IOutputProcessor.InputBuffer => _buffer;

		bool IOutputProcessor.IsSingleThreaded => false;

		public BufferDepleter(IOutBuffer inputBuffer, int numOfThreads) : base(numOfThreads)
		{
			this._buffer = inputBuffer;
		}

		public bool IsCompleted()
		{
			bool ret = this._buffer.IsCompleted();
			return ret;
		}

		public override void Process()
		{
			this._buffer.Take();
			this._isRunning = !IsCompleted();
		}
	}
}
