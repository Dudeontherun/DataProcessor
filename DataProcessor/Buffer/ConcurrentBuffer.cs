using DataProcessor.Buffer;
using DataProcessor.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace DataProcessor
{
	public class ConcurrentBuffer : IDisposable, IBuffer, IOutBuffer
	{
		private BlockingCollection<IDataRow> _buffer;

		public int BoundedCapacity { get { return this._buffer.BoundedCapacity; } }

		public ConcurrentBuffer(int boundedCapacity)
		{
			this._buffer = new BlockingCollection<IDataRow>(boundedCapacity);
		}

		public IDataRow Take()
		{
			if (this._buffer.IsCompleted == false) 
			{
				try
				{
					return this._buffer.Take();
				}
#pragma warning disable CS0168 // The variable 'e' is declared but never used
				catch (Exception e) { return null; }
#pragma warning restore CS0168 // The variable 'e' is declared but never used
			}
			else { return null; }
		}

		public bool TryTake(out IDataRow row)
		{
			return this._buffer.TryTake(out row);
		}

		private int _waitingToAdd = 0;
		public int WaitingToAdd { get => _waitingToAdd; }

		public void Add(IDataRow row)
		{
			Interlocked.Increment(ref this._waitingToAdd);

			try { this._buffer.Add(row); }
			catch(Exception e) { throw e; }
			finally {  Interlocked.Decrement(ref _waitingToAdd); }
		}

		public List<IDataRow> GetBufferItems()
		{
			var arr = new IDataRow[_buffer.BoundedCapacity];
			this._buffer.CopyTo(arr, 0);
			var list = arr.ToList();
			list.RemoveAll(i => i == null);
			return list;
		}

		public int Count => _buffer.Count;

		public void CompleteAdding()
		{
			this._buffer.CompleteAdding();
		}

		public bool IsCompleted()
		{
			return this._buffer.IsCompleted;
		}

		private bool _isDisposed = false;
		public void Dispose()
		{
			if (!this._isDisposed)
			{
				this._buffer.Dispose();
				this._isDisposed = true;
			}
		}
	}
}
