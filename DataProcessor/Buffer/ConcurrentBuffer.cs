using DataProcessor.Buffer;
using DataProcessor.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

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
				catch (Exception e) { return null; }
			}
			else { return null; }
		}

		public bool TryTake(out IDataRow row)
		{
			return this._buffer.TryTake(out row);
		}

		public void Add(IDataRow row)
		{
			this._buffer.Add(row);
		}

		public List<IDataRow> GetBufferItems()
		{
			var arr = new IDataRow[_buffer.BoundedCapacity];
			this._buffer.CopyTo(arr, 0);
			var list = arr.ToList();
			list.RemoveAll(i => i == null);
			return list;
		}

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
