using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataProcessor.Buffer
{
	public interface IBuffer
	{
		public IDataRow Take();
		public bool TryTake(out IDataRow row);
		public void Add(IDataRow row);
		public List<IDataRow> GetBufferItems();
		public void CompleteAdding();
		public bool IsCompleted();
	}
}
