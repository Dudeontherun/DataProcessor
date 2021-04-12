using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataProcessor.Buffer
{
	public interface IOutBuffer
	{
		public IDataRow Take();
		public bool TryTake(out IDataRow row);
		public List<IDataRow> GetBufferItems();
		public bool IsCompleted();
	}
}