using System;

namespace DataProcessor.Interfaces
{
	public interface IAggregate
	{
		public Guid Id { get; set; }

		public string ColumnName { get; set; }
		public object ProcessItem(object item);

		public Func<object, object> Function { get; set; }
	}
}
