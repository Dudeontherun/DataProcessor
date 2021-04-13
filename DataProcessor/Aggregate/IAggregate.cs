using System;

namespace DataProcessor.Interfaces
{
	public interface IAggregate
	{
		public Guid Id { get; set; }

		public string OldColumnName { get; set; }
		public string NewColumnName { get; set; }

		public object ProcessColumn(object item);

		public Func<object, object> Function { get; set; }
	}
}
