using System;

namespace DataProcessor.Interfaces
{
	public interface IAggregate
	{
		public Guid Id { get; set; }

		public string[] OldColumnNames { get; set; }
		public string NewColumnName { get; set; }

		public bool Init();

		public object ProcessColumn(params object[] item);

		public string Function { get; }
	}
}
