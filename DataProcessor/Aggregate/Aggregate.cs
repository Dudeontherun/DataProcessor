using DataProcessor.Interfaces;
using System;

namespace DataProcessor.Aggregate
{
	[Serializable]
	public class Aggregate : IAggregate
	{
		protected Guid _id;
		Guid IAggregate.Id { get => this._id; set => this._id = value; }

		public Aggregate(Func<object, object> function, string columnName, Guid identifer)
		{
			this._id = identifer;
			this.ColumnName = columnName;
			this.Function = function;
		}

		public Func<object, object> Function { get; set; }
		public string ColumnName { get; set; }

		public object ProcessItem(object item)
		{
			object result = null;
			try { result = Function.Invoke(item); }
			catch (Exception e) { throw e; }

			return result;
		}
	}
}
