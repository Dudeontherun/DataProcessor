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
			this.OldColumnName = columnName;
			this.NewColumnName = columnName;
			this.Function = function;
		}

		public Aggregate(Func<object, object> function, string oldColumnName, string newColumnName, Guid identifier)
		{
			this._id = identifier;
			this.OldColumnName = oldColumnName;
			this.NewColumnName = newColumnName;
			this.Function = function;
		}

		public Func<object, object> Function { get; set; }
		public string OldColumnName { get; set; }
		public string NewColumnName { get; set; }

		public object ProcessColumn(object item)
		{
			object result = null;
			try { result = Function.Invoke(item); }
			catch (Exception e) { throw e; }

			return result;
		}
	}
}
