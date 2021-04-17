using DataProcessor.Interfaces;
using Microsoft.CodeDom.Providers.DotNetCompilerPlatform;
using System;
using System.Linq;

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
			this.OldColumnNames = new string[1] { columnName };
			this.NewColumnName = columnName;
			this.Function = function;
		}

		public Aggregate(Func<object, object> function, string oldColumName, string newColumnName, Guid identifier)
		{
			this._id = identifier;
			this.OldColumnNames = new string[1] { oldColumName };
			this.NewColumnName = newColumnName;
			this.Function = function;
		}

		public Aggregate(Func<object, object> function, string[] oldColumnName, string newColumnName, Guid identifier)
		{
			this._id = identifier;
			this.OldColumnNames = oldColumnName;
			this.NewColumnName = newColumnName;
			this.Function = function;
		}

		public Func<object, object> Function { get; set; }
		public string[] OldColumnNames { get; set; }
		public string NewColumnName { get; set; }

		public object ProcessColumn(params object[] item)
		{
			if(item == null || item.Length != OldColumnNames.Length) { throw new Exception("Not enough parameters provided"); }
			
			object result = null;
			try { result = Function.Invoke(item); }
			catch (Exception e) { throw new Exception("Inovking function failed", e); }

			return result;
		}

		//public static void Test()
		//{
		//	var refs = AppDomain.CurrentDomain.GetAssemblies();
		//	var refFiles = refs.Where(i => !i.IsDynamic).Select(i => i.Location).ToArray();
		//	var codeProvider = new CSharpCodeProvider()
		//}
	}
}
