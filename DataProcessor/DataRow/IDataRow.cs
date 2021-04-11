using System.Collections.Generic;

namespace DataProcessor.Interfaces
{
	public interface IDataRow
	{
		public object GetColumn(int index);
		public object GetColumn(string columnName);
		public void SetColumn(string columnName, object obj);
		public void SetColumn(int index, object obj);
		public void AddColumn(string columnName);
		public void AddColumn(string columnName, object obj);
		public string GetColumnName(int index);
		public int GetColumnCount();

		public IEnumerable<string> GetColumns();
		public IEnumerable<object> GetValues();

	}
}
