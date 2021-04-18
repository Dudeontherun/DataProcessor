using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataProcessor.DataRow
{
	public class BaseDataRow : IDataRow
	{
		private Dictionary<string, object> _fields = new Dictionary<string, object>();
		private Dictionary<int, string> _index = new Dictionary<int, string>();

		public BaseDataRow()
		{ }

		public BaseDataRow(string[] headerFields, string[] fields)
		{
			if (headerFields == null || fields == null) { throw new ArgumentNullException(); }
			if (headerFields.Length < fields.Length) { throw new ArgumentException("More header fields than fields"); }

			for (int i = 0; i < headerFields.Length; i++)
			{
				string header = headerFields[i];
				string value = (i < fields.Length) ? fields[i] : String.Empty;
				this._fields.Add(header, value);
				this._index.Add(i, header);
			}
		}

		public void AddColumn(string columnName)
		{
			this._fields.Add(columnName, String.Empty);
			this._index.Add(this._fields.Count - 1, columnName);
		}

		public void AddColumn(string columnName, object obj)
		{
			this._fields.Add(columnName, obj);
			this._index.Add(this._fields.Count - 1, columnName);
		}

		public object GetColumn(int index)
		{
			string header = this._index[index];
			object ret = this._fields[header];
			return ret;
		}

		public object GetColumn(string columnName)
		{
			object ret = null;
			if (this._fields.ContainsKey(columnName)) { ret = this._fields[columnName]; }

			return ret;
		}

		public int GetColumnCount()
		{
			return this._fields.Count();
		}

		public string GetColumnName(int index)
		{
			return this._index[index];
		}
		public void SetColumn(string columnName, object obj)
		{
			this._fields.Add(columnName, obj);
			this._index.Add(this._fields.Count - 1, columnName);
		}
		public void SetColumn(int index, object obj)
		{
			var header = this._index[index];
			this._fields[header] = obj;
		}

		private bool _dispose = false;
		public void Dispose()
		{
			if (!_dispose)
			{
				this._fields.Clear();
				this._index.Clear();
				this._fields = null;
				this._index = null;
				this._dispose = true;
			}
		}

		public IEnumerable<object> GetValues()
		{
			var values = this._fields.Select(i => i.Value);
			return values;
		}

		public IEnumerable<string> GetColumns()
		{
			var columns = this._fields.Select(i => i.Key);
			return columns;
		}

		public int GetOrdinal(string name)
		{
			if(this._index.ContainsValue(name)) { return this._index.Single(i => i.Value == name).Key; }
			else { throw new Exception("name is not found"); }
		}
	}
}
