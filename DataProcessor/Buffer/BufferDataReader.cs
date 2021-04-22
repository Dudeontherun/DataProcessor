using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace DataProcessor.Buffer
{
	public sealed class BufferDataReader : System.Data.IDataReader
	{
		public BufferDataReader(IOutBuffer buffer)
		{
			this._buffer = buffer;
		}

		public bool NextResult()
		{
			throw new NotImplementedException();
		}

		private Interfaces.IDataRow _currentRow;
		private int _recordsAffected = 0;
		private IOutBuffer _buffer;

		public bool Read()
		{
			bool ret = false;
			try
			{
				this._currentRow = this._buffer.Take();
				this._recordsAffected++;
				ret = !this._buffer.IsCompleted();
			}
#pragma warning disable CS0168 // The variable 'e' is declared but never used
			catch (Exception e) { ret = false; }
#pragma warning restore CS0168 // The variable 'e' is declared but never used

			return ret;
		}

		/// <summary>
		/// Grab column based on position
		/// </summary>
		/// <param name="i">index</param>
		/// <returns>column's data</returns>
		public object this[int i]
		{
			get
			{
				object column = _currentRow.GetColumn(i);
				return column;
			}
		}

		/// <summary>
		/// Grab column based on name
		/// </summary>
		/// <param name="name">Column's name</param>
		/// <returns></returns>
		public object this[string name]
		{
			get
			{
				object column = _currentRow.GetColumn(name);
				return column;
			}
		}

		/// <summary>
		/// wtf?
		/// </summary>
		public int Depth => throw new NotSupportedException();


		public bool IsClosed => this._buffer.IsCompleted();

		/// <summary>
		/// Usually, returns num of row thats changed, inserted, or deleted.
		/// Our case: inserted only.
		/// </summary>
		public int RecordsAffected => this._recordsAffected;

		public int FieldCount
		{
			get
			{
				if (this._currentRow == null) { Read(); }

				int ret = this._currentRow.GetColumnCount();
				return ret;
			}
		}

		/// <summary>
		/// Dispose everything I guess.
		/// </summary>
		public void Close()
		{
			this.Dispose();
		}


		private bool _disposed = false;
		/// <summary>
		/// Dispose our buffer
		/// </summary>
		public void Dispose()
		{
			if(!_disposed)
			{
				this._buffer.Dispose();
				this._disposed = true;
			}
		}


		public bool GetBoolean(int i)
		{
			var column = this._currentRow.GetColumn(i);
			bool ret = false;
			if (column is string) { bool.Parse((string)column); }
			else if (column is bool) { ret = (bool)column; }
			else if (column is int) { ret = (int)column == 1; }
			else { throw new Exception("column is unknown"); }

			return ret;
		}

		public byte GetByte(int i)
		{
			var column = this._currentRow.GetColumn(i);

			byte ret;
			BinaryFormatter formatter = new BinaryFormatter();
			using(MemoryStream memory = new MemoryStream())
			{
				formatter.Serialize(memory, column);
				memory.Position = 0;
				ret = (byte) memory.ReadByte();
			}
			return ret;
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException("What do you do with this?");
		}

		public char GetChar(int i)
		{
			var column = this._currentRow.GetColumn(i);
			char ret;
			if (column is char) { ret = (char)column; }
			else if (column is string) { ret = ((string)column)[0]; }
			else if (column is int) { ret = char.ConvertFromUtf32((int)column)[0]; }
			else { throw new Exception("unable to get char"); }

			return ret;
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException();
		}

		public IDataReader GetData(int i)
		{
			throw new NotSupportedException("There's not depth.");
		}


		/// <summary>
		/// Everything will be temporarly string
		/// </summary>
		/// <param name="i">index</param>
		public string GetDataTypeName(int i) => typeof(string).Name;

		public DateTime GetDateTime(int i)
		{
			var column = this._currentRow.GetColumn(i);
			DateTime ret;
			if (column is string) { ret = DateTime.Parse((string)column); }
			else if (column is DateTime) { ret = (DateTime)column; }
			else { throw new Exception("Couldn't understand column (Datetime)"); }

			return ret;
		}

		public decimal GetDecimal(int i)
		{
			var column = this._currentRow.GetColumn(i);
			decimal ret;
			if (column is decimal) { ret = (decimal)column; }
			else if (column is string) { ret = decimal.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (decimal)"); }

			return ret;
		}

		public double GetDouble(int i)
		{
			var column = this._currentRow.GetColumn(i);
			double ret;
			if (column is double) { ret = (double)column; }
			else if (column is string) { ret = double.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (double)"); }

			return ret;
		}

		public Type GetFieldType(int i)
		{
			var column = this._currentRow.GetColumn(i);
			var type = column.GetType();
			return type;
		}

		public float GetFloat(int i)
		{
			var column = this._currentRow.GetColumn(i);
			float ret;
			if (column is float) { ret = (float)column; }
			else if (column is string) { ret = float.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (float)"); }

			return ret;
		}

		public Guid GetGuid(int i)
		{ 
			var column = this._currentRow.GetColumn(i);
			Guid ret;
			if (column is Guid) { ret = (Guid)column; }
			else if (column is string) { ret = Guid.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (Guid)"); }

			return ret;
		}

		public short GetInt16(int i)
		{
			var column = this._currentRow.GetColumn(i);
			short ret;
			if (column is short) { ret = (short)column; }
			else if (column is string) { ret = short.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (short)"); }

			return ret;
		}

		public int GetInt32(int i)
		{
			var column = this._currentRow.GetColumn(i);
			int ret;
			if (column is int) { ret = (int)column; }
			else if (column is string) { ret = int.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (short)"); }

			return ret;
		}

		public long GetInt64(int i)
		{
			var column = this._currentRow.GetColumn(i);
			long ret;
			if (column is long) { ret = (long)column; }
			else if (column is string) { ret = long.Parse((string)column); }
			else { throw new Exception("Couldn't understand column (short)"); }

			return ret;
		}

		public string GetName(int i)
		{
			var name = this._currentRow.GetColumnName(i);
			return name;
		}

		public int GetOrdinal(string name)
		{
			var index = this._currentRow.GetOrdinal(name);
			return index;
		}

		public DataTable GetSchemaTable()
		{
			DataTable ret = new DataTable();
			var columns = this._currentRow.GetColumns();
			foreach(string name in columns)
			{
				DataColumn dataColumn = new DataColumn(name);
				ret.Columns.Add(dataColumn);
			}

			return ret;
		}

		public string GetString(int i)
		{
			var column = this._currentRow.GetColumn(i);
			string ret;
			//TODO: Add 36 more data types...
			if(column is string) { ret = (string)column; }
			else { throw new NotImplementedException(); }

			return ret;
		}

		public object GetValue(int i)
		{
			var column = this._currentRow.GetColumn(i);
			return column;
		}

		public int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}

		public bool IsDBNull(int i)
		{
			var column = this._currentRow.GetColumn(i);
			bool ret = column is null;

			return ret;
		}
	}
}
