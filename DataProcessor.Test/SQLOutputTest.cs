using DataProcessor.DataRow;
using DataProcessor.Interfaces;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using DataProcessor.Output;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace DataProcessor.Test
{
	[TestClass]
	public class SQLOutputTest
	{
		private ConcurrentBuffer _buffer;

		[TestInitialize]
		public void Init()
		{
			//100,000,000
			long size = 10000000;
			 this._buffer = new ConcurrentBuffer(100000); 

			var rng = System.Security.Cryptography.RNGCryptoServiceProvider.Create();
			Task.Run(() =>
			{
				for(long i = 0; i < size; i++)
				{
					Task.Run(() =>
					{
						byte[] bytes = new byte[1024];
						rng.GetBytes(bytes);
						IDataRow row = new BaseDataRow();
						row.SetColumn("Id", Guid.NewGuid());
						row.SetColumn("Name", Convert.ToBase64String(bytes, 0, 20));
						row.SetColumn("Salary", Convert.ToInt32(bytes[0]));
						this._buffer.Add(row);
					});
				}

				this._buffer.CompleteAdding();
			});
		}

		[TestMethod]
		public void Put()
		{
			var connBuilder = new SqlConnectionStringBuilder();
			connBuilder.DataSource = System.Net.Dns.GetHostName();
			connBuilder.InitialCatalog = "CsvImportTest";
			connBuilder.UserID = connBuilder.Password = "user";

			string connectionString = connBuilder.ToString();

			//TODO: Figure out how Mocking SQL works and implement it.  I don't like having sql server on my consumer machine.
			SQLOutput output = new SQLOutput(_buffer, int.MaxValue, connectionString, "dbo.Import");
			output.Start();

			while(output.IsCompleted() == false)
			{
				Thread.Sleep(1000);
			}
		}
	}
}
