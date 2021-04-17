using DataProcessor.Buffer;
using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace DataProcessor.Output
{
	public sealed class SQLOutput : Base.BaseProcessor, IOutputProcessor
	{
		public bool IsSingleThreaded => true;

#pragma warning disable CS0108 // 'SQLOutput.Id' hides inherited member 'BaseProcessor.Id'. Use the new keyword if hiding was intended.
		public Guid Id { get; set; }
#pragma warning restore CS0108 // 'SQLOutput.Id' hides inherited member 'BaseProcessor.Id'. Use the new keyword if hiding was intended.

		private IOutBuffer _input;
		private BufferDataReader _reader;

		IOutBuffer IOutputProcessor.InputBuffer => this._input;
		private string _connectionString;
		private string _destinationTableName;
		private int _timeout;

		public SQLOutput(IOutBuffer input, int timeoutSeconds, string connectionString, string destinationTableName) : base(1)
		{
			this._input = input;
			this._reader = new BufferDataReader(input);

			//Can we connect?
			SqlConnection connection = new SqlConnection(connectionString);
			connection.Open();
			connection.Dispose();
			this._connectionString = connectionString;
			this._destinationTableName = destinationTableName;
			this._timeout = timeoutSeconds;
		}

		public bool IsCompleted() => this._input.IsCompleted();

		void IOutputProcessor.Process() => Process();
		public override void Process()
		{
			using (SqlBulkCopy bulkCopier = new SqlBulkCopy(this._connectionString))
			{
				bulkCopier.DestinationTableName = this._destinationTableName;
				bulkCopier.BulkCopyTimeout = this._timeout;
				bulkCopier.EnableStreaming = true;
				bulkCopier.WriteToServer(_reader);
			}
			this._isRunning = false;
		}
	}
}