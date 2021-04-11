using System.Collections.Generic;

namespace DataProcessor.Interfaces
{
	public interface IAggregateManager : IProcessor
	{
		public IInputProcessor InputProcessor { get; }

		public bool IsFinished();
		public List<IDataRow> GetBufferItems();
		public IDataRow Read();
		public void AddAggregate(IAggregate aggregate);
		public void AddAggregate(int position, IAggregate aggregate);
		public bool RemoveAggregate(IAggregate aggregate);
		public void RemoveAggregate(int position);
		public List<IAggregate> GetAggregates();
	}
}