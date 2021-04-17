using DataProcessor.Buffer;
using System.Collections.Generic;

namespace DataProcessor.Interfaces
{
	public interface IAggregateManager : IProcessor, IOutBuffer
	{
		protected IOutBuffer InputBuffer { get; }

		public void AddAggregate(IAggregate aggregate);
		public void AddAggregate(params IAggregate[] aggregates);
		public void AddAggregate(int position, IAggregate aggregate);
		public bool RemoveAggregate(IAggregate aggregate);
		public void RemoveAggregate(int position);
		public List<IAggregate> GetAggregates();
	}
}