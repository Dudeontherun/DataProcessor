using DataProcessor.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Xml.Serialization;

namespace DataProcessor.Base
{
	[Serializable]
	public abstract class BaseProcessor : IProcessor, IDisposable
	{
		public Guid Id;

		[XmlIgnore]
		Guid IProcessor.Id { get => this.Id; set => this.Id = value; }

		[XmlIgnore]
		protected volatile bool _isRunning = false;

		private int _numThreads;
		
		[XmlIgnore]
		private readonly List<Thread> _threadList = new List<Thread>();

		[XmlIgnore]
		private object _modifyProcessorLock = new object();

		public BaseProcessor(int numThreads)
		{
			this._numThreads = numThreads;
		}

		public int GetNumOfThreads() => this._numThreads;

		public void SetThreads(int numOfThreads)
		{
			lock (this._modifyProcessorLock)
			{
				this._numThreads = numOfThreads;
				if (this._isRunning == true) { ChangeThreadsWhileRunning(); }
			}
		}

		private void ChangeThreadsWhileRunning()
		{
			lock (this._modifyProcessorLock)
			{
				if (this._isRunning == false) { return; }

				//Update list.
				var finishedThreads = this._threadList.Where(i => !i.IsAlive).ToList();
				finishedThreads.ForEach(i => this._threadList.Remove(i));

				if (this._threadList.Count > _numThreads)
				{
					int threadsToRemove = this._threadList.Count - _numThreads;
					while (threadsToRemove-- > 0)
					{
						var thread = this._threadList[0];
						if (thread.IsAlive) { thread.Abort(); }

						threadsToRemove--;
					}
				}
				else
				{
					int threadsToAdd = _numThreads - this._threadList.Count;
					while (threadsToAdd-- > 0)
					{
						var thread = new Thread(new ThreadStart(ProcessInternal));
						thread.Start();
						this._threadList.Add(thread);
					}
				}
			}
		}

		public abstract void Process();

		protected virtual void ProcessInternal()
		{
			while (this._isRunning)
			{
				Process();
			}
		}

		public void Start()
		{
			if (this._isRunning) { return; }

			this._isRunning = true;
			this.ChangeThreadsWhileRunning();
		}

		public void Stop(bool forceStop = false)
		{
			if (this._isRunning == false) { return; }

			lock (this._modifyProcessorLock)
			{
				this._isRunning = false;
				if (forceStop)
				{
					foreach (Thread thread in this._threadList)
					{
						if (thread.IsAlive) { thread.Abort(); }
					}
					this._threadList.Clear();
				}
			}
		}

		[XmlIgnore]
		private bool _isDisposed = false;
		public void Dispose()
		{
			if (!this._isDisposed)
			{
				foreach (Thread thread in this._threadList)
				{
					if (thread.IsAlive) { thread.Abort(); }
				}

				this._threadList.Clear();
				this._isDisposed = true;
			}
		}
	}
}
