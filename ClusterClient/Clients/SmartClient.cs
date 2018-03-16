using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
	public class SmartClusterClient : ClusterClientBase
	{
		private readonly Random rand = new Random();
		public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
		{
		}

		public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
		{
			ReplicaAddresses.Shuffle(rand);
			var timeoutOneTask = new TimeSpan(timeout.Ticks / ReplicaAddresses.Length);
			var timerForAllTasks = Task.Delay(timeout);

			var resultTasks = new List<Task> {timerForAllTasks};

			foreach (var uri in ReplicaAddresses)
			{
				var timer = Task.Delay(timeoutOneTask);
				var webRequest = CreateRequest(uri + "?query=" + query);
				Log.InfoFormat("Processing {0}", webRequest.RequestUri);
				resultTasks.Add(ProcessRequestAsync(webRequest));
				var taskWithTimer = resultTasks.Concat(new[] {timer}).ToList();
				var completed = await Task.WhenAny(taskWithTimer);

				if (timerForAllTasks.IsCompleted)
					throw new TimeoutException();
				if (timer.IsCompleted)
					continue;
				if (!completed.IsFaulted)
					return ((Task<string>)completed).Result;

				resultTasks.Remove(completed);
			}

			throw new TimeoutException();
		}

		protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
	}
}