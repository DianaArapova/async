using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
	public class PriorityClient : ClusterClientBase
	{
		public PriorityClient(string[] replicaAddresses) : base(replicaAddresses)
		{
		}

		public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
		{
			var replica = ReplicaAddresses.ToList();
			var priorityReplica = replica
				.Select((t, i) => new Tuple<int, string>(Priority[i], t))
				.ToList();
			priorityReplica.Sort();
			var timeoutOneTask = new TimeSpan(timeout.Ticks / ReplicaAddresses.Length);

			for (var i = 0; i < priorityReplica.Count; i++)
			{
				var priorityReplicaAddress = priorityReplica[i];
				var replicaAddress = priorityReplicaAddress.Item2;
				var webRequest = CreateRequest(replicaAddress + "?query=" + query);
				Log.InfoFormat("Processing {0}", webRequest.RequestUri);

				var resultTask = ProcessRequestAsync(webRequest);
				await Task.WhenAny(resultTask, Task.Delay(timeoutOneTask));
				if (!resultTask.IsCompleted || resultTask.IsFaulted)
				{
					Priority[i] += 1;
					continue;
				}

				Priority[i] -= 1;
				return resultTask.Result;
			}

			throw new TimeoutException();
		}

		protected override ILog Log => LogManager.GetLogger(typeof(PriorityClient));
	}
}