using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
	public class SimpleClient : ClusterClientBase
	{
		public SimpleClient(string[] replicaAddresses) : base(replicaAddresses)
		{
		}

		public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
		{
			var requests = new List<Task>();
			foreach (var replicaAddress in ReplicaAddresses)
			{
				var webRequest = CreateRequest(replicaAddress + "?query=" + query);
				Log.InfoFormat("Processing {0}", webRequest.RequestUri);
				requests.Add(ProcessRequestAsync(webRequest));
			}
			var timer = Task.Delay(timeout);
			requests.Add(timer);

			while (true)
			{
				var resultTask = await Task.WhenAny(requests);
				if (timer.IsCompleted)
					throw new TimeoutException();
				if (resultTask.IsFaulted)
				{
					requests.Remove(resultTask);
					continue;
				}
				return ((Task<string>)resultTask).Result;
			}
		}

		protected override ILog Log => LogManager.GetLogger(typeof(SimpleClient));
	}
}