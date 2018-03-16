using System;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
	public class RoundRobinClient : ClusterClientBase
	{
		[ThreadStatic]
		private static Random rand = new Random();

		public RoundRobinClient(string[] replicaAddresses) : base(replicaAddresses)
		{
		}

		public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
		{
			var replica = ReplicaAddresses.ToArray();
			replica.Shuffle(rand);
			foreach (var s in replica)
			{
				Console.Write(s + " ");
			}
			Console.WriteLine();
			var timeoutOneTask = new TimeSpan(timeout.Ticks / ReplicaAddresses.Length);

			foreach (var replicaAddress in replica)
			{
				var webRequest = CreateRequest(replicaAddress + "?query=" + query);
				Log.InfoFormat("Processing {0}", webRequest.RequestUri);

				var resultTask = ProcessRequestAsync(webRequest);
				await Task.WhenAny(resultTask, Task.Delay(timeoutOneTask));
				if (!resultTask.IsCompleted)
					continue;
				if (resultTask.IsFaulted)
					continue;
				
				return resultTask.Result;
			}

			throw new TimeoutException();
		}

		protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClient));
	}

	public static class ArrayExtensions
	{
		public static void Shuffle<T>(this T[] array, Random rand)
		{
			for (var i = 0; i < array.Length / 2; i++)
			{
				var indexFirst = rand.Next(array.Length);
				var reminder = array[i];
				array[i] = array[indexFirst];
				array[indexFirst] = reminder;
			}
		}
	}
}