using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using System.Text;

namespace SendTemperature
{
    public class SensorData
    {
        public string sensorid { get; set; }
        public int floor { get; set; }
        public string bayid { get; set; }
        public string timestamp { get; set; }
        public double temperatureincelsius { get; set; }
        public double humidityinpercentage { get; set; }
        public bool manualtrigger { get; set; }
        public int building { get; set; }
        public string campus { get; set; }
    }
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class SendTemperature : StatelessService
    {
        static List<string> campusList = new List<string> { "Redmond,Washington,98052", "San Fransisco, California,94104", "Bellevue, Washington, 98004", "Hyderabad, India, 500032" , "Bengaluru, India, 560071", "Paris, France, 92130", "Munich, Germany, 85716", "Beijing, China,100102", "Istanbul, Turkey, 34340", "Auckland, New Zealand, 000" };
        static List<string> bayList = new List<string> {"North","South","East","West","Northeast","Northwest","Southeast","Southwest","Northend","Westend","Southend","Eastend" };

        public SendTemperature(StatelessServiceContext context)
            : base(context)
        {
        }

                /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.
            RegistryManager registryManager;
            string connectionString = "HostName=tempdataingestion.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=A/YpaynaRDPQn6GcUp+RVQvv6jaobWfnyvQFi4Io4SM=";
            //long iterations = 0;
            DeviceClient deviceClient;
            string iotHubURI = "tempdataingestion.azure-devices.net";
            int counter = 0;
            int comparer = 10;
            bool sentMin = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Random r = new Random();
                int buildingid = r.Next(50);
                int campusid = r.Next(10);
                string deviceID = "gateway_" + "C" + campusid + "B" + buildingid; 
                Device device;
                registryManager = RegistryManager.CreateFromConnectionString(connectionString);
                try
                {
                    device = await registryManager.AddDeviceAsync(new Device(deviceID));
                }
                catch (DeviceAlreadyExistsException e)
                {
                    device = await registryManager.GetDeviceAsync(deviceID);
                }
                string deviceKey = device.Authentication.SymmetricKey.PrimaryKey;
                deviceClient = DeviceClient.Create(iotHubURI, new DeviceAuthenticationWithRegistrySymmetricKey(deviceID, deviceKey));


                // Sending simulated sensor data 
                List<SensorData> data = new List<SensorData>();
                Random rnd = new Random();
                Random boolr = new Random();
                var iLimit = rnd.Next(120,150);
                for (int i = 0; i <= iLimit; i++)
                {
                    SensorData d = new SensorData();
                    var bayidgen = rnd.Next(10);
                    d.bayid = bayList[bayidgen];
                    d.floor = rnd.Next(1,15);
                    d.campus = campusList[campusid];
                    d.building = buildingid;
                    d.sensorid = "b" + d.bayid + "f" + d.floor + "b" + d.building + "c" + d.campus;
                   // if (boolr.Next(0, 1) == 1 && (d.floor%2)==0 && (campusid %2)==0)
                   // {
                     
                   // }
                   // else
                   // {
                        d.manualtrigger = false;
                   // }
                    if (counter == comparer)
                    {
                        if (sentMin)
                        {
                            d.temperatureincelsius = 28 * rnd.NextDouble();
                            d.manualtrigger = true;
                            sentMin = false;
                        }
                        else
                        {
                            d.temperatureincelsius = 18 * rnd.NextDouble();
                            d.manualtrigger = true;

                            sentMin = true;
                        }
                        
                        comparer = comparer + 10;
                    }
                    else
                    {
                        d.temperatureincelsius = Math.Round(Math.Abs(rnd.NextDouble() + 25),2);
                    }
                    counter++;
                    if (counter > 1000)
                    {
                        // reset counter
                        counter=0;
                        comparer = 10;
                    }
                    var rndHumidity = rnd.NextDouble();
                    if (rndHumidity <= 0.10)
                    {
                        d.humidityinpercentage = rndHumidity + 0.47;
                    }
                    if(rndHumidity > 0.10 && rndHumidity <= 0.20)
                    {
                        d.humidityinpercentage = rndHumidity + 0.37;
                    }
                    if (rndHumidity > 0.20 && rndHumidity <= 0.30)
                    {
                        d.humidityinpercentage = rndHumidity + 0.27;
                    }
                    if (rndHumidity > 0.30 && rndHumidity <= 0.40)
                    {
                        d.humidityinpercentage = rndHumidity + 0.17;
                    }
                    if (rndHumidity > 0.40)
                    {
                        d.humidityinpercentage = rndHumidity;
                    }

                    d.timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    data.Add(d);   
                }
                
                var dataAsString = JsonConvert.SerializeObject(data);
                var msg = new Microsoft.Azure.Devices.Client.Message(Encoding.ASCII.GetBytes("{\"reading\":" + dataAsString + "}"));
                await deviceClient.SendEventAsync(msg);

                ServiceEventSource.Current.ServiceMessage(this, "Sending-{0}", msg);


                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }
}
