using System;
using System.Collections.Generic;
using System.Linq;
using Fleck;
using JetBrains.Annotations;
using Newtonsoft.Json;

namespace Blip
{
    /// <summary>
    ///     Super lightweight RPC / PUBSUB server.
    /// </summary>
    public abstract class Blip
    {
        /// <summary>
        ///     Raised when Blip logs a warning message.
        /// </summary>
        public Action<Blip, string> LogWarning;

        /// <summary>
        ///     Table of handlers to RPC registered delegates.
        /// </summary>
        protected Dictionary<string, Delegate> RegisteredServices;

        /// <summary>
        ///     Create a new blip service.
        /// </summary>
        protected Blip()
        {
            RegisteredServices = new Dictionary<string, Delegate>();
        }

        /// <summary>
        ///     Location of this Blip service as a string. For example, a websocket woud be: ws://0.0.0.0:1234/hello
        /// </summary>
        public virtual string Location => throw new NotImplementedException();

        /// <summary>
        ///     Register a delegate as an RPC delegate with this server.
        /// </summary>
        /// <remarks>This will overwrite existing procedures with the same name without warning.</remarks>
        /// <param name="target">The name to register this service with.</param>
        /// <param name="function">The function, method, or lambda.</param>
        public virtual void Register(string target, Delegate function)
        {
            // Sanity check.
            if (string.IsNullOrWhiteSpace(target)) throw new ArgumentNullException(nameof(target));

            // Place in the registered table.
            RegisteredServices[target] = function ?? throw new ArgumentNullException(nameof(function));
        }

        /// <summary>
        ///     Unregister a delegate intended to handle a particular operation.
        /// </summary>
        /// <param name="target">The RPC target of the procedure to remove.</param>
        /// <returns>True if removed successfully, false if not.</returns>
        public bool Unregister(string target)
        {
            // Remove it from the RPC table if it exists.
            return RegisteredServices.Remove(target);
        }

        /// <summary>
        ///     Publish data to a topic. This will be sent to all connected subscribers.
        /// </summary>
        /// <param name="topic">The name of the topic to publish too.</param>
        /// <param name="arguments">The arguments to push to the topic.</param>
        public abstract void Publish(string topic, params object[] arguments);
    }

    /// <inheritdoc cref="Blip" />
    /// <summary>
    ///     Super lightweight RPC / PUBSUB server using Fleck as a WebSocket transport layer.
    /// </summary>
    /// <remarks>
    ///     The service parameter types float and Int32 are not supprted by the JSON conversion.
    ///     This is because there is not promise that these types can be converted from dynamic JSON.
    ///     RPC request messages are as follows:
    ///     {
    ///     Target    : "procedurename",    // Procedure target.
    ///     Arguments : [arg1, arg2],       // Arguments as a list of primitive JSON types.
    ///     Call      : "responseID",       // Response ID for callbacks - always called with success / failure.
    ///     TODO: //async   : true,               // Should this method be run in a separate thread?
    ///     }
    ///     RPC response messages are as follows:
    ///     {
    ///     Target   : "responseID",        // Passed in with the request.
    ///     Success  : true,                // True or False based on sucess or execption.
    ///     Result   : data,                // Result of RPC procedure converted to JSON.
    ///     // If success == false, this contains the error message.
    ///     }
    ///     RPC publish message are as follows:
    ///     {
    ///     Topic     : name,               // The name of the topic to publish too.
    ///     Arguments : [data],             // List of data items to be published.
    ///     }
    /// </remarks>
    public class BlipWebSocket : Blip, IDisposable
    {
        /// <summary>
        ///     Table of currently connected clients.
        /// </summary>
        private readonly List<IWebSocketConnection> _clients;

        /// <summary>
        ///     Types that are not permissable as dynamic type conversions from JSON.
        ///     These cannot be parameters in registered services.
        /// </summary>
        private readonly string[] _disallowedTypes =
        {
            typeof(int).FullName, typeof(float).FullName, typeof(float).FullName, typeof(byte).FullName,
            typeof(short).FullName
        };

        /// <summary>
        ///     Reference to the websocket server.
        /// </summary>
        private WebSocketServer _server;

        /// <inheritdoc />
        /// <summary>
        ///     Create a new BlipWebSocket.
        /// </summary>
        /// <param name="location">The location to create the service at. For example, a websocket woud be: ws://0.0.0.0:1234/hello</param>
        public BlipWebSocket(string location)
        {
            _clients = new List<IWebSocketConnection>();
            _server = new WebSocketServer(location);
            _server.Start(socket =>
            {
                socket.OnOpen = () =>
                {
                    lock (_clients)
                    {
                        _clients.Add(socket);
                    }
                };
                socket.OnClose = () =>
                {
                    lock (_clients)
                    {
                        _clients.Remove(socket);
                    }
                };
                socket.OnError = e =>
                {
                    lock (_clients)
                    {
                        _clients.Remove(socket);
                    }
                };
                socket.OnMessage = data => { HandleMessage(socket, data); };
            });
        }

        /// <inheritdoc />
        /// <summary>
        ///     Location of this Blip service as a string. For example, a websocket woud be: ws://0.0.0.0:1234/hello
        /// </summary>
        public override string Location => _server.Location;

        /// <summary>
        ///     Tear down this server and free memory.
        /// </summary>
        public void Dispose()
        {
            // Tear down server.
            _server?.Dispose();
            _server = null;

            // Clear clients.
            lock (_clients)
            {
                _clients.Clear();
            }
        }

        /// <inheritdoc />
        /// <summary>
        ///     Register a delegate as an RPC delegate with this server.
        /// </summary>
        /// <remarks>This will overwrite existing procedures with the same name without warning.</remarks>
        /// <param name="target">The name to register this service with.</param>
        /// <param name="function">The function, method, or lambda.</param>
        public override void Register(string target, Delegate function)
        {
            // Sanity.
            if (function == null) throw new ArgumentNullException(nameof(function));

            // Check method does not contain Int32 or float parameters.
            var args = function.Method.GetParameters();
            var errors = args.Count(p => _disallowedTypes.Contains(p.ParameterType.FullName));
            if (errors > 0)
                throw new Exception("BlipWebSocket cannot support parameters with type: " +
                                    string.Join(", ", _disallowedTypes.Select(t => t.ToString())));

            // Base.
            base.Register(target, function);
        }

        /// <summary>
        ///     Handle incoming data from the websocket.
        /// </summary>
        /// <remarks>Check it is valid, handle if RPC.</remarks>
        /// <param name="client">The client connection.</param>
        /// <param name="jsonMessage">The message payload as JSON.</param>
        private void HandleMessage(IWebSocketConnection client, string jsonMessage)
        {
            // Convert to BlipRequest.
            BlipRequest request;
            try
            {
                request = JsonConvert.DeserializeObject<BlipRequest>(jsonMessage);
                request.Validate();
            }
            catch (Exception)
            {
                LogWarning?.Invoke(this, "Dropped bad Blip request from " + client.ConnectionInfo.ClientIpAddress);
                return;
            }

            // Locate target delegate.
            if (!RegisteredServices.TryGetValue(request.Target, out var target))
            {
                LogWarning?.Invoke(this,
                    "Missing RPC registered handler for target from " + client.ConnectionInfo.ClientIpAddress);
                return;
            }

            // Dynamic invoke.
            string responseJson = null;
            try
            {
                var result = target.DynamicInvoke(request.Arguments);
                responseJson =
                    JsonConvert.SerializeObject(new BlipResponse
                    {
                        Target = request.Call,
                        Success = true,
                        Result = result
                    });
            }
            catch (Exception e)
            {
                var err = e.InnerException;
                if (err != null) e = err;
                responseJson = JsonConvert.SerializeObject(new BlipResponse
                {
                    Target = request.Call,
                    Success = false,
                    Result = new {e.Message, Stacktrace = e.StackTrace}
                });
            }

            // Pass it back.
            DispatchData(client, responseJson);
        }

        /// <inheritdoc />
        /// <summary>
        ///     Publish data to a topic. This will be sent to all connected subscribers.
        /// </summary>
        /// <param name="topic">The name of the topic to publish too.</param>
        /// <param name="arguments">The arguments to push to the topic.</param>
        public override void Publish(string topic, params object[] arguments)
        {
            // Prepare response.
            var topicJson = JsonConvert.SerializeObject(new BlipPublish {Topic = topic, Arguments = arguments});

            // For each client.
            lock (_clients)
            {
                foreach (var client in _clients)
                    DispatchData(client, topicJson);
            }
        }

        /// <summary>
        ///     Attempt to send data to a client.
        /// </summary>
        /// <param name="client">The client to send data too.</param>
        /// <param name="data">The data to send.</param>
        private void DispatchData(IWebSocketConnection client, string data)
        {
            try
            {
                client.Send(data);
            }
            catch (Exception)
            {
                LogWarning?.Invoke(this, "Error sending data to " + client.ConnectionInfo.ClientIpAddress);
            }
        }

        #region Message Packets

        /// <summary>Used to parse incoming message requests.</summary>
        [UsedImplicitly]
        private class BlipRequest
        {
            public object[] Arguments { get; set; }
            public string Call { get; set; }
            public string Target { get; set; }

            public void Validate()
            {
                if (string.IsNullOrWhiteSpace(Target))
                    throw new Exception("Missing or malformed RPC procedure target argument");
                if (string.IsNullOrWhiteSpace(Call))
                    throw new Exception("Missing or malformed RPC response handler id");
            }
        }

        /// <summary>Returned to callers after an RPC call.</summary>
        private class BlipResponse
        {
            public object Result { get; set; } // The result of the server request.
            public bool Success { get; set; } // Did the server request complete sucessfully.
            public string Target { get; set; } // The callback target on the client.
        }

        /// <summary>Sent to all clients as subscribers</summary>
        private class BlipPublish
        {
            public object[] Arguments { get; set; }
            public string Topic { get; set; }
        }

        #endregion
    }
}