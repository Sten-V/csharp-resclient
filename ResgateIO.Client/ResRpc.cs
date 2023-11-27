using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ResgateIO.Client
{
    internal delegate void ResponseCallback(RequestResult result, ResError err);

    internal class ResRpc : IDisposable
    {
        public event EventHandler<ResourceEventArgs> ResourceEvent;
        public event ErrorEventHandler Error;

        private readonly object _requestLock = new object();
        private readonly JsonSerializerSettings _serializerSettings;

        private IWebSocket _webSocket;
        private int _requestId = 1;
        private Dictionary<int, RpcRequest> _requests = new Dictionary<int, RpcRequest>();
        private bool _isDisposed;

        public ResRpc(IWebSocket webSocket, JsonSerializerSettings serializerSettings)
        {
            _serializerSettings = serializerSettings;
            _webSocket = webSocket;
            _webSocket.MessageReceived += WebSocket_MessageReceived;
        }

        private void WebSocket_MessageReceived(object sender, MessageEventArgs e)
        {
            string message = null;
            MessageDto rpcMessage = null;

            try
            {
                message = Encoding.UTF8.GetString(e.Message);
                rpcMessage = JsonConvert.DeserializeObject<MessageDto>(message);
            }
            catch (Exception ex)
            {
                Error?.Invoke(
                    this,
                    new ErrorEventArgs(
                        new InvalidMessageException(
                            e.Message, "Error deserializing incoming message.", ex)));
            }

            if (rpcMessage?.Id != null)
            {
                HandleResponse(rpcMessage);
            }
            else if (rpcMessage?.Event != null)
            {
                HandleEvent(rpcMessage);
            }
            else
            {
                throw new InvalidOperationException($"Invalid message from server: {message}");
            }
        }

        private void HandleResponse(MessageDto message)
        {
            try
            {
                var req = ConsumeRequest(message.Id ?? default);

                if (message.Error != null)
                {
                    req.Callback(null, message.Error);
                }
                else
                {
                    req.Callback(new RequestResult
                    {
                        Result = message.Result
                    }, null);
                }
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, new ErrorEventArgs(ex));
            }
        }

        private void HandleEvent(MessageDto message)
        {
            try
            {
                // Event
                var idx = message.Event.LastIndexOf('.');
                if (idx < 0 || idx == message.Event.Length - 1)
                {
                    throw new InvalidOperationException($"Malformed event name: {message.Event}");
                }
                var rid = message.Event.Substring(0, idx);

                ResourceEvent?.Invoke(this, new ResourceEventArgs
                {
                    ResourceID = rid,
                    EventName = message.Event.Substring(idx + 1),
                    Data = message.Data,
                });
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, new ErrorEventArgs(ex));
            }
        }

        public void Request(string method, object parameters, ResponseCallback callback)
        {
            RpcRequest req;
            lock (_requestLock)
            {
                if (_requests == null)
                {
                    Task.Run(() => callback(null, new ResError(ResError.CodeConnectionError, "Connection closed")));
                    return;
                }

                req = new RpcRequest
                {
                    Id = _requestId++,
                    Method = method,
                    Params = parameters,
                    Callback = callback,
                };
                _requests.Add(req.Id, req);
            }

            var dta = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(req, _serializerSettings));

            Task.Run(async () =>
            {
                try
                {
                    await _webSocket.SendAsync(dta)
                        .ConfigureAwait(false);
                }
                catch (ResException e)
                {
                    ConsumeRequest(req.Id);
                    callback(null, e.Error);
                }
                catch (Exception e)
                {
                    ConsumeRequest(req.Id);
                    callback(null, new ResError(e.Message));
                }
            });
        }

        private RpcRequest ConsumeRequest(int id)
        {
            lock (_requestLock)
            {
                if (_requests == null)
                {
                    throw new InvalidOperationException($"Incoming request disposed: {id}");
                }

                if (_requests.TryGetValue(id, out var req))
                {
                    _requests.Remove(id);
                    return req;
                }
            }

            throw new InvalidOperationException($"Invalid incoming request ID: {id}");
        }

        public Task DisconnectAsync()
        {
            return _webSocket.DisconnectAsync();
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            AbortPendingRequests();

            _webSocket?.Dispose();
            _webSocket = null;

            ResourceEvent = null;
            Error = null;

            _isDisposed = true;
        }

        private void AbortPendingRequests()
        {
            if (_requests == null)
                return;

            Dictionary<int, RpcRequest> pendingRequests;
            lock (_requestLock)
            {
                pendingRequests = _requests;
                _requests = null;
            }

            foreach (var req in pendingRequests)
            {
                Task.Run(() => req.Value.Callback(
                    null,
                    new ResError(ResError.CodeConnectionError, "Connection closed")));
            }
        }
    }
}
