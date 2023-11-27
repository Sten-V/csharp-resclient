using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ResgateIO.Client
{
    public class ResClient : IResClient
    {
        public event EventHandler<ResourceEventArgs> ResourceEvent;
        public event ErrorEventHandler Error;
        public event EventHandler<ConnectionStatusEventArgs> ConnectionStatusChanged;

        /// <summary>
        /// Supported RES protocol version.
        /// </summary>
        public const string ProtocolVersion = "1.2.1";

        private const string LegacyProtocol = "1.1.1";

        private static readonly int LegacyProtocolVersion = VersionToInt(LegacyProtocol);

        private readonly string _hostUrl;
        private readonly Func<Task<IWebSocket>> _webSocketFactory;
        private readonly object _connectLock = new object();

        private ResRpc _rpc;
        private JsonSerializerSettings _serializerSettings;
        private Func<ResClient, Task> _onConnectCallback;
        private int _reconnectDelay = 3000;
        private CancellationTokenSource _reconnectCancellationTokenSource;
        private Task _connectTask;
        private int _protocol;
        private bool _isOnline;
        private bool _tryReconnect;
        private bool _isDisposed;
        private ItemCache _cache;

        /// <summary>
        /// Supported RES protocol version by Resgate.
        /// </summary>
        public string ResgateProtocol { get; private set; }

        public bool IsConnected => _rpc != null;

        public ResClient(string hostUrl)
        {
            _hostUrl = hostUrl;
            _webSocketFactory = CreateWebSocket;
            CreateItemCache();
        }

        public ResClient(Func<Task<IWebSocket>> webSocketFactory)
        {
            _webSocketFactory = webSocketFactory;
            CreateItemCache();
        }

        private void CreateItemCache()
        {
            _cache = new ItemCache(this);
            _cache.Error += OnError;
            _cache.ResourceEvent += OnCacheResourceEvent;
        }

        /// <summary>
        /// Sets the settings used with JSON serialization.
        /// Must be called before connecting.
        /// </summary>
        /// <param name="settings">JSON serializer settings.</param>
        /// <returns>The ResClient instance.</returns>
        public ResClient SetSerializerSettings(JsonSerializerSettings settings)
        {
            _serializerSettings = settings;
            return this;
        }

        /// <summary>
        /// Sets the on connect callback used to authenticate the connection.
        /// Must be called before connecting.
        /// </summary>
        /// <param name="settings">JSON serializer settings.</param>
        /// <returns>The ResClient instance.</returns>
        public ResClient SetOnConnect(Func<ResClient, Task> callback)
        {
            _onConnectCallback = callback;
            return this;
        }

        /// <summary>
        /// Sets the reconnection delay.
        /// Must be called before connecting.
        /// </summary>
        /// <param name="milliseconds">Delay in milliseconds.</param>
        /// <returns>The ResClient instance.</returns>
        public ResClient SetReconnectDelay(int milliseconds)
        {
            _reconnectDelay = milliseconds;
            return this;
        }

        /// <summary>
        /// Sets the reconnection delay.
        /// Must be called before connecting.
        /// </summary>
        /// <param name="duration">Duration time span.</param>
        /// <returns>The ResClient instance.</returns>
        public ResClient SetReconnectDelay(TimeSpan duration)
        {
            _reconnectDelay = duration.Milliseconds;
            return this;
        }

        /// <summary>
        /// Registers a model factory for a specific resource pattern.
        /// The pattern may contain wildcards:
        /// * (asterisk) is a partial wildcard.
        ///     Eg. "directory.user.*.details"
        /// > (greater than) is a full wildcard.
        ///     Eg. "library.books.>"
        /// </summary>
        /// <param name="pattern">Resource name pattern.</param>
        /// <param name="factory">Model factory delegate.</param>
        public void RegisterModelFactory(string pattern, ModelFactory factory)
        {
            _cache.RegisterModelFactory(pattern, factory);
        }

        /// <summary>
        /// Registers a collection factory for a specific resource pattern.
        /// The pattern may contain wildcards:
        /// * (asterisk) is a partial wildcard.
        ///     Eg. "directory.user.*.details"
        /// > (greater than) is a full wildcard.
        ///     Eg. "library.books.>"
        /// </summary>
        /// <param name="pattern">Resource name pattern.</param>
        /// <param name="factory">Collection factory delegate.</param>
        public void RegisterCollectionFactory(string pattern, CollectionFactory factory)
        {
            _cache.RegisterCollectionFactory(pattern, factory);
        }

        public async Task ConnectAsync()
        {
            Task task;
            var calledConnect = false;
            lock (_connectLock)
            {
                _tryReconnect = true;

                CancelOngoingReconnect();

                if (_connectTask == null)
                {
                    calledConnect = true;
                    _connectTask = ConnectInternalAsync();
                }

                task = _connectTask;
            }

            try
            {
                await task.ConfigureAwait(false);
            }
            catch
            {
                if (calledConnect)
                {
                    _connectTask = null;
                }

                throw;
            }
        }

        private async Task ConnectInternalAsync()
        {
            var webSocket = await _webSocketFactory()
                .ConfigureAwait(false);

            webSocket.ConnectionStatusChanged += WebSocket_ConnectionStatusChanged;

            _rpc = new ResRpc(webSocket, _serializerSettings);
            _rpc.ResourceEvent += OnResourceEvent;
            _rpc.Error += OnError;

            try
            {
                await HandshakeAsync()
                    .ConfigureAwait(false);

                if (_onConnectCallback != null)
                {
                    await _onConnectCallback(this)
                        .ConfigureAwait(false);
                }

                _isOnline = true;

                SubscribeToAllStale();

                ConnectionStatusChanged?.Invoke(this, new ConnectionStatusEventArgs(ConnectionStatus.Connected));
            }
            catch
            {
                DisposeRpc();
                throw;
            }
        }

        private void WebSocket_ConnectionStatusChanged(object sender, ConnectionStatusEventArgs e)
        {
            if (e.ConnectionStatus == ConnectionStatus.DisconnectedGracefully ||
                e.ConnectionStatus == ConnectionStatus.DisconnectedWithError)
            {
                WebSocket_Disconnected();
            }

            ConnectionStatusChanged?.Invoke(this, e);
        }

        /// <summary>
        /// Disconnects the WebSocket and sets the client to offline mode.
        /// </summary>
        /// <returns>Task that completes once disconnected.</returns>
        public async Task DisconnectAsync()
        {
            lock (_connectLock)
            {
                _tryReconnect = false;
                CancelOngoingReconnect();
            }

            if (_rpc == null)
            {
                return;
            }

            try
            {
                await _rpc.DisconnectAsync()
                    .ConfigureAwait(false);
            }
            finally
            {
                DisposeRpc();
                lock (_connectLock)
                {
                    _isOnline = false;
                }
            }
        }

        private void WebSocket_Disconnected()
        {
            var hasStale = _cache.SetAllStale();
            DisposeRpc();

            lock (_connectLock)
            {
                var wasOnline = _isOnline;
                _isOnline = false;

                _tryReconnect = hasStale && _tryReconnect;
                if (_tryReconnect)
                {
                    if (wasOnline)
                    {
                        Task.Run(Reconnect);
                    }
                    else
                    {
                        StartReconnectTimer();
                    }
                }
            }
        }

        private void StartReconnectTimer()
        {
            if (!_tryReconnect)
            {
                return;
            }

            CancelOngoingReconnect();

            _reconnectCancellationTokenSource = new CancellationTokenSource();
            Task.Delay(
                    _reconnectDelay,
                    _reconnectCancellationTokenSource.Token)
                .ContinueWith(async _ => await Reconnect().ConfigureAwait(false));
        }

        private async Task Reconnect()
        {
            try
            {
                await ConnectAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                OnError(this, new ErrorEventArgs(ex));
                lock (_connectLock)
                {
                    StartReconnectTimer();
                }
            }
        }

        /// <summary>
        /// Get and subscribe to a resource from the API.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <returns>The resource.</returns>
        public Task<ResResource> SubscribeAsync(string rid)
        {
            var ci = _cache.Subscribe(rid, Subscribe);
            return ci.ResourceTask;
        }

        /// <summary>
        /// Unsubscribe to a previously subscribed resource, or a resource returned by a CallAsync or AuthAsync call.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        public async Task UnsubscribeAsync(string rid)
        {
            await _cache
                .Unsubscribe(rid, Unsubscribe)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sends a request to an API resource call method.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <param name="parameters">Method parameters.</param>
        /// <returns>The result.</returns>
        public Task<object> CallAsync(string rid, string method, object parameters)
        {
            return RequestAsync("call", rid, method, parameters);
        }

        /// <summary>
        /// Sends a request to an API resource call method.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <returns>The result.</returns>
        public Task<object> CallAsync(string rid, string method)
        {
            return CallAsync(rid, method, null);
        }

        /// <summary>
        /// Sends a request to an API resource call method and returns the result as a value of type T.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <param name="parameters">Method parameters.</param>
        /// <returns>The result.</returns>
        public Task<T> CallAsync<T>(string rid, string method, object parameters)
        {
            return RequestAsync<T>("call", rid, method, parameters);
        }

        /// <summary>
        /// Sends a request to an API resource call method and returns the result as a value of type T.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <returns>The result.</returns>
        public Task<T> CallAsync<T>(string rid, string method)
        {
            return CallAsync<T>(rid, method, null);
        }

        /// <summary>
        /// Sends a request to an API resource authentication method.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <param name="parameters">Method parameters.</param>
        /// <returns>The result.</returns>
        public Task<object> AuthAsync(string rid, string method, object parameters)
        {
            return RequestAsync("auth", rid, method, parameters);
        }

        /// <summary>
        /// Sends a request to an API resource authentication method.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <returns>The result.</returns>
        public Task<object> AuthAsync(string rid, string method)
        {
            return AuthAsync(rid, method, null);
        }

        /// <summary>
        /// Sends a request to an API resource authentication method and returns the result as a value of type T.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <param name="parameters">Method parameters.</param>
        /// <returns>The result.</returns>
        public Task<T> AuthAsync<T>(string rid, string method, object parameters)
        {
            return RequestAsync<T>("auth", rid, method, parameters);
        }

        /// <summary>
        /// Sends a request to an API resource authentication method and returns the result as a value of type T.
        /// </summary>
        /// <param name="rid">Resource ID.</param>
        /// <param name="method">Method.</param>
        /// <returns>The result.</returns>
        public Task<T> AuthAsync<T>(string rid, string method)
        {
            return AuthAsync<T>(rid, method, null);
        }

        private async Task<object> RequestAsync(string type, string rid, string method, object parameters)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            Send(type, rid, method, parameters, (result, err) =>
            {
                if (err != null)
                {
                    tcs.SetException(new ResException(err));
                }
                else
                {
                    tcs.SetResult(HandleRequestResult(result));
                }
            });

            return await tcs.Task
                .ConfigureAwait(false);
        }

        private object HandleRequestResult(RequestResult result)
        {
            if (_protocol <= LegacyProtocolVersion)
            {
                return result.Result;
            }

            if (!(result.Result is JObject r))
            {
                return null;
            }

            // Check if the result is a resource response
            if (r.TryGetValue("rid", out var rid))
            {
                var resourceId = (string)rid;

                var ci = _cache.AddResourcesAndSubscribe(result.Result, resourceId);
                return ci.Resource;
            }

            return r["payload"];
        }

        private async Task<T> RequestAsync<T>(string type, string rid, string method, object parameters)
        {
            var o = await RequestAsync(type, rid, method, parameters)
                .ConfigureAwait(false);

            if (o is T)
            {
                return (T)o;
            }

            if (o is JToken)
            {
                var token = (JToken)o;
                return token.ToObject<T>();
            }

            // Try to type cast it as a last measure
            return (T)Convert.ChangeType(o, typeof(T));
        }

        private void Subscribe(CacheItem ci, ResponseCallback callback)
        {
            Send("subscribe", ci.ResourceID, null, null, callback);
        }

        private async Task HandshakeAsync()
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _rpc.Request("version", new VersionRequestDto(ProtocolVersion), (result, err) =>
            {
                if (err != null && err.Code != ResError.CodeInvalidRequest)
                {
                    tcs.SetException(new ResException(err));
                    return;
                }

                try
                {
                    _protocol = 0;
                    if (result.Result != null)
                    {
                        var versionResponse = result.Result.ToObject<VersionResponseDto>();
                        _protocol = VersionToInt(versionResponse.Protocol);
                        ResgateProtocol = versionResponse.Protocol;
                    }

                    // Set legacy protocol.
                    if (_protocol == 0)
                    {
                        _protocol = VersionToInt(LegacyProtocol);
                        ResgateProtocol = LegacyProtocol;
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(new ResException(ex.Message, ex));
                    return;
                }

                tcs.SetResult(null);
            });

            await tcs.Task
                .ConfigureAwait(false);
        }

        private void Unsubscribe(string rid, ResponseCallback callback)
        {
            Send("unsubscribe", rid, null, null, callback);
        }

        private void Send(string action, string rid, string method, object parameters, ResponseCallback callback)
        {
            Task.Run(async () =>
            {
                string m = String.IsNullOrEmpty(method)
                    ? action + "." + rid
                    : action + "." + rid + "." + method;

                try
                {
                    await ConnectAsync()
                        .ConfigureAwait(false);
                }
                catch (ResException e)
                {
                    callback(null, e.Error);
                    return;
                }
                catch (Exception e)
                {
                    callback(null, new ResError(e.ToString()));
                    return;
                }

                _rpc.Request(m, parameters, callback);
            });
        }

        private void SubscribeToAllStale()
        {
            _cache.SubscribeStale(Subscribe);
        }

        private void DisposeRpc()
        {
            _rpc?.Dispose();
            _rpc = null;
            _connectTask = null;
        }

        private async Task<IWebSocket> CreateWebSocket()
        {
            var webSocket = new WebSocket();
            await webSocket.ConnectAsync(_hostUrl)
                .ConfigureAwait(false);
            return webSocket;
        }

        private static int VersionToInt(string version)
        {
            if (string.IsNullOrEmpty(version))
            {
                return 0;
            }

            var v = 0;
            try
            {
                var parts = version.Split('.');
                foreach (var part in parts)
                {
                    v = v * 1000 + int.Parse(part);
                }
            }
            catch (Exception)
            {
                return 0;
            }
            return v;
        }

        private void OnResourceEvent(object sender, ResourceEventArgs ev)
        {
            try
            {
                _cache.HandleEvent(ev);
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, new ErrorEventArgs(ex));
            }
        }

        private void OnError(object sender, ErrorEventArgs ev)
        {
            Error?.Invoke(this, ev);
        }

        private void OnCacheResourceEvent(object sender, ResourceEventArgs ev)
        {
            ResourceEvent?.Invoke(this, ev);
        }

        private void CancelOngoingReconnect()
        {
            _reconnectCancellationTokenSource?.Cancel();
            _reconnectCancellationTokenSource?.Dispose();
            _reconnectCancellationTokenSource = null;
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            DisposeRpc();

            ResourceEvent = null;
            Error = null;
            ConnectionStatusChanged = null;

            _isDisposed = true;
        }
    }
}
