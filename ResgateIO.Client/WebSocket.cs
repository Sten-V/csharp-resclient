using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace ResgateIO.Client
{
    public class WebSocket : IWebSocket
    {
        public event EventHandler<MessageEventArgs> MessageReceived;
        public event EventHandler<ConnectionStatusEventArgs> ConnectionStatusChanged;

        private const int ReceiveBufferSize = 8192;

        private ClientWebSocket _webSocket;
        private CancellationTokenSource _cancellationTokenSource;

        public async Task ConnectAsync(string url)
        {
            if (_webSocket != null)
            {
                if (_webSocket.State == WebSocketState.Open)
                {
                    return;
                }

                _webSocket.Dispose();
            }

            _webSocket = new ClientWebSocket();
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = new CancellationTokenSource();

            try
            {
                await _webSocket.ConnectAsync(
                        new Uri(url),
                        _cancellationTokenSource.Token)
                    .ConfigureAwait(false);
            }
            catch (WebSocketException ex)
            {
                throw new ResException(ResError.CodeConnectionError, ex.Message, ex);
            }

            await Task.Factory.StartNew(ReceiveLoop,
                    _cancellationTokenSource.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)
                .ConfigureAwait(false);

            ConnectionStatusChanged?.Invoke(
                this,
                new ConnectionStatusEventArgs(ConnectionStatus.Connected));
        }

        public async Task DisconnectAsync()
        {
            if (_webSocket?.State != WebSocketState.Open)
                return;

            _cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(2));

            await _webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Closing connection",
                    CancellationToken.None)
                .ConfigureAwait(false);

            ConnectionStatusChanged?.Invoke(
                this,
                new ConnectionStatusEventArgs(ConnectionStatus.DisconnectedGracefully));
        }

        public async Task SendAsync(byte[] data)
        {
            if (_webSocket == null || _webSocket.State != WebSocketState.Open)
            {
                throw new InvalidOperationException("WebSocket not connected.");
            }

            try
            {
                await _webSocket.SendAsync(
                        new ArraySegment<byte>(data),
                        WebSocketMessageType.Binary,
                        true,
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (WebSocketException ex)
            {
                throw new ResException(ResError.CodeConnectionError, ex.Message, ex);
            }
        }

        private async Task ReceiveLoop()
        {
            var buffer = new byte[ReceiveBufferSize];
            try
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    using (var inputStream = new MemoryStream(ReceiveBufferSize))
                    {
                        WebSocketReceiveResult receiveResult;
                        do
                        {
                            receiveResult = await _webSocket.ReceiveAsync(
                                    new ArraySegment<byte>(buffer),
                                    _cancellationTokenSource.Token)
                                .ConfigureAwait(false);

                            if (receiveResult.MessageType != WebSocketMessageType.Close)
                                inputStream.Write(buffer, 0, receiveResult.Count);

                        } while (!receiveResult.EndOfMessage);

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                            break;

                        OnMessageReceived(inputStream.ToArray());
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // Ignored
            }
            catch
            {
                ConnectionStatusChanged?.Invoke(
                    this,
                    new ConnectionStatusEventArgs(ConnectionStatus.DisconnectedWithError));

                throw;
            }
        }

        private void OnMessageReceived(byte[] msg)
        {
            MessageReceived?.Invoke(this, new MessageEventArgs(msg));
        }

        public void Dispose()
        {
            DisconnectAsync().GetAwaiter().GetResult();

            _webSocket?.Dispose();
            _webSocket = null;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;

            MessageReceived = null;
            ConnectionStatusChanged = null;
        }
    }
}
