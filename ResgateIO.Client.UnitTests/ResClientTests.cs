using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ResgateIO.Client.UnitTests
{
    public class ResClientTests : TestsBase
    {
        public ResClientTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void ResClient_CreatesInstance()
        {
            var client = new ResClient("ws://127.0.0.1:8080");
            Assert.NotNull(client);
        }

        [Fact(Skip = "requires resgate listening on port 8080")]
        public async Task ConnectAsync_ConnectsToResgate()
        {
            var client = new ResClient("ws://127.0.0.1:8080");
            await client.ConnectAsync();

            Assert.Equal("1.2.2", client.ResgateProtocol);
        }

        [Fact]
        public async Task ConnectAsync_ConnectsToMockWebSocket()
        {
            var ws = new MockWebSocket(Output);
            var resgate = new MockResgate(ws);
            var client = new ResClient(() => Task.FromResult<IWebSocket>(ws));

            var connectTask = client.ConnectAsync();
            await resgate.HandshakeAsync("1.2.2");
            await connectTask;

            Assert.Equal("1.2.2", client.ResgateProtocol);
        }

        [Fact]
        public async Task GetAsync_WithModelResponse_GetsModel()
        {
            await ConnectAndHandshake();

            var creqTask = Client.GetAsync("test.model");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("subscribe.test.model");
            req.SendResult(new JObject
            {
                { "models", new JObject
                    {
                        { "test.model", new JObject { { "foo", "bar" } } }
                    }
                }
            });
            var result = await creqTask;
            Assert.Equal("test.model", result.ResourceID);
            Assert.IsType<ResModel>(result);

            var model = result as ResModel;
            Test.AssertEqualJSON(new JObject { { "foo", "bar" } }, model);
        }

        [Fact]
        public async Task GetAsync_WithCollectionResponse_GetsCollection()
        {
            await ConnectAndHandshake();

            var creqTask = Client.GetAsync("test.collection");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("subscribe.test.collection");
            req.SendResult(new JObject
            {
                { "collections", new JObject
                    {
                        { "test.collection", new JArray { "foo", "bar" } }
                    }
                }
            });
            var result = await creqTask;
            Assert.Equal("test.collection", result.ResourceID);
            Assert.IsType<ResCollection>(result);

            var collection = result as ResCollection;
            Assert.Equal(new List<object> { "foo", "bar" }, collection);
        }

        [Fact]
        public async Task GetAsync_CalledTwiceOnModel_GetsModelFromCache()
        {
            await ConnectAndHandshake();

            var creqTask = Client.GetAsync("test.model");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("subscribe.test.model");
            req.SendResult(new JObject { { "models", new JObject {
                { "test.model", Test.Model }
            } } });
            var model1 = await creqTask as ResModel;
            Test.AssertEqualJSON(Test.Model, model1);

            var model2 = await Client.GetAsync("test.model") as ResModel;
            Assert.Same(model1, model2);
        }

        [Fact]
        public async Task GetAsync_CalledTwiceOnCollection_GetsCollectionFromCache()
        {
            await ConnectAndHandshake();

            var creqTask = Client.GetAsync("test.collection");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("subscribe.test.collection");
            req.SendResult(new JObject { { "collections", new JObject {
                { "test.collection", Test.Collection }
            } } });
            var collection1 = await creqTask as ResCollection;
            Test.AssertEqualJSON(Test.Collection, collection1);

            var collection2 = await Client.GetAsync("test.collection") as ResCollection;
            Assert.Same(collection1, collection2);
        }

        [Fact]
        public async Task GetAsync_WithParentModel_GetsChildModel()
        {
            await ConnectAndHandshake();

            var creqTask = Client.GetAsync("test.model.parent");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("subscribe.test.model.parent");
            req.SendResult(new JObject { { "models", new JObject {
                { "test.model.parent", Test.Resources["test.model.parent"] },
                { "test.model", Test.Resources["test.model"] },
            } } });
            var parent = await creqTask as ResModel;
            Assert.Equal("test.model.parent", parent.ResourceID);
            Test.AssertEqualJSON(new JObject { { "ref", Test.Model } }, parent);
        }

        [Theory]
        [InlineData("\"foo\"", "foo")]
        [InlineData("null", null)]
        [InlineData("42", 42)]
        [InlineData("true", true)]
        [InlineData("[\"foo\",null,42,true]", new object[] { "foo", null, 42, true })]
        public async Task CallAsync_AnonymousResult_GetsResult(string payload, object expected)
        {
            await ConnectAndHandshake();

            var creqTask = Client.CallAsync("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("call.test.model.method");
            req.SendResult(new JObject { { "payload", JToken.Parse(payload) } });
            var result = await creqTask;
            Test.AssertEqualJSON(expected == null ? JValue.CreateNull() : JToken.FromObject(expected), result);
        }

        [Fact]
        public async Task CallAsync_ObjectResult_GetsResult()
        {
            await ConnectAndHandshake();

            var creqTask = Client.CallAsync<Test.Payload>("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("call.test.model.method");
            req.SendResult(new JObject { { "payload", new JObject { { "foo", "bar" } } } });
            var payload = await creqTask;
            Test.AssertEqualJSON("bar", payload.Foo);
        }

        [Fact]
        public async Task CallAsync_PrimitiveResult_GetsResult()
        {
            await ConnectAndHandshake();

            var creqTask = Client.CallAsync<string>("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("call.test.model.method");
            req.SendResult(new JObject { { "payload", "foo" } });
            var result = await creqTask;
            Test.AssertEqualJSON("foo", result);
        }

        [Fact]
        public async Task CallAsync_ResourceResponse_GetsResource()
        {
            await ConnectAndHandshake();

            var creqTask = Client.CallAsync("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("call.test.model.method");
            req.SendResult(new JObject { { "rid", "test.ref" }, { "models", new JObject { { "test.ref", Test.Model } } } });
            var model = await creqTask;
            Assert.IsType<ResModel>(model);
            Test.AssertEqualJSON(Test.Model, model);
        }

        [Theory]
        [InlineData("\"foo\"", "foo")]
        [InlineData("null", null)]
        [InlineData("42", 42)]
        [InlineData("true", true)]
        [InlineData("[\"foo\",null,42,true]", new object[] { "foo", null, 42, true })]
        public async Task AuthAsync_AnonymousResult_GetsResult(string payload, object expected)
        {
            await ConnectAndHandshake();

            var creqTask = Client.AuthAsync("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("auth.test.model.method");
            req.SendResult(new JObject { { "payload", JToken.Parse(payload) } });
            var result = await creqTask;
            Test.AssertEqualJSON(expected == null ? JValue.CreateNull() : JToken.FromObject(expected), result);
        }

        [Fact]
        public async Task AuthAsync_ObjectResult_GetsResult()
        {
            await ConnectAndHandshake();

            var creqTask = Client.AuthAsync<Test.Payload>("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("auth.test.model.method");
            req.SendResult(new JObject { { "payload", new JObject { { "foo", "bar" } } } });
            var payload = await creqTask;
            Test.AssertEqualJSON("bar", payload.Foo);
        }

        [Fact]
        public async Task AuthAsync_PrimitiveResult_GetsResult()
        {
            await ConnectAndHandshake();

            var creqTask = Client.AuthAsync<string>("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("auth.test.model.method");
            req.SendResult(new JObject { { "payload", "foo" } });
            var result = await creqTask;
            Test.AssertEqualJSON("foo", result);
        }

        [Fact]
        public async Task AuthAsync_ResourceResponse_GetsResource()
        {
            await ConnectAndHandshake();

            var creqTask = Client.AuthAsync("test.model", "method");
            var req = await WebSocket.GetRequestAsync();
            req.AssertMethod("auth.test.model.method");
            req.SendResult(new JObject { { "rid", "test.ref" }, { "models", new JObject { { "test.ref", Test.Model } } } });
            var model = await creqTask;
            Assert.IsType<ResModel>(model);
            Test.AssertEqualJSON(Test.Model, model);
        }
    }
}
