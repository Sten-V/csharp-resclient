using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

[assembly: InternalsVisibleTo("ResgateIO.Client.UnitTests")]
namespace ResgateIO.Client
{

    internal class ItemCache
    {
        private delegate TraverseState TraverseCallback(CacheItem traversedCI, TraverseState ret);

        private readonly struct TraverseState
        {
            public readonly ReferenceState State;
            public readonly string ResourceID;

            public TraverseState(ReferenceState state, string rid)
            {
                State = state;
                ResourceID = rid;
            }

            public TraverseState(ReferenceState state)
            {
                State = state;
                ResourceID = null;
            }
        }

        private static readonly TraverseState TraverseStop = new TraverseState(ReferenceState.Stop);
        private static readonly TraverseState TraverseContinue = new TraverseState(ReferenceState.None);

        private readonly Dictionary<string, CacheItem> cache = new Dictionary<string, CacheItem>();
        private readonly object cacheLock = new object();
        private IResourceType[] resourceTypes;
        private PatternMap<ResourceFactory> resourcePatterns;

        public ResClient Client { get; private set; }

        public const int ResourceTypeModel = 0;
        public const int ResourceTypeCollection = 1;

        // Events
        public event ErrorEventHandler Error;
        public event EventHandler<ResourceEventArgs> ResourceEvent;

        // Cache dictionary exposed for test assertion purpose.
        public IReadOnlyDictionary<string, CacheItem> Cache { get { return cache; } }

        public ItemCache() : this(null) { }

        public ItemCache(ResClient client)
        {
            Client = client;
            createResourceTypes();
        }

        private void createResourceTypes()
        {

            resourcePatterns = new PatternMap<ResourceFactory>();

            resourceTypes = new IResourceType[]
            {
                 new ResourceTypeModel(this, resourcePatterns),
                 new ResourceTypeCollection(this, resourcePatterns),
                 new ResourceTypeError(this, resourcePatterns),
            };

            foreach (var resourceType in resourceTypes)
            {
                resourceType.Error += new ErrorEventHandler(onError);
            }
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
            var rt = (ResourceTypeModel)resourceTypes[ResourceTypeModel];
            rt.Patterns.Add(pattern, new ResourceFactory(factory));
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
            var rt = (ResourceTypeCollection)resourceTypes[ResourceTypeCollection];
            rt.Patterns.Add(pattern, new ResourceFactory(factory));
        }

        public CacheItem Subscribe(string rid, Action<CacheItem, ResponseCallback> subscribe)
        {
            CacheItem ci;
            lock (cacheLock)
            {
                if (!cache.TryGetValue(rid, out ci))
                {
                    ci = new CacheItem(this, rid);
                    cache[rid] = ci;
                }
                if (ci.AddSubscription(true))
                {
                    subscribe(ci, (result, err) =>
                    {
                        if (err == null)
                        {
                            try
                            {
                                AddResources(result.Result);
                            }
                            catch (ResException ex)
                            {
                                err = ex.Error;
                            }
                            catch (Exception ex)
                            {
                                err = new ResError(ex.Message);
                            }
                        }

                        if (err != null)
                        {
                            ci.RemoveSubscription(false);
                            TryDelete(ci);
                            ci.TrySetException(new ResException(err));
                        }
                    });
                }
            }

            return ci;
        }

        public async Task Unsubscribe(string rid, Action<string, ResponseCallback> unsubscribe)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            CacheItem ci;
            lock (cacheLock)
            {
                if (!cache.TryGetValue(rid, out ci))
                {
                    tcs.SetException(new InvalidOperationException(String.Format("Resource not found in cache: {0}", rid)));
                    return;
                }
                if (ci.Subscriptions == 0)
                {
                    tcs.SetException(new InvalidOperationException(String.Format("Resource not directly subscribed: {0}", rid)));
                    return;
                }
                if (ci.VirtualSubscriptions > 0)
                {
                    ci.RemoveSubscription(true);
                    tcs.SetResult(null);
                    return;
                }
            }

            unsubscribe(ci.ResourceID, (result, err) =>
            {
                if (err == null)
                {
                    try
                    {
                        // Try delete the unsubscribed resource.
                        ci.RemoveSubscription(false);
                        TryDelete(ci);
                        tcs.SetResult(null);
                        return;
                    }
                    catch (ResException ex)
                    {
                        tcs.SetException(ex);
                        return;
                    }
                    catch (Exception ex)
                    {
                        err = new ResError(ex.Message);
                    }
                }

                if (err != null)
                {
                    tcs.SetException(new ResException(err));
                }
            });

            await tcs.Task
                .ConfigureAwait(false);
        }

        public CacheItem AddResourcesAndSubscribe(JToken result, string rid)
        {
            CacheItem ci;
            lock (cacheLock)
            {
                addResources(result);
                if (!cache.TryGetValue(rid, out ci))
                {
                    throw new ResException(String.Format("Resource not found in cache: {0}", rid));
                }
                ci.AddSubscription(false);
            }
            return ci;
        }

        public bool SetAllStale()
        {
            bool hasStale = false;
            lock (cacheLock)
            {
                foreach (CacheItem ci in cache.Values)
                {
                    ci.SetStale();
                    hasStale = true;
                }
            }
            return hasStale;
        }

        public void SubscribeStale(Action<CacheItem, ResponseCallback> subscribe)
        {
            lock (cacheLock)
            {
                foreach (var pair in cache)
                {
                    var ci = pair.Value;
                    if (ci.TrySubscribeStale())
                    {
                        subscribe(ci, (result, err) =>
                        {
                            if (err == null)
                            {
                                try
                                {
                                    AddResources(result.Result);
                                }
                                catch (ResException ex)
                                {
                                    err = ex.Error;
                                }
                                catch (Exception ex)
                                {
                                    err = new ResError(ex.Message);
                                }
                            }

                            if (err != null)
                            {
                                ci.ClearSubscriptions();
                                TryDelete(ci);

                                invokeEvent(ci, new ResourceUnsubscribeEventArgs
                                {
                                    ResourceID = ci.ResourceID,
                                    EventName = "unsubscribe",
                                    Reason = err,
                                });
                            }
                        });
                    }
                }
            }
        }

        /// <summary>
        /// Tries to delete a cached item.
        /// It will be deleted or set as stale if there are no subscriptions or references.
        /// </summary>
        /// <param name="ci"></param>
        public void TryDelete(CacheItem ci)
        {
            lock (cacheLock)
            {
                var refs = getRefState(ci);

                foreach (var pair in refs)
                {
                    CacheItemReference r = pair.Value;
                    switch (r.State)
                    {
                        case ReferenceState.Delete:
                            deleteRef(r.Item);
                            break;
                    }
                }
            }
        }

        private IResourceType getResourceType(ResourceType typ)
        {
            switch (typ)
            {
                case ResourceType.Model:
                    return resourceTypes[ResourceTypeModel];
                case ResourceType.Collection:
                    return resourceTypes[ResourceTypeCollection];
                case ResourceType.Error:
                    throw new NotImplementedException();
            }

            throw new ArgumentException("Unknown resource type");
        }

        private void deleteRef(CacheItem item)
        {
            if (item.InternalResource != null)
            {
                IResourceType typ = getResourceType(item.Type);

                IEnumerable<object> values = typ.GetResourceValues(item.InternalResource);
                if (values != null)
                {
                    foreach (object value in values)
                    {
                        CacheItem refItem = getRefItem(value);
                        if (refItem != null)
                        {
                            refItem.AddReference(-1);
                        }
                    }
                }
            }

            cache.Remove(item.ResourceID);
        }

        private CacheItem getRefItem(object value)
        {
            ResResource resource = value as ResResource;
            if (resource == null)
            {
                return null;
            }

            if (cache.TryGetValue(resource.ResourceID, out CacheItem refItem))
            {
                return refItem;
            }

            // refItem not in cache means
            // item has been deleted as part of
            // a refState object
            return null;
        }

        /// <summary>
        /// Gets the reference state for a cache item and all its reference, if the item was to be removed.
        /// </summary>
        /// <param name="ci">Cache item</param>
        /// <returns>Set of cache item references.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private Dictionary<string, CacheItemReference> getRefState(CacheItem ci)
        {
            var refs = new Dictionary<string, CacheItemReference>();

            // Quick exit if directly subscribed
            if (ci.Subscriptions > 0)
            {
                return refs;
            }

            refs[ci.ResourceID] = new CacheItemReference { Item = ci, Count = ci.References, State = ReferenceState.None };
            traverse(ci, (traversedCI, state) => seekRefs(refs, traversedCI), new TraverseState(ReferenceState.None), true);
            traverse(ci, (traversedCI, state) => markDelete(refs, traversedCI, state), new TraverseState(ReferenceState.Delete));
            return refs;
        }

        /// <summary>
        /// Seeks for resources that no longer has any reference and may be deleted.
        /// </summary>
        /// <remarks>Used as callback for the traverse method.</remarks>
        /// <param name="refs">Set of cache item references.</param>
        /// <param name="ci">Cache item.</param>
        /// <returns>ReferenceState.Abort if not traversing further, or else ReferenceState.None.</returns>
        private TraverseState seekRefs(Dictionary<string, CacheItemReference> refs, CacheItem ci)
        {
            if (ci.Subscriptions > 0)
            {
                return TraverseStop;
            }

            var rid = ci.ResourceID;
            if (refs.TryGetValue(rid, out var refState))
            {
                // The reference has already been encountered and traversed.
                // Just count down references and stop traversing further.
                refState.Count--;
                return TraverseStop;
            }

            // First time encountering this referenced item. Add it to the set.
            refs[rid] = new CacheItemReference { Item = ci, Count = ci.References - 1, State = ReferenceState.None };
            return TraverseContinue;
        }

        /// <summary>
        /// Marks reference as Delete, Keep, or Stale, depending on the values returned from a seekRefs traverse.
        /// </summary>
        /// <param name="refs">Set of cache item references.</param>
        /// <param name="ci">Cache item.</param>
        /// <param name="state">State as returned from parent's traverse callback.</param>
        /// <returns>State to pass to children. Abort means no traversing to children.</returns>
        private TraverseState markDelete(Dictionary<string, CacheItemReference> refs, CacheItem ci, TraverseState state)
        {
            // Quick exit if it is already subscribed
            if (ci.Subscriptions > 0)
            {
                return TraverseStop;
            }

            var rid = ci.ResourceID;
            var refState = refs[rid];

            if (refState.State == ReferenceState.Keep)
            {
                return TraverseStop;
            }

            if (state.State == ReferenceState.Delete)
            {
                if (refState.Count > 0)
                {
                    refState.State = ReferenceState.Keep;
                }
                else if (refState.State != ReferenceState.None)
                {
                    return TraverseStop;
                }
                else
                {
                    refState.State = ReferenceState.Delete;
                    return state;
                }
                return new TraverseState(refState.State, rid);
            }

            // A stale item can never cover itself
            if (state.ResourceID != null && state.ResourceID == rid)
            {
                return TraverseStop;
            }

            refState.State = ReferenceState.Keep;
            return refState.Count > 0
                ? new TraverseState(ReferenceState.Keep, rid)
                : state;
        }

        private void traverse(CacheItem ci, TraverseCallback cb, TraverseState state, bool skipFirst = false)
        {
            // Call callback to get new state to pass to
            // children. If Abort, we should not traverse deeper
            if (!skipFirst)
            {
                state = cb(ci, state);
                if (state.State == ReferenceState.Stop)
                {
                    return;
                }
            }

            if (ci.InternalResource != null)
            {
                IResourceType typ = getResourceType(ci.Type);

                IEnumerable<object> values = typ.GetResourceValues(ci.InternalResource);
                if (values != null)
                {
                    foreach (object value in values)
                    {
                        CacheItem refItem = getRefItem(value);
                        if (refItem != null)
                        {
                            traverse(refItem, cb, state, false);
                        }
                    }
                }
            }
        }

        public void AddResources(JToken data)
        {
            lock (cacheLock)
            {
                addResources(data);
            }
        }

        /// <summary>
        /// Gets a cache item from the cache, or throws an exception if it doesn't exist.
        /// </summary>
        /// <param name="rid">Resource ID</param>
        /// <returns>Cache item.</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public CacheItem GetItem(string rid)
        {
            CacheItem ci;
            lock (cacheLock)
            {
                if (!cache.TryGetValue(rid, out ci))
                {
                    throw new InvalidOperationException(String.Format("Resource not found in cache: {0}", rid));
                }
            }
            return ci;
        }

        public void HandleEvent(ResourceEventArgs ev)
        {
            CacheItem ci = GetItem(ev.ResourceID);

            // Assert that the resource is set.
            // Should not be needed unless the gateway acts up.
            if (!ci.IsSet)
            {
                return;
            }

            if (ev.EventName == "unsubscribe")
            {
                ev = handleUnsubscribeEvent(ci, ev);
            }
            else
            {
                var type = getResourceType(ci.Type);
                ev = type.HandleEvent(ci.InternalResource, ev);
            }

            invokeEvent(ci, ev);
        }

        private void invokeEvent(CacheItem ci, ResourceEventArgs ev)
        {
            if (ev != null)
            {
                // Invoke event for resource
                try
                {
                    ci.Resource.HandleEvent(ev);
                }
                catch (Exception ex)
                {
                    Error?.Invoke(this, new ErrorEventArgs(ex));
                }

                // Invoke event for global listeners
                try
                {
                    ResourceEvent?.Invoke(this, ev);
                }
                catch (Exception ex)
                {
                    Error?.Invoke(this, new ErrorEventArgs(ex));
                }
            }
        }

        /// <summary>
        /// Adds a resources from a request result to the cache.
        /// The cacheLock must be held before call.
        /// </summary>
        /// <param name="result">Request result</param>
        private void addResources(JToken result)
        {
            if (result == null)
            {
                return;
            }

            JObject r = result as JObject;
            if (r == null)
            {
                return;
            }

            JObject[] typeResources = new JObject[resourceTypes.Length];
            Dictionary<string, JToken>[] typeSync = new Dictionary<string, JToken>[resourceTypes.Length];

            // Create empty resources for missing ones, and a dictionary of already existing resources to be synchronized.
            for (int i = 0; i < resourceTypes.Length; i++)
            {
                IResourceType type = resourceTypes[i];
                JProperty resourceProp = r.Property(type.ResourceProperty);
                if (resourceProp != null)
                {
                    JObject resources = resourceProp.Value as JObject;
                    if (resources != null)
                    {
                        typeResources[i] = resources;
                        typeSync[i] = createResources(resources, type);
                    }
                }
            }

            // Initialize new resources with data
            for (int i = 0; i < resourceTypes.Length; i++)
            {
                IResourceType type = resourceTypes[i];
                JObject resources = typeResources[i];
                if (resources != null)
                {
                    var sync = typeSync[i];
                    foreach (JProperty prop in resources.Properties())
                    {
                        string rid = prop.Name;
                        // Only initialize if not set for synchronization
                        if (sync == null || !sync.ContainsKey(rid))
                        {
                            CacheItem ci = cache[rid];
                            ci.SetInternalResource(type.InitResource(ci.Resource, prop.Value));

                        }
                    }
                }
            }

            // Synchronize stale resources with new data
            for (int i = 0; i < resourceTypes.Length; i++)
            {
                var sync = typeSync[i];
                if (sync != null)
                {
                    IResourceType type = resourceTypes[i];
                    foreach (KeyValuePair<string, JToken> pair in sync)
                    {
                        var ci = cache[pair.Key];
                        type.SynchronizeResource(ci.ResourceID, ci.InternalResource, pair.Value, (ev) =>
                        {
                            invokeEvent(ci, ev);
                        });
                    }
                }
            }

            // Complete all resource tasks
            for (int i = 0; i < resourceTypes.Length; i++)
            {
                IResourceType type = resourceTypes[i];
                JObject resources = typeResources[i];
                if (resources != null)
                {
                    foreach (JProperty prop in resources.Properties())
                    {
                        string rid = prop.Name;
                        cache[rid].CompleteTask();
                    }
                }
            }
        }

        private Dictionary<string, JToken> createResources(JObject resources, IResourceType type)
        {
            Dictionary<string, JToken> sync = null;

            foreach (JProperty prop in resources.Properties())
            {
                string rid = prop.Name;
                CacheItem ci = null;
                if (!cache.TryGetValue(rid, out ci))
                {
                    // If the resource is not cached since before, create a new cache item for it.
                    ci = new CacheItem(this, rid);
                    cache[rid] = ci;
                }

                // If it is set since before, it is stale and needs to be updated
                if (ci.IsSet)
                {
                    if (ci.Resource.Type != type.ResourceType)
                    {
                        throw new InvalidOperationException("Resource type inconsistency");
                    }

                    sync = sync ?? new Dictionary<string, JToken>();
                    sync[rid] = prop.Value;
                }
                else
                {
                    ci.SetResource(type.CreateResource(rid), type.ResourceType);
                }
            }

            return sync;
        }

        public object ParseValue(JToken value, bool addIndirect)
        {
            if (value.Type == JTokenType.Null)
            {
                return null;
            }

            var obj = value as JObject;
            if (obj != null)
            {
                // Test for resource reference
                JToken ridToken = obj["rid"];
                if (ridToken != null)
                {
                    var rid = ridToken.Value<string>();

                    // Test for soft reference
                    JToken softToken = obj["soft"];
                    if (softToken != null && softToken.Value<bool>())
                    {
                        return new ResRef(rid);
                    }

                    CacheItem item = cache[rid];
                    if (addIndirect)
                    {
                        item.AddReference(1);
                    }
                    return item.Resource;
                }

                // Test for data value
                JToken dataToken = obj["data"];
                if (dataToken != null)
                {
                    return dataToken;
                }

                // Test for action value
                JToken actionToken = obj["action"];
                if (actionToken != null)
                {
                    if (actionToken.Value<string>() == "delete")
                    {
                        return ResAction.Delete;
                    }
                }
            }
            else
            {
                var val = value as JValue;
                if (val != null)
                {
                    return val.Value;
                }
            }

            throw new InvalidOperationException("Invalid RES value: " + value.ToString(Formatting.None));
        }

        private ResourceEventArgs handleUnsubscribeEvent(CacheItem ci, ResourceEventArgs ev)
        {
            ci.ClearSubscriptions();
            TryDelete(ci);

            ResError reason = null;
            JObject data = ev.Data as JObject;
            if (data != null)
            {
                JToken reasonToken = data["reason"];
                if (reasonToken != null)
                {
                    reason = reasonToken.ToObject<ResError>();
                }
            }

            if (reason == null)
            {
                reason = new ResError("Missing unsubscribe reason.");
            }

            return new ResourceUnsubscribeEventArgs
            {
                ResourceID = ev.ResourceID,
                EventName = ev.EventName,
                Data = ev.Data,
                Reason = reason,
            };
        }
        private void onError(object sender, ErrorEventArgs ev)
        {
            Error?.Invoke(this, ev);
        }

    }
}
